package workers

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// ErrorMaxWorker is returned when no new worker can be spawned (max concurrency).
var ErrorMaxWorker = errors.New("max number of workers reached")

// ErrorNotRunning is returned when the pool is not running.
var ErrorNotRunning = errors.New("pool is not running")

var ErrorOverloaded = errors.New("poo")

// WorkerFunc is the signature of the job handler.
type WorkerFunc func(golly.Context, interface{}) error

// Pool manages worker goroutines that can grow/shrink between minW and maxW.
type Pool struct {
	name string
	ctx  golly.Context

	// Worker lifecycle
	minW, maxW int32

	activeWorkers atomic.Int32
	spawnedCnt    atomic.Int32

	processed atomic.Int32
	enqueued  atomic.Int32

	// Underlying job queue
	jobs chan Job

	// Slices of idle workers
	workers []*Worker

	lock sync.RWMutex

	// Concurrency
	poolWG sync.WaitGroup

	// Control signals
	quit    chan struct{}
	checkin chan Notification

	running bool

	// Handler is the function that will be executed by default for each job.
	handler WorkerFunc

	// IdleTimeout for reaping. Workers beyond minW can be reaped if idle too long.
	idleTimeout time.Duration

	waitTimeout time.Duration

	reapInterval time.Duration
}

type PoolConfig struct {
	// Name of the pool, defaults to the handler function name
	Name string

	// MinW min number of workers to allow defaults to 0
	MinW int32

	// MaxW max numer of workers to allow defaults to 10
	MaxW int32

	// How long to allow a worker to sit around idle
	// defaults to 5 minutes to reduce routine churn
	IdleTimeout time.Duration

	// WaitTimeout how long to wait for a new worker or the
	// job buffer to become available default 2 seconds
	WaitTimeout time.Duration

	// ReapInterval is the duration of time the system waits
	// between reap intervals (Defaults to 10% of IdleTimeout)
	ReapInterval time.Duration

	// Depth of the queue defaults to 100
	QueueDepth int32

	Logger *logrus.Logger

	// Handler is the default handler to use
	Handler WorkerFunc
}

func (p *Pool) ProcessedCount() int32 {
	return p.processed.Load()
}

func (p *Pool) EnqueuedCount() int32 {
	return p.enqueued.Load()
}

// Name returns the Pool's name.
func (p *Pool) Name() string { return p.name }

// Workers returns the current slice of idle workers.
func (p *Pool) Workers() []*Worker {
	p.lock.RLock()
	defer p.lock.RUnlock()

	return p.workers
}

// Wait blocks until the Pool has fully stopped (no active workers).
func (p *Pool) Wait() {
	p.poolWG.Wait()
}

func (p *Pool) EnQueueFunc(ctx golly.Context, handler WorkerFunc, data any) error {

	if !p.running {
		return ErrorNotRunning
	}

	job := Job{Ctx: ctx, Data: data, Handler: handler}

	select {
	case p.jobs <- job:
		p.enqueued.Add(1)

		return nil

	case <-time.After(p.waitTimeout):
		// We waited 2s for capacity in the channel, timed out
		return fmt.Errorf("job queue is full; enqueue timed out")
	case <-ctx.Context().Done():
		// The caller’s context might have been cancelled
		return ctx.Context().Err()
	}

}

// EnQueue adds a job to the pool's job channel. Non-blocking if channel has capacity.
func (p *Pool) EnQueue(ctx golly.Context, data any) error {
	if p.handler == nil {
		return fmt.Errorf("global handler not set use EnqueFunc instead")
	}

	return p.EnQueueFunc(ctx, p.handler, data)
}

// Stop signals the pool to stop processing new jobs and eventually shut down.
func (p *Pool) Stop() {
	// idempotent
	if p.running {
		close(p.jobs)
	}
}

// Run starts the main loop, spawning workers up to minW right away if desired.
func (p *Pool) Run(ctx golly.Context) {
	p.running = true
	p.poolWG.Add(1)

	defer p.poolWG.Done()

	logger := ctx.Logger().WithFields(logrus.Fields{"pool": p.name})

	p.ctx = ctx.Dup()
	p.ctx.SetLogger(logger)

	reap := time.NewTicker(p.reapInterval)
	defer reap.Stop()

	logger.Debug("Pool started.")

	for p.running {
		select {
		case <-ctx.Context().Done():
			logger.Debug("Stopping: context canceled")
			p.running = false

		case job, ok := <-p.jobs:
			if !ok {
				p.running = false
				break
			}

			p.enqueued.Add(-1)
			p.processed.Add(1)

			// If we successfully got a job, dispatch it to a worker
			p.dispatch(job)

		case notify := <-p.checkin:
			p.Checkin(notify.Worker)

		case <-reap.C:
			// Reap idle workers
			go p.reap()
		}
	}

	fmt.Printf("Here")

	logger.Debug("Waiting for workers to finish...")

	// set maxW to 0 so any leftover idle workers can exit
	p.maxW = 0
	p.reap()

	logger.Debugf("Reaping leftover workers (active jobs: %d)", p.activeWorkers.Load())

	logger.Debug("Pool terminated.")
}

// dispatch checks out an idle worker or spawns a new one, then performs the job.
func (p *Pool) dispatch(j Job) {
	if !p.running {
		p.ctx.Logger().Errorf("Attempted to dispatch a job while pool is not running")
		return
	}

	worker, err := p.Checkout()
	if err != nil {
		// If we can't get a worker, re-queue the job to retry
		if err != ErrorMaxWorker {
			p.ctx.Logger().Errorf("Error checking out worker: %v", err)
		}
		p.jobs <- j // put job back to the queue
		return
	}

	// this is a hax right now will think of a better way to signal this
	// later
	j.OnComplete = p.checkin

	worker.Perform(j)
}

// Checkout either grabs an idle worker or spawns a new one if under maxW.
func (p *Pool) Checkout() (*Worker, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	// If there's an idle worker available, pop it from the slice
	if len(p.workers) > 0 {
		worker := p.workers[len(p.workers)-1]
		p.workers = p.workers[:len(p.workers)-1]
		return worker, nil
	}

	// If we are below max concurrency, create a new worker
	if p.activeWorkers.Load() < p.maxW {
		w := p.newWorker()
		w.Start()

		return w, nil
	}

	// No idle workers and at max concurrency
	return nil, ErrorMaxWorker
}

// Checkin places a worker back into the idle slice.
func (p *Pool) Checkin(worker *Worker) {
	p.lock.Lock()
	defer p.lock.Unlock()

	p.workers = append(p.workers, worker)
}

// newWorker increments counters and creates a worker.
func (p *Pool) newWorker() *Worker {
	p.activeWorkers.Add(1)

	return NewWorker(fmt.Sprintf("%s-%06d", p.name, p.spawnedCnt.Add(1)))
}

// reap removes any idle or non-running workers from the pool, respecting the
// minimum worker count (minW). It returns the total number of workers actually
// reaped (stopped and removed from the pool).
//
// The function proceeds as follows:
//
//  1. Checks the current active worker count (activeWorkers). If activeWorkers
//     is at or below minW, no reaping is performed immediately.
//
//  2. Acquires a write lock (p.lock) to safely iterate over the p.workers slice.
//
//  3. For each worker in p.workers:
//     - If the pool has already removed enough workers to approach minW
//     (i.e., active - reaped <= minW), we stop removing any further workers.
//     - Otherwise, checks two conditions to determine if a worker should be reaped:
//     a) !w.IsRunning()  (the worker is no longer running), or
//     b) w.IsIdleFor(p.idleTimeout) (the worker has been idle too long).
//
//     - If either condition applies, this worker is removed from the slice,
//     appended to a temporary list (toReap), and the reaped count is incremented.
//
//     - If neither condition applies, the loop moves on to check the next worker.
//
//  4. Decrements the pool’s active worker counter by the number of reaped workers.
//
//  5. Releases the lock before actually calling worker.Stop() on each reaped worker.
//     This ensures that any potentially blocking Stop() call does not lock up
//     the entire pool.
//
// In practice, this design avoids holding the lock during potentially long-running
// operations (like stopping threads), thus reducing contention. However, it does mean
// that after the function releases the lock, other pool operations may run while
// individual workers are being stopped.
//
// Example usage within a scheduled “housekeeping” or “heartbeat” goroutine:
//
//	go func() {
//	  for {
//	    select {
//	    case <-ticker.C:
//	      reapedCount := p.reap()
//	      if reapedCount > 0 {
//	        p.logger.Infof("Reaped %d workers", reapedCount)
//	      }
//	    case <-quit:
//	      return
//	    }
//	  }
//	}()
func (p *Pool) reap() (reaped int32) {
	active := p.activeWorkers.Load()
	if active <= p.minW {
		return 0
	}

	var toReap []*Worker

	p.lock.Lock()

	// We'll collect the workers to stop in toReap so we can call Stop() outside the lock.
	for i := 0; i < len(p.workers); {
		worker := p.workers[i]
		// JIC
		if worker == nil {
			break
		}

		if worker.IsRunning() {
			if (active-reaped <= p.minW) || !worker.IsIdleFor(p.idleTimeout) {
				i++
				continue
			}
		}

		// Remove from slice
		p.workers = append(p.workers[:i], p.workers[i+1:]...)
		toReap = append(toReap, worker)
	}

	p.lock.Unlock()

	if reaped := len(toReap); reaped > 0 {
		p.activeWorkers.Add(int32(-reaped))

		p.ctx.Logger().Debugf("Reaped %d inactive workers", reaped)
	}

	// Now that we've released the lock, stop each worker.
	// This may block, but won't hold up the entire pool's lock.
	for _, w := range toReap {
		w.Stop()
	}

	return reaped
}

// NewPool creates and configures a new worker Pool.
func NewPool(config ...PoolConfig) *Pool {
	var pc PoolConfig

	if len(config) > 0 {
		pc = config[0]
	}

	if pc.Name == "" {
		if handler := pc.Handler; handler != nil {
			pc.Name = utils.GetTypeWithPackage(handler)
		} else {
			pc.Name = fmt.Sprintf("pool-%s", uuid.NewString())
		}
	}

	if pc.MaxW == 0 {
		pc.MaxW = 10
	}

	if pc.IdleTimeout == 0 {
		pc.IdleTimeout = 5 * time.Minute
	}

	if pc.WaitTimeout == 0 {
		pc.WaitTimeout = 2 * time.Second
	}

	if pc.ReapInterval == 0 {
		pc.ReapInterval = pc.IdleTimeout / 10
	}

	if pc.QueueDepth == 0 {
		pc.QueueDepth = 100
	}

	if pc.Logger == nil {
		pc.Logger = logrus.New()
	}

	return &Pool{
		name: pc.Name,
		minW: pc.MinW,
		maxW: pc.MaxW,

		// We create a channel with a capacity = max * 3 or 500 (whichever is larger)
		jobs:    make(chan Job, pc.QueueDepth),
		quit:    make(chan struct{}),
		checkin: make(chan Notification),

		reapInterval: pc.ReapInterval,

		running:     false,
		handler:     pc.Handler,
		idleTimeout: pc.IdleTimeout,
		waitTimeout: pc.WaitTimeout,
	}
}
