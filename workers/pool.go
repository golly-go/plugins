package workers

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

var (
	ErrorMaxWorker  = fmt.Errorf("max number of workers reached")
	ErrorNotRunning = fmt.Errorf("pool is not running")
)

type WorkerFunc func(golly.Context, interface{}) error

type Pool struct {
	ctx golly.Context

	workers Workers

	lock sync.RWMutex

	handler WorkerFunc
	jobs    chan Job

	name    string
	quit    chan struct{}
	running bool

	workerWG sync.WaitGroup
	wg       sync.WaitGroup

	minW int32
	maxW int32

	logger *logrus.Entry

	activeWorkers atomic.Int32
	spawnedCnt    atomic.Int32

	idleTimeout time.Duration
}

func (pb *Pool) NewWorker(ctx golly.Context, id string) *Worker {
	pb.activeWorkers.Add(1)

	cnt := pb.spawnedCnt.Add(1)

	return NewWorker(WorkerConfig{
		ID:     fmt.Sprintf("%s-%06d", pb.name, cnt),
		Logger: pb.logger,
	})
}

func (pb *Pool) Name() string     { return pb.name }
func (pb *Pool) Workers() Workers { return pb.workers }
func (pb *Pool) Wait()            { pb.wg.Wait() }

func (pb *Pool) EnQueue(ctx golly.Context, job interface{}) error {
	pb.jobs <- Job{ctx, job, pb.handler}

	return nil
}

func (pb *Pool) Checkout() (*Worker, error) {
	// Lock the worker pool to safely access the workers slice.
	pb.lock.Lock()
	defer pb.lock.Unlock()

	// If there are available workers in the pool, use one of them.
	if len(pb.workers) > 0 {
		// Get the last worker in the slice.
		worker := pb.workers[len(pb.workers)-1]
		// Remove the worker from the slice.
		pb.workers = pb.workers[:len(pb.workers)-1]
		// Return the worker.
		return worker, nil
	}

	// If there are no available workers but we have not reached maxW, create a new worker.
	if pb.activeWorkers.Load() < pb.maxW {
		worker := pb.NewWorker(pb.ctx, "worker")
		return worker, nil
	}

	// If we've reached the maximum number of workers, return an error.
	return nil, ErrorMaxWorker
}

func (pb *Pool) Checkin(worker *Worker) {
	pb.lock.Lock()
	defer pb.lock.Unlock()

	pb.workers = append(pb.workers, worker)
}

func (pb *Pool) Stop() {
	close(pb.quit)
}

func (pb *Pool) reap() (reaped int32) {
	active := int32(pb.activeWorkers.Load())

	if active > pb.minW {
		fmt.Printf("reaped: %d, active: %d, minW: %d\n", reaped, active, pb.minW)

		for pos, worker := range pb.workers {

			if active-reaped <= pb.minW {
				pb.activeWorkers.Add(-reaped)
				break
			}

			if worker.IsIdle() {
				reaped++

				if pos+1 <= len(pb.workers) {
					pb.workers = append(pb.workers[:pos], pb.workers[pos+1:]...)
				} else {
					pb.workers = pb.workers[:pos]
				}
			}
		}
	}

	if reaped > 0 {
		pb.logger.Infof("%s: repead %d workers", pb.name, reaped)
	}

	return
}

func (pb *Pool) Run(ctx golly.Context) {
	pb.running = true

	pb.wg.Add(1)
	defer pb.wg.Done()

	pb.logger = ctx.Logger().WithFields(logrus.Fields{
		"spawner": pb.name,
	})

	heartbeat := time.NewTicker(500 * time.Millisecond)
	defer heartbeat.Stop()

	for pb.running {
		select {
		case <-pb.quit:
			pb.logger.Debug("stopping quit channel")
			pb.running = false
		case <-ctx.Context().Done():
			pb.logger.Debug("stopping context done")
			pb.running = false
		case j := <-pb.jobs:
			pb.Perform(j)
		case <-heartbeat.C:
			pb.reap()
		}
	}

	pb.logger.Debug("waiting for worker completion to shutdown")

	pb.workerWG.Wait()

	pb.logger.Debugf("repead jobs for shutdown (active jobs: %d)", pb.activeWorkers.Load())

	pb.maxW = 0
	pb.reap()

	pb.logger.Debug("terminated")
}

func (pb *Pool) Perform(j Job) {
	if !pb.running {
		pb.logger.Errorf("Attempted to perform job while pool is not running")
		return
	}

	worker, err := pb.Checkout()
	if err != nil {
		if err == ErrorMaxWorker {
			// Optionally, implement a back-off strategy before re-enqueuing the job
			pb.jobs <- j
		} else {
			pb.logger.Errorf("Error checking out worker: %#v", err)
			// Consider handling the error differently if needed
		}
		return
	}

	pb.workerWG.Add(1)

	go func() {
		defer func() {
			pb.workerWG.Done()
			pb.Checkin(worker)
		}()
		worker.Perform(j)
	}()
}

func NewPool(name string, min, max int32, handler WorkerFunc) *Pool {
	return &Pool{
		name:        name,
		minW:        int32(math.Max(float64(min), 1)),
		maxW:        int32(math.Max(float64(max), 25)),
		handler:     handler,
		quit:        make(chan struct{}),
		jobs:        make(chan Job, int(math.Max(float64(max*3), 500))),
		idleTimeout: 30 * time.Second,
	}
}
