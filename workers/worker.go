package workers

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
)

type Notification struct {
	Worker *Worker
	Job    *Job
}

// Job holds data plus the specific WorkerFunc to handle it.
type Job struct {
	Ctx     *golly.Context
	Data    interface{}
	Handler WorkerFunc

	OnComplete chan Notification
}

// Worker is a single goroutine that processes jobs.
type Worker struct {
	id string

	startedAt time.Time
	lastJobAt time.Time

	running    atomic.Bool
	processing atomic.Bool

	job     chan Job
	checkin chan *Worker

	wg sync.WaitGroup
}

// NewWorker constructs a Worker from WorkerConfig.
func NewWorker(id string) *Worker {
	return &Worker{
		id:        id,
		startedAt: time.Now(),
		job:       make(chan Job),
	}
}

// ID returns the Worker's identifier.
func (w *Worker) ID() string {
	return w.id
}

// Start begins the worker goroutine.
func (w *Worker) Start() {
	w.running.Store(true)

	w.wg.Add(1)
	go w.loop()
}

// IsIdleFor checks if the worker is not processing and has been idle > IdleTimeout.
func (w *Worker) IsIdleFor(timeout time.Duration) bool {
	processing := w.processing.Load()

	return !processing && time.Since(w.lastJobAt) > timeout
}

func (w *Worker) IsRunning() bool {
	return w.running.Load()
}

// loop is the main routine that processes jobs until we receive a quit signal.
func (w *Worker) loop() {
	defer w.wg.Done()

	for {
		job, ok := <-w.job
		if !ok {
			break
		}

		if err := job.Handler(job.Ctx, job.Data); err != nil {
			job.Ctx.Logger().Error(err)
			break
		}

		if job.OnComplete == nil {
			continue
		}

		job.OnComplete <- Notification{
			Worker: w,
			Job:    &job,
		}
	}

	w.running.Store(false)
}

func (w *Worker) Perform(job Job) {
	w.job <- job
}

// Stop signals the worker to shut down, then waits for it to exit.
func (w *Worker) Stop() {
	if !w.running.Load() {
		return
	}

	w.running.Store(false)

	close(w.job)

	w.wg.Wait()
}
