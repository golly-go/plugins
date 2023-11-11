package workers

import (
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

type Job struct {
	Ctx     golly.Context
	Data    interface{}
	Handler WorkerFunc
}

type Workers []*Worker

func (wks Workers) Find(finder func(*Worker) bool) *Worker {
	for _, worker := range wks {
		if finder(worker) {
			return worker
		}
	}
	return nil
}

func (wks Workers) Each(fnc func(*Worker)) {
	for _, worker := range wks {
		fnc(worker)
	}
}

func (wks Workers) Parition(finder func(*Worker) bool) (match []*Worker, nmatch []*Worker) {
	for _, worker := range wks {
		if finder(worker) {
			match = append(match, worker)
		} else {
			nmatch = append(nmatch, worker)
		}
	}
	return
}

type WorkerConfig struct {
	ID string

	OnJobStart func(*Worker, Job) error
	OnJobEnd   func(*Worker, Job) error

	Logger *logrus.Entry

	IdleTimeout time.Duration
}
type Worker struct {
	id          string
	startedAt   time.Time
	lastJobAt   time.Time
	processing  bool
	logger      *logrus.Entry
	onJobStart  func(*Worker, Job) error
	onJobEnd    func(*Worker, Job) error
	IdleTimeout time.Duration
}

func NewWorker(config WorkerConfig) *Worker {
	return &Worker{
		id: config.ID,
		logger: config.Logger.WithFields(logrus.Fields{
			"worker.type": "Worker",
			"worker.id":   config.ID,
		}),
		onJobStart:  config.OnJobStart,
		onJobEnd:    config.OnJobEnd,
		startedAt:   time.Now(),
		processing:  false,
		IdleTimeout: config.IdleTimeout,
	}
}

func (w *Worker) ID() string {
	return w.id
}

func (w *Worker) IsIdle() bool {
	return !w.processing && time.Since(w.lastJobAt) > w.IdleTimeout
}

func (w *Worker) Perform(j Job) {
	w.processing = true
	defer func() {
		w.processing = false
		w.lastJobAt = time.Now()
	}()

	if w.onJobStart != nil {
		if err := w.onJobStart(w, j); err != nil {
			w.logger.Errorln("Error in job start callback:", err)
			return
		}
	}

	// Perform the job
	if err := j.Handler(j.Ctx, j.Data); err != nil {
		w.logger.Errorln("Error performing job:", err)
	}

	if w.onJobEnd != nil {
		if err := w.onJobEnd(w, j); err != nil {
			w.logger.Errorln("Error in job end callback:", err)
		}
	}
}
