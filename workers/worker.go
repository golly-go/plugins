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

type Worker interface {
	Perform(j Job)
	Stop()
	Run()
	IsIdle() bool
}

type WorkerConfig struct {
	ID string

	OnJobStart func(Worker, Job) error
	OnJobEnd   func(Worker, Job) error
	Logger     *logrus.Entry
}

type GenericWorker struct {
	id        string
	quit      chan struct{}
	c         chan Job
	startedAt time.Time
	lastJobAt time.Time

	processing bool
	running    bool

	ID string

	onJobStart func(Worker, Job) error
	onJobEnd   func(Worker, Job) error
	logger     *logrus.Entry
}

func NewGenericWorker(config WorkerConfig) *GenericWorker {

	return &GenericWorker{
		id: config.ID,
		logger: config.Logger.WithFields(logrus.Fields{
			"worker.id":    config.ID,
			"worker.start": time.Now(),
		}),

		onJobStart: config.OnJobStart,
		onJobEnd:   config.OnJobEnd,

		quit:       make(chan struct{}),
		processing: false,
		running:    true,
	}
}

func (w *GenericWorker) Perform(j Job) { w.c <- j }

func (w *GenericWorker) handle(j Job) error {
	defer func() {
		w.processing = false

		if w.onJobEnd != nil {
			if err := w.onJobEnd(w, j); err != nil {
				w.logger.Errorln("error in job end callback", err)
			}
		}

		if r := recover(); r != nil {
			w.logger.Errorln("recovered from panic:", r)
		}
	}()

	w.processing = true

	if w.onJobStart != nil {
		if err := w.onJobStart(w, j); err != nil {
			return err
		}
	}

	w.lastJobAt = time.Now()
	err := j.Handler(j.Ctx, j.Data)

	elapsed := time.Since(w.startedAt)

	w.
		logger.
		WithField("worker.duration", elapsed.Nanoseconds()).
		Infof("finished perform (%s)", elapsed.String())

	return err

}

func (w *GenericWorker) Run() {
	w.startedAt = time.Now()

	go func() {
		for w.running {
			select {
			case <-w.quit:
				w.running = false
			case j := <-w.c:
				if err := w.handle(j); err != nil {
					w.logger.Errorf("unable to process job: %s", err.Error())
				}
			}
		}
	}()
}

func (w *GenericWorker) Stop() {
	w.running = false
	close(w.quit)
}

func (w *GenericWorker) IsIdle() bool {
	return !w.processing && time.Since(w.lastJobAt) > 30*time.Second
}
