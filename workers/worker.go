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
	ID() string
	Perform(j Job)
	Stop()
	Run()
	IsIdle() bool
}

type Workers map[Worker]bool

func (wks Workers) Find(finder func(Worker) bool) Worker {
	for worker := range wks {
		if finder(worker) {
			return worker
		}
	}
	return nil
}

func (wks Workers) Each(fnc func(Worker)) {
	for worker := range wks {
		fnc(worker)
	}
}

func (wks Workers) Parition(finder func(Worker) bool) (match []Worker, nmatch []Worker) {
	for worker := range wks {
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

	onJobStart func(Worker, Job) error
	onJobEnd   func(Worker, Job) error
	logger     *logrus.Entry
}

func (w *GenericWorker) ID() string    { return w.id }
func (w *GenericWorker) Perform(j Job) { w.c <- j }
func (w *GenericWorker) Stop() {
	if w.running {
		w.running = false
		close(w.quit)
	}
}
func (w *GenericWorker) IsIdle() bool {
	return !w.processing && time.Since(w.lastJobAt) > 30*time.Second
}

func (w *GenericWorker) handle(j Job) error {
	defer func() {
		w.processing = false

		if r := recover(); r != nil {
			w.logger.Errorln("recovered from panic:", r)
		}

		if w.onJobEnd != nil {
			if err := w.onJobEnd(w, j); err != nil {
				w.logger.Errorln("error in job end callback", err)
			}
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

func NewGenericWorker(config WorkerConfig) *GenericWorker {
	return &GenericWorker{
		id: config.ID,
		logger: config.Logger.WithFields(logrus.Fields{
			"worker.type":      "GenericWorker",
			"worker.id":        config.ID,
			"worker.startedAt": time.Now(),
		}),

		onJobStart: config.OnJobStart,
		onJobEnd:   config.OnJobEnd,
		c:          make(chan Job, 3),
		quit:       make(chan struct{}),
		running:    true,
		processing: false,
	}
}
