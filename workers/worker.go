package workers

import (
	"sync"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

type Worker interface {
	Perform(golly.Context, interface{}, WorkerFunc)
	Stop()
	IsIdle() bool
}

type GenericWorker struct {
	id        string
	quit      chan struct{}
	startedAt time.Time
	lastJobAt time.Time
	logger    *logrus.Entry
	running   bool

	wg *sync.WaitGroup
}

func NewGenericWorker(id string, logger *logrus.Entry, wg *sync.WaitGroup) *GenericWorker {
	return &GenericWorker{
		id:        id,
		quit:      make(chan struct{}),
		startedAt: time.Now(),
		wg:        wg,

		logger: logger.WithFields(logrus.Fields{
			"worker.id":    id,
			"worker.start": time.Now(),
		}),
	}
}

func (w *GenericWorker) Perform(ctx golly.Context, job interface{}, handler WorkerFunc) {
	defer func() {
		w.wg.Done()
		w.running = false

		if r := recover(); r != nil {
			ctx.Logger().Errorln("recovered from panic:", r)
		}
	}()

	w.running = true
	w.wg.Add(1)

	w.lastJobAt = time.Now()
	handler(ctx, job)

	elapsed := time.Since(w.startedAt)

	w.
		logger.
		WithField("worker.duration", elapsed.Nanoseconds()).
		Infof("finished perform (%s)", elapsed.String())

}

func (w *GenericWorker) Stop() {}

func (w *GenericWorker) IsIdle() bool {
	return !w.running && time.Since(w.lastJobAt) > 30*time.Second
}
