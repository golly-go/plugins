package kafka

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

type WorkerFunc func(golly.Context, interface{}) error

type WorkerPool struct {
	Name string
	C    chan interface{}
	quit chan struct{}

	running bool

	handler WorkerFunc

	wg sync.WaitGroup

	minW   int
	maxW   int
	buffer int

	logger *logrus.Entry

	workercnt atomic.Int32
}

type Worker struct {
	id        string
	quit      chan struct{}
	startedAt time.Time
	lastJobAt time.Time
	logger    *logrus.Entry
}

func NewWorker(id string, logger *logrus.Entry) *Worker {
	return &Worker{
		id:        id,
		quit:      make(chan struct{}),
		startedAt: time.Now(),
		logger: logger.WithFields(logrus.Fields{
			"worker.id":    id,
			"worker.start": time.Now(),
		}),
	}
}

func (w *Worker) Run(ctx golly.Context, jobs chan interface{}, handler WorkerFunc) {
	w.logger.Infof("starting worker process")

	defer func() {
		if r := recover(); r != nil {
			ctx.Logger().Errorln("recovered from panic:", r)
		}
	}()

	var running = true
	for running {
		select {
		case <-w.quit:
			running = false
		case data := <-jobs:
			handler(ctx, data)
			w.lastJobAt = time.Now()
		}
	}

	elapsed := time.Since(w.startedAt)

	w.
		logger.
		WithField("worker.duration", elapsed.Nanoseconds()).
		Infof("stopping worker process (%s)", elapsed.String())

}

func (w *Worker) Stop() {
	close(w.quit)
}

func (w *Worker) IsIdle() bool {
	return time.Since(w.lastJobAt) > 30*time.Second
}

func NewPool(name string, min, max, buffer int, handler WorkerFunc) *WorkerPool {
	return &WorkerPool{
		Name:    name,
		minW:    min,
		maxW:    max,
		buffer:  buffer,
		handler: handler,
		quit:    make(chan struct{}),
		C:       make(chan interface{}, buffer),
	}
}

func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

func (wp *WorkerPool) Stop() {
	close(wp.quit)
}

func (wp *WorkerPool) SpawnWorker(ctx golly.Context) *Worker {
	cnt := wp.workercnt.Add(1)

	worker := NewWorker(fmt.Sprintf("%s-%06d", wp.Name, cnt), ctx.Logger())

	go worker.Run(ctx, wp.C, wp.handler)

	return worker
}

func (wp *WorkerPool) Spawn(ctx golly.Context) {
	wp.running = true
	workers := []*Worker{}

	wp.logger = ctx.Logger().WithFields(logrus.Fields{
		"spawner": wp.Name,
	})

	stop := time.NewTicker(500 * time.Millisecond)
	start := time.NewTicker(1 * time.Second)

	defer stop.Stop()
	defer start.Stop()

	for i := 0; i < wp.minW; i++ {
		workers = append(workers, wp.SpawnWorker(ctx))
	}

	wp.logger.Infof("starting spawner %s", wp.Name)
	defer func() {
		wp.logger.Infof("ending spawner %s", wp.Name)
	}()

	for wp.running {
		select {
		case <-wp.quit:
			wp.logger.Debug("stopping quit channel")
			wp.running = false
		case <-ctx.Context().Done():
			wp.logger.Debug("stopping context done")

			wp.running = false

		case <-start.C:
			l := len(workers)

			if len(wp.C) >= (cap(wp.C)/wp.maxW)*l {
				if l < wp.maxW {
					workers = append(workers, wp.SpawnWorker(ctx))
				}
			}
		case <-stop.C:
			l := len(workers)

			if len(wp.C) < (cap(wp.C)/wp.maxW)*l {
				if l > wp.minW {
					if workers[l-1].IsIdle() {
						workers[l-1].Stop()
						workers = workers[:l-1]
					}
				}

			}
		}
	}

	for i := len(workers) - 1; i >= 0; i-- {
		workers[i].Stop()
	}
}
