package workers

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

type Pool interface {
	Name() string
	Run(golly.Context)
	Stop()
	Wait()
	Spawn(golly.Context) Worker
	Handler() WorkerFunc

	NewWorker(golly.Context, string) Worker

	EnQueue(golly.Context, interface{}) error
}

type WorkerFunc func(golly.Context, interface{}) error

type GenericPool struct{ PoolBase }

type PoolBase struct {
	ctx golly.Context

	workers []Worker
	lock    sync.RWMutex

	handler WorkerFunc

	name    string
	quit    chan struct{}
	running bool

	wg sync.WaitGroup

	minW int32
	maxW int32

	logger *logrus.Entry

	activeWorkers atomic.Int32
}

func (pb *PoolBase) NewWorker(ctx golly.Context, id string) Worker {
	cnt := pb.activeWorkers.Add(1)

	return NewGenericWorker(WorkerConfig{
		ID:         fmt.Sprintf("%s-%06d", pb.name, cnt),
		Logger:     pb.logger,
		OnJobStart: func(w Worker, j Job) error { pb.wg.Add(1); return nil },
		OnJobEnd:   func(w Worker, j Job) error { pb.wg.Done(); pb.Checkin(w); return nil },
	})
}

func (pb *PoolBase) Name() string        { return pb.name }
func (pb *PoolBase) Handler() WorkerFunc { return pb.handler }

// TODO Figure out how to better handle this wait in the checkin/checkout system
func (pb *PoolBase) Wait() { pb.wg.Wait() }

func (pb *PoolBase) Spawn(ctx golly.Context) Worker { w, _ := pb.Checkout(); return w }

func (pb *PoolBase) EnQueue(ctx golly.Context, job interface{}) error {
	worker, err := pb.Checkout()
	if err != nil {
		return err
	}

	worker.Perform(Job{ctx, job, pb.handler})
	return nil
}

func (pb *PoolBase) Checkout() (Worker, error) {
	if !pb.running {
		return nil, fmt.Errorf("terminating")
	}

	for len(pb.workers) == 0 {
		start := time.Now()

		for pb.activeWorkers.Load()+1 > pb.maxW {
			if time.Since(start) > 10*time.Second {
				return nil, fmt.Errorf("wait time exeeded")
			}
			time.Sleep(5 * time.Millisecond)
		}

		if worker := pb.NewWorker(pb.ctx, "worker"); worker != nil {
			return worker, nil
		}
	}

	pb.lock.Lock()
	defer pb.lock.Unlock()

	index := len(pb.workers) - 1
	worker := pb.workers[index]

	pb.workers = pb.workers[:index]

	return worker, nil
}

func (pb *PoolBase) Checkin(worker Worker) {
	pb.lock.Lock()
	defer pb.lock.Unlock()

	pb.workers = append(pb.workers, worker)
}

func (pb *PoolBase) Stop() {
	close(pb.quit)
}

func (pb *PoolBase) reap() (reaped int32) {
	active := int32(pb.activeWorkers.Load())

	if active > pb.minW {
		for pos, worker := range pb.workers {
			if worker.IsIdle() {
				reaped++

				worker.Stop()

				pb.workers = append(pb.workers[:pos], pb.workers[pos+1:]...)
			}

			if active-reaped <= pb.minW {
				pb.activeWorkers.Add(-reaped)
				break
			}

		}
	}
	return
}

func (pb *PoolBase) Run(ctx golly.Context) {
	pb.running = true

	pb.logger = ctx.Logger().WithFields(logrus.Fields{
		"spawner": pb.Name,
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

		case <-heartbeat.C:
			if reaped := pb.reap(); reaped > 0 {
				pb.logger.Debugf("%s: repead %d workers", pb.name, reaped)
			}
		}
	}

	pb.wg.Wait()

	pb.maxW = 0
	pb.reap()

}

func NewGenericPool(name string, min, max int32, handler WorkerFunc) Pool {
	return &GenericPool{
		PoolBase: PoolBase{
			name:    name,
			minW:    min,
			maxW:    max,
			handler: handler,
			quit:    make(chan struct{}),
		},
	}
}

var _ Pool = &PoolBase{}
