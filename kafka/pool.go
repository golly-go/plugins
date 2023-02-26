package kafka

import (
	"sync"
	"time"

	"github.com/golly-go/golly"
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
}

func NewPool(name string, min, max, buffer int, handler WorkerFunc) *WorkerPool {
	return &WorkerPool{
		minW:    min,
		maxW:    max,
		buffer:  buffer,
		handler: handler,
		quit:    make(chan struct{}),
		C:       make(chan interface{}, buffer),
	}
}

func (wp *WorkerPool) worker(ctx golly.Context, qc chan struct{}) {
	running := true

	wp.wg.Add(1)

	for wp.running && running {
		select {
		case <-qc:
			running = false
		case data := <-wp.C:
			wp.handler(ctx, data)
		}
	}

	wp.wg.Add(-1)
}

func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}

func (wp *WorkerPool) Stop() {
	close(wp.quit)
}

func (wp *WorkerPool) Spawn(ctx golly.Context) {
	wp.running = true
	quitChans := []chan struct{}{}

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()

	for i := 0; i < wp.minW; i++ {
		qc := make(chan struct{})
		quitChans = append(quitChans, qc)

		go wp.worker(ctx, qc)
	}

	for wp.running {
		select {
		case <-wp.quit:
			wp.running = false
		case <-ctx.Context().Done():
			wp.running = false
		case <-ticker.C:
			l := len(quitChans)

			if len(wp.C) >= cap(wp.C)/2 {
				if l < wp.maxW {
					qc := make(chan struct{})
					go wp.worker(ctx, qc)
				}
				break
			}

			if l > wp.minW {
				close(quitChans[l-1])
				quitChans = quitChans[:l-1]
			}
		}
	}

	for i := len(quitChans); i >= 0; i-- {
		close(quitChans[i])
	}
}
