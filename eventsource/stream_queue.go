package eventsource

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
)

// --------------------------------------------------
// ERRORS & CONSTANTS
// --------------------------------------------------

var ErrQueueDraining = errors.New("queue is draining and not accepting new events")

const (
	defaultPartitions = 4
	defaultQueueSize  = 1000
)

// --------------------------------------------------------------------
// JOB - ties together an Event + golly.Context
// --------------------------------------------------------------------

// Job wraps an event with its associated context
type Job struct {
	Ctx   *golly.Context
	Event Event
}

// --------------------------------------------------------------------
// aggregatorState
// --------------------------------------------------------------------

// aggregatorState tracks ordering & out-of-order buffering for one aggregate
type aggregatorState struct {
	nextVersion  int64
	blockedSince time.Time
	buffered     map[int64]Job
}

// newAggregatorState constructs a fresh aggregatorState
func newAggregatorState() *aggregatorState {
	return &aggregatorState{
		nextVersion: 1,
		buffered:    make(map[int64]Job, 8),
	}
}

// --------------------------------------------------------------------
// partitionWorker
// --------------------------------------------------------------------

type partitionWorker struct {
	id             string
	jobs           chan Job
	aggregatorMap  map[string]*aggregatorState
	handler        StreamHandler
	blockedTimeout time.Duration
	wg             *sync.WaitGroup

	drained int32
}

func newPartitionWorker(
	id string,
	bufferSize int,
	blockedTimeout time.Duration,
	handler StreamHandler,
	wg *sync.WaitGroup,
) *partitionWorker {
	return &partitionWorker{
		id:             id,
		jobs:           make(chan Job, bufferSize),
		aggregatorMap:  make(map[string]*aggregatorState, 64),
		handler:        handler,
		blockedTimeout: blockedTimeout,
		wg:             wg,
	}
}

func (pw *partitionWorker) start() {
	pw.wg.Add(1)
	go pw.run()
}

// run consumes events from pw.jobs in a single goroutine
func (pw *partitionWorker) run() {
	defer pw.wg.Done()

	golly.Logger().Tracef("partitionWorker %s started", pw.id)

	// Ticker for time-based skip
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case job, ok := <-pw.jobs:
			if !ok {
				pw.drain()
				return
			}

			pw.processJob(job)

		case <-ticker.C:
			pw.checkBlockedAggregators()
			// Optionally cleanup aggregator states that are empty
			pw.cleanupEmptyAggregators()
		}
	}
}

// processJob buffers the event, tries to flush
func (pw *partitionWorker) processJob(job Job) {
	evt := job.Event
	aggID := evt.AggregateID

	agg, exists := pw.aggregatorMap[aggID]
	if !exists {
		agg = newAggregatorState()
		pw.aggregatorMap[aggID] = agg
	}

	agg.buffered[evt.Version] = job
	pw.flushAggregator(agg)
}

// flushAggregator processes consecutive versions, sets blockedSince if missing
func (pw *partitionWorker) flushAggregator(agg *aggregatorState) {
	for {
		job, found := agg.buffered[agg.nextVersion]
		if !found {
			// missing => aggregator is blocked
			if agg.blockedSince.IsZero() {
				agg.blockedSince = time.Now()
			}
			return
		}
		// we have the event
		delete(agg.buffered, agg.nextVersion)
		agg.blockedSince = time.Time{}

		// call user-defined handler
		pw.handler(job.Ctx, job.Event)

		agg.nextVersion++
	}
}

// checkBlockedAggregators times out missing versions => skip one version
func (pw *partitionWorker) checkBlockedAggregators() {
	now := time.Now()
	for _, agg := range pw.aggregatorMap {
		if agg.blockedSince.IsZero() {
			continue
		}

		if now.Sub(agg.blockedSince) <= pw.blockedTimeout {
			continue
		}

		// skip exactly one version
		agg.nextVersion++
		agg.blockedSince = time.Time{}

		pw.flushAggregator(agg)
	}
}

// cleanupEmptyAggregators removes aggregator states that have no buffered items & are not blocked
func (pw *partitionWorker) cleanupEmptyAggregators() {
	for aggID, agg := range pw.aggregatorMap {
		if len(agg.buffered) == 0 && agg.blockedSince.IsZero() {
			// aggregator is idle, no events left => remove
			delete(pw.aggregatorMap, aggID)
		}
	}
}

// drain forcibly processes or skips all aggregator states
func (pw *partitionWorker) drain() {
	if !atomic.CompareAndSwapInt32(&pw.drained, 0, 1) {
		return
	}

	// read leftover items
	for job := range pw.jobs {
		pw.processJob(job)
	}

	// final flush or skip everything
	for aggID, agg := range pw.aggregatorMap {

		bufferKeys := make([]int, 0, len(agg.buffered))
		for k := range agg.buffered {
			bufferKeys = append(bufferKeys, int(k))
		}

		sort.Ints(bufferKeys)

		// Keep skipping or processing until aggregator is empty
		for _, v := range bufferKeys {

			// if we have nextVersion in buffer, process it
			job := agg.buffered[int64(v)]

			delete(agg.buffered, agg.nextVersion)

			pw.handler(job.Ctx, job.Event)
		}

		// aggregator is now fully processed or skipped
		delete(pw.aggregatorMap, aggID)
	}
}

// --------------------------------------------------------------------
// StreamQueue
// --------------------------------------------------------------------

type StreamQueue struct {
	name       string
	partitions []*partitionWorker
	numParts   uint32
	wg         sync.WaitGroup
	running    int32
}

type StreamQueueConfig struct {
	Name           string
	NumPartitions  uint32
	BufferSize     int
	BlockedTimeout time.Duration
	Handler        StreamHandler
}

// NewStreamQueue
func NewStreamQueue(cfg StreamQueueConfig) *StreamQueue {
	if cfg.NumPartitions == 0 {
		cfg.NumPartitions = defaultPartitions
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = defaultQueueSize
	}
	if cfg.BlockedTimeout == 0 {
		cfg.BlockedTimeout = 1 * time.Second
	}

	sq := &StreamQueue{
		partitions: make([]*partitionWorker, cfg.NumPartitions),
		numParts:   cfg.NumPartitions,
	}

	for i := 0; i < int(cfg.NumPartitions); i++ {
		pw := newPartitionWorker(fmt.Sprintf("%s-%d", cfg.Name, i), cfg.BufferSize, cfg.BlockedTimeout, cfg.Handler, &sq.wg)
		sq.partitions[i] = pw
	}

	return sq
}

func (sq *StreamQueue) Start() {
	if !atomic.CompareAndSwapInt32(&sq.running, 0, 1) {
		return
	}
	for _, pw := range sq.partitions {
		pw.start()
	}
}

func (sq *StreamQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&sq.running, 1, 0) {
		return
	}
	for _, pw := range sq.partitions {
		close(pw.jobs)
	}
	sq.wg.Wait()
}

func (sq *StreamQueue) Enqueue(ctx *golly.Context, evt Event) error {
	if atomic.LoadInt32(&sq.running) != 1 {
		return ErrQueueDraining
	}
	part := sq.getPartition(evt.AggregateID)
	sq.partitions[part].jobs <- Job{Ctx: ctx, Event: evt}
	return nil
}

func (sq *StreamQueue) getPartition(aggID string) uint32 {
	if aggID == "" {
		aggID = "no-agg-id"
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(aggID))
	return h.Sum32() % sq.numParts
}
