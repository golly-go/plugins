package eventsource

import (
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

// --------------------------------------------------
// ERRORS & CONSTANTS
// --------------------------------------------------

var ErrQueueDraining = errors.New("queue is draining and not accepting new events")

const (
	defaultPartitions = 4
	defaultQueueSize  = 1000
)

// --------------------------------------------------
// AGGREGATOR STATE + POOL
// --------------------------------------------------

// aggregatorState tracks ordering & out-of-order buffering for one aggregate.
type aggregatorState struct {
	nextVersion  int64
	blockedSince time.Time
	buffered     map[int64]Event
}

// aggregatorStatePool reuses aggregatorState objects to reduce allocations.
var aggregatorStatePool = sync.Pool{
	New: func() interface{} {
		// Pre-initialize a small map to reduce expansions:
		return &aggregatorState{
			nextVersion: 1,
			buffered:    make(map[int64]Event, 8),
		}
	},
}

func acquireAggregatorState() *aggregatorState {
	agg := aggregatorStatePool.Get().(*aggregatorState)
	// Reset fields:
	agg.nextVersion = 1
	agg.blockedSince = time.Time{}
	// Clear the map without reallocating:
	for k := range agg.buffered {
		delete(agg.buffered, k)
	}
	return agg
}

func releaseAggregatorState(agg *aggregatorState) {
	aggregatorStatePool.Put(agg)
}

// --------------------------------------------------
// PARTITION WORKER
// --------------------------------------------------

type partitionWorker struct {
	// Single channel for events; closed on Stop().
	events chan Event

	// aggregatorMap is only accessed by this partition goroutine => no lock needed
	aggregatorMap map[string]*aggregatorState

	// user callback
	handler StreamHandler

	// time-based skipping
	blockedTimeout time.Duration

	logger *logrus.Logger
	wg     *sync.WaitGroup

	// drained ensures we only do final draining once
	drained int32
}

func newPartitionWorker(
	bufferSize int,
	blockedTimeout time.Duration,
	handler StreamHandler,
	wg *sync.WaitGroup,
	logger *logrus.Logger,
) *partitionWorker {
	return &partitionWorker{
		events:         make(chan Event, bufferSize),
		aggregatorMap:  make(map[string]*aggregatorState, 64),
		handler:        handler,
		blockedTimeout: blockedTimeout,
		logger:         logger,
		wg:             wg,
	}
}

// start spawns the partition worker goroutine
func (pw *partitionWorker) start() {
	pw.wg.Add(1)
	go pw.run()
}

// run processes incoming events + a ticker for blocked aggregator checks.
// This is the ONLY goroutine that touches aggregatorMap => no lock needed.
func (pw *partitionWorker) run() {
	defer pw.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case evt, ok := <-pw.events:
			if !ok {
				// channel closed -> final drain
				pw.drain()
				return
			}
			pw.processEvent(evt)

		case <-ticker.C:
			pw.checkBlockedAggregators()
		}
	}
}

// processEvent buffers an event in aggregatorMap, ensures ordering by flush.
func (pw *partitionWorker) processEvent(evt Event) {
	agg, exists := pw.aggregatorMap[evt.AggregateID]
	if !exists {
		agg = acquireAggregatorState()
		pw.aggregatorMap[evt.AggregateID] = agg
	}

	agg.buffered[evt.Version] = evt
	pw.flushAggregator(agg)
}

// flushAggregator processes consecutive versions from nextVersion upward.
func (pw *partitionWorker) flushAggregator(agg *aggregatorState) {
	for {
		e, found := agg.buffered[agg.nextVersion]
		if !found {
			// missing => aggregator is blocked
			if agg.blockedSince.IsZero() {
				agg.blockedSince = time.Now()
			}
			return
		}
		delete(agg.buffered, agg.nextVersion)
		agg.blockedSince = time.Time{}

		// user-defined handling
		pw.handler(e)
		agg.nextVersion++
	}
}

// checkBlockedAggregators times out missing versions & calls flush if skipping.
func (pw *partitionWorker) checkBlockedAggregators() {
	now := time.Now()

	for _, agg := range pw.aggregatorMap {
		if agg.blockedSince.IsZero() {
			continue
		}

		elapsed := now.Sub(agg.blockedSince)
		if elapsed > pw.blockedTimeout {
			// skip missing versions
			for {
				_, found := agg.buffered[agg.nextVersion]
				if found {
					break
				}
				agg.nextVersion++
			}
			agg.blockedSince = time.Time{}
			pw.flushAggregator(agg)
		}

		// Optionally remove aggregator if empty & flush done... domain choice
		// if len(agg.buffered) == 0 { ... }
	}
}

// drain empties the channel + aggregator states (once).
func (pw *partitionWorker) drain() {
	if !atomic.CompareAndSwapInt32(&pw.drained, 0, 1) {
		return
	}

	// read leftover events
	for evt := range pw.events {
		pw.processEvent(evt)
	}

	// final aggregator flush
	for aggID, agg := range pw.aggregatorMap {
		pw.flushAggregator(agg)
		// aggregator is now done - release to pool
		releaseAggregatorState(agg)

		delete(pw.aggregatorMap, aggID)
	}
}

// --------------------------------------------------
// STREAM QUEUE
// --------------------------------------------------

type StreamQueue struct {
	partitions []*partitionWorker
	numParts   uint32
	wg         sync.WaitGroup
	running    int32

	logger *logrus.Logger
}

type StreamQueueConfig struct {
	NumPartitions  uint32
	BufferSize     int
	BlockedTimeout time.Duration
	Handler        StreamHandler
	Logger         *logrus.Logger
}

// NewStreamQueue constructs the queue with config defaults & spawns partition workers.
func NewStreamQueue(cfg StreamQueueConfig) *StreamQueue {
	if cfg.NumPartitions == 0 {
		cfg.NumPartitions = defaultPartitions
	}
	if cfg.BufferSize == 0 {
		cfg.BufferSize = defaultQueueSize
	}
	if cfg.BlockedTimeout == 0 {
		cfg.BlockedTimeout = 3 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = golly.NewLogger()
	}

	sq := &StreamQueue{
		partitions: make([]*partitionWorker, cfg.NumPartitions),
		numParts:   cfg.NumPartitions,
		logger:     cfg.Logger,
	}

	for i := 0; i < int(cfg.NumPartitions); i++ {
		pw := newPartitionWorker(
			cfg.BufferSize,
			cfg.BlockedTimeout,
			cfg.Handler,
			&sq.wg,
			cfg.Logger,
		)
		sq.partitions[i] = pw
	}

	return sq
}

// Start runs each partition worker goroutine
func (sq *StreamQueue) Start() {
	if !atomic.CompareAndSwapInt32(&sq.running, 0, 1) {
		return
	}

	for i := range sq.partitions {
		sq.partitions[i].start()
	}
}

// Stop closes each partition's events channel & waits for them to drain.
func (sq *StreamQueue) Stop() {
	if !atomic.CompareAndSwapInt32(&sq.running, 1, 0) {
		return
	}

	// Close channels => each worker sees EOF, drains, flushes, returns
	for i := range sq.partitions {
		close(sq.partitions[i].events)
	}

	// Wait for all partition goroutines to finish
	sq.wg.Wait()
	sq.logger.Trace("All partitions stopped and drained")
}

// Enqueue inserts an event into the correct partition channel.
func (sq *StreamQueue) Enqueue(evt Event) error {
	if atomic.LoadInt32(&sq.running) != 1 {
		return ErrQueueDraining
	}

	part := sq.getPartition(evt.AggregateID)
	sq.partitions[part].events <- evt
	return nil
}

// getPartition picks a partition by hashing the AggregateID
func (sq *StreamQueue) getPartition(aggID string) uint32 {
	if aggID == "" {
		aggID = "no-agg-id"
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(aggID))
	return h.Sum32() % sq.numParts
}
