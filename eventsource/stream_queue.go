package eventsource

import (
	"errors"
	"sort"
	"sync"

	"github.com/cespare/xxhash/v2" // Fast hash algorithm, good for UUIDs
	"github.com/golly-go/golly"
)

const (
	defaultPartitions = 8
	defaultQueueSize  = 1000
)

// aggregatorState tracks the processing state for a single aggregate
type aggregatorState struct {
	nextVersion int64               // Next expected version
	buffer      map[int64]queueItem // Buffered events by version
	mu          sync.Mutex          // Protects buffer and nextVersion
}

func newAggregatorState() *aggregatorState {
	return &aggregatorState{
		nextVersion: 0,
		buffer:      make(map[int64]queueItem),
	}
}

type streamQueue struct {
	numParts   uint32
	partitions []chan queueItem
	handler    StreamHandler
	done       chan struct{}
	wg         sync.WaitGroup
	mu         sync.RWMutex
	running    bool
}

type queueItem struct {
	ctx         *golly.Context
	event       Event
	partitionID uint32
}

// getPartition determines partition using AggregateID for consistent ordering
func (q *streamQueue) getPartition(evt Event) uint32 {
	// If no AggregateID, fallback to Type to maintain consistency
	id := evt.AggregateID
	if id == "" {
		id = evt.Type
	}

	h := xxhash.New()
	h.WriteString(id)
	return uint32(h.Sum64() % uint64(q.numParts))
}

// Allow users to configure number of partitions
type StreamQueueConfig struct {
	NumPartitions uint32
	BufferSize    int
	Handler       StreamHandler
}

func newStreamQueue(opts StreamQueueConfig) *streamQueue {
	if opts.NumPartitions == 0 {
		opts.NumPartitions = defaultPartitions
	}
	if opts.BufferSize == 0 {
		opts.BufferSize = defaultQueueSize
	}

	q := &streamQueue{
		numParts:   opts.NumPartitions,
		partitions: make([]chan queueItem, opts.NumPartitions),
		done:       make(chan struct{}),
		handler:    opts.Handler,
	}

	// Initialize partition channels
	for i := range q.partitions {
		q.partitions[i] = make(chan queueItem, opts.BufferSize)
	}

	return q
}

func (q *streamQueue) start() {
	q.mu.Lock()
	if q.running {
		q.mu.Unlock()
		return
	}
	q.running = true
	q.mu.Unlock()

	for i := range q.partitions {
		q.wg.Add(1)
		go func(partID int) {
			defer q.wg.Done()
			q.runPartition(partID)
		}(i)
	}
}

func (q *streamQueue) runPartition(partID int) {
	// Track state for each aggregate in this partition
	aggregators := make(map[string]*aggregatorState)

	for {
		select {
		case <-q.done:
			q.drainPartition(partID, aggregators)
			return
		case item, ok := <-q.partitions[partID]:
			if !ok {
				return
			}
			if q.handler == nil {
				continue
			}
			q.processItem(partID, item, aggregators)
		}
	}
}

// processItem handles a single event, managing ordering within its aggregate
func (q *streamQueue) processItem(partID int, item queueItem, aggregators map[string]*aggregatorState) {
	aggID := item.event.AggregateID
	if aggID == "" {
		// For non-aggregate events, process immediately
		item.event.partitionID = uint32(partID)
		q.handler(item.ctx, item.event)
		return
	}

	agg, exists := aggregators[aggID]
	if !exists {
		agg = newAggregatorState()
		aggregators[aggID] = agg
	}

	agg.mu.Lock()
	defer agg.mu.Unlock()

	// Buffer the event
	agg.buffer[item.event.Version] = item
	q.processBufferedEvents(partID, agg)
}

// processBufferedEvents processes any events that are ready in version order
func (q *streamQueue) processBufferedEvents(partID int, agg *aggregatorState) {
	for {
		if next, ok := agg.buffer[agg.nextVersion]; ok {
			next.event.partitionID = uint32(partID)
			q.handler(next.ctx, next.event)
			delete(agg.buffer, agg.nextVersion)
			agg.nextVersion++
		} else {
			break
		}
	}
}

// drainPartition processes remaining events in the partition
func (q *streamQueue) drainPartition(partID int, aggregators map[string]*aggregatorState) {
	// First drain the channel
	for {
		select {
		case item, ok := <-q.partitions[partID]:
			if !ok {
				break
			}
			if q.handler != nil {
				q.processItem(partID, item, aggregators)
			}
		default:
			goto drainBuffers
		}
	}

drainBuffers:
	// Then drain all buffered events
	for _, agg := range aggregators {
		agg.mu.Lock()
		// Process all remaining events in version order
		versions := make([]int64, 0, len(agg.buffer))
		for version := range agg.buffer {
			versions = append(versions, version)
		}
		// Sort versions to process in order
		sort.Slice(versions, func(i, j int) bool {
			return versions[i] < versions[j]
		})

		// Process all buffered events
		for _, version := range versions {
			item := agg.buffer[version]
			item.event.partitionID = uint32(partID)
			q.handler(item.ctx, item.event)
			delete(agg.buffer, version)
		}
		agg.mu.Unlock()
	}
}

func (q *streamQueue) enqueue(ctx *golly.Context, evt Event) error {
	select {
	case <-q.done:
		return ErrQueueDraining
	default:
		partID := q.getPartition(evt)
		select {
		case q.partitions[partID] <- queueItem{
			ctx:         ctx,
			event:       evt,
			partitionID: partID,
		}:
			return nil
		case <-q.done:
			return ErrQueueDraining
		}
	}
}

func (q *streamQueue) stop() {
	q.mu.Lock()
	if !q.running {
		q.mu.Unlock()
		return
	}
	q.running = false
	close(q.done)
	q.mu.Unlock()

	q.wg.Wait()

	// Close all partition channels
	for i := range q.partitions {
		close(q.partitions[i])
	}
}

// Errors
var ErrQueueDraining = errors.New("queue is draining and not accepting new events")

// SetHandler sets the event handler
func (q *streamQueue) SetHandler(h StreamHandler) {
	q.handler = h
}
