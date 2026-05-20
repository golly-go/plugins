package eventsource

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/golly-go/golly"
)

const (
	projectionBatchSize = 100

	// defaultProjectionWorkers is the number of parallel projection goroutines.
	// Events are routed by hash(AggregateID) % workers, so per-aggregate ordering
	// is preserved while independent aggregates project concurrently.
	defaultProjectionWorkers = 8
)

type IDdProjection interface {
	ID() string
}

type TopicsProjection interface {
	Topics() []string
}

type AggregateProjection interface {
	AggregateTypes() []any
}

type EventProjection interface {
	EventTypes() []any
}

type Projection interface {
	HandleEvent(context.Context, Event) error

	// Position returns the last known position in the global event stream.
	Position(context.Context) int64
	SetPosition(context.Context, int64) error

	// Reset to clear state for rebuild
	Reset(context.Context) error
}

type ProjectionHandler func(context.Context, Event) error

// ProjectionBase is an embeddable helper for common Projection logic.
type ProjectionBase struct {
	position int64 // concurrency-safe via atomic
}

// Position/SetPosition with atomic
func (p *ProjectionBase) Position(context.Context) int64 {
	return atomic.LoadInt64(&p.position)
}

func (p *ProjectionBase) SetPosition(ctx context.Context, pos int64) error {
	atomic.StoreInt64(&p.position, pos)
	return nil
}

// Reset sets position to -1
func (p *ProjectionBase) Reset(ctx context.Context) error {
	atomic.StoreInt64(&p.position, -1)
	return nil
}

// ProjectionManager manages multiple projections, each identified by a key.
//
// Events are dispatched to N hash-partitioned worker goroutines. Each worker
// owns a dedicated channel and processes events sequentially — so per-aggregate
// ordering is always guaranteed. Different aggregates fan out across workers and
// project concurrently, eliminating the head-of-line blocking that the original
// single-goroutine design caused under write bursts (e.g. graph nodes emitting
// 17 corpus texts + 11 entity creates in rapid succession).
type ProjectionManager struct {
	mu          sync.RWMutex
	projections map[string]Projection // id -> projection

	eventHandlers map[string][]ProjectionHandler // eventType -> handlers
	topicHandlers map[string][]ProjectionHandler // topic -> handlers

	// Hash-partitioned workers.
	workers    []chan Job
	numWorkers int

	stop    chan struct{}
	wg      sync.WaitGroup
	running atomic.Bool
}

// NewProjectionManager creates a ProjectionManager with defaultProjectionWorkers.
func NewProjectionManager() *ProjectionManager {
	return NewProjectionManagerWithWorkers(defaultProjectionWorkers)
}

// NewProjectionManagerWithWorkers creates a ProjectionManager with n workers.
func NewProjectionManagerWithWorkers(n int) *ProjectionManager {
	if n < 1 {
		n = 1
	}
	// Distribute the total buffer across workers; each gets at least 128 slots.
	perWorker := 1000 / n
	if perWorker < 128 {
		perWorker = 128
	}
	workers := make([]chan Job, n)
	for i := range workers {
		workers[i] = make(chan Job, perWorker)
	}
	return &ProjectionManager{
		projections:   make(map[string]Projection),
		eventHandlers: make(map[string][]ProjectionHandler),
		topicHandlers: make(map[string][]ProjectionHandler),
		workers:       workers,
		numWorkers:    n,
		stop:          make(chan struct{}),
	}
}

// List returns an iterator over all registered projections with their IDs.
// Snapshot is taken when List() is called. Iteration order is deterministic
// (sorted by ID). Compatible with Go 1.23+ range-over-func using iter.Seq2.
//
//	for id, proj := range manager.List() {
//	    fmt.Printf("Projection %s: %v\n", id, proj)
//	}
func (pm *ProjectionManager) List() iter.Seq2[string, Projection] {
	pm.mu.RLock()
	snap := make(map[string]Projection, len(pm.projections))
	for id, proj := range pm.projections {
		snap[id] = proj
	}
	pm.mu.RUnlock()

	return func(yield func(string, Projection) bool) {
		for id, proj := range snap {
			if !yield(id, proj) {
				return
			}
		}
	}
}

// Register adds one or more projections to the manager and indexes them by their filters.
func (pm *ProjectionManager) Register(projs ...Projection) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, proj := range projs {
		hasFilters := false

		key := projectionKey(proj)
		pm.projections[key] = proj

		fnc := func(ctx context.Context, event Event) error {
			defer func() {
				if r := recover(); r != nil {
					golly.DefaultLogger().Errorf("panic in projection %s: %v", resolveInterfaceName(proj), r)
				}
			}()
			return projectEvent(ctx, proj, event, true)
		}

		if ep, ok := proj.(EventProjection); ok {
			events := ep.EventTypes()
			for _, event := range events {
				pm.eventHandlers[ObjectName(event)] = append(pm.eventHandlers[ObjectName(event)], fnc)
			}
			hasFilters = true
		}

		var topics []string = projectionTopics(proj)
		if len(topics) > 0 {
			for pos := range topics {
				pm.topicHandlers[topics[pos]] = append(pm.topicHandlers[topics[pos]], fnc)
			}
			hasFilters = true
		}

		if !hasFilters {
			pm.eventHandlers[AllEvents] = append(pm.eventHandlers[AllEvents], fnc)
		}
	}
}

// Start begins async processing of projection events.
// One goroutine is launched per worker slot.
func (pm *ProjectionManager) Start() {
	if pm.running.Swap(true) {
		return // Already running
	}

	for i := range pm.workers {
		pm.wg.Add(1)
		go pm.runWorker(i)
	}
}

// Stop gracefully shuts down all workers, draining in-flight events before returning.
func (pm *ProjectionManager) Stop() {
	trace("stopped called in projection manager")

	if !pm.running.Swap(false) {
		return // Already stopped
	}

	trace("stopping projection manager, draining events")

	close(pm.stop)
	pm.wg.Wait()

	trace("projection manager stopped")
}

// runWorker processes events for one hash partition.
// Events are routed here by aggregateWorkerIndex so same-aggregate ordering
// is preserved within this goroutine while other workers run concurrently.
func (pm *ProjectionManager) runWorker(idx int) {
	defer func() {
		// Drain remaining events from this worker's channel before exiting.
		pm.drainWorker(pm.workers[idx])
		pm.wg.Done()
	}()

	trace("starting projection worker %d", idx)

	ch := pm.workers[idx]
	for {
		select {
		case job := <-ch:
			if job.wait != nil {
				close(job.wait)
				continue
			}
			pm.handleEvent(job.Ctx, job.Event)
		case <-pm.stop:
			return
		}
	}
}

// Wait blocks until all workers have processed every event that was enqueued
// before this call. A sentinel Job is sent to every worker; when all sentinels
// are acknowledged we know the queues are fully drained.
func (pm *ProjectionManager) Wait() {
	if !pm.running.Load() {
		return
	}

	var wg sync.WaitGroup
	for _, ch := range pm.workers {
		wg.Add(1)
		wait := make(chan struct{})
		ch <- Job{wait: wait}
		go func(w chan struct{}) {
			<-w
			wg.Done()
		}(wait)
	}
	wg.Wait()
}

func (pm *ProjectionManager) drainWorker(ch chan Job) {
	for {
		select {
		case job := <-ch:
			if job.wait != nil {
				close(job.wait)
				continue
			}
			pm.handleEvent(job.Ctx, job.Event)
		default:
			return
		}
	}
}

// dispatch routes an event to the worker responsible for the event's aggregate.
// Routing is deterministic: same AggregateID always maps to the same worker,
// preserving per-aggregate event ordering.
func (pm *ProjectionManager) dispatch(ctx context.Context, evt Event) {
	if !pm.running.Load() {
		golly.DefaultLogger().Warnf("projection manager not running, dropping event")
		return
	}

	detached := golly.ToGollyContext(ctx).Detach()
	idx := aggregateWorkerIndex(evt.AggregateID, pm.numWorkers)

	select {
	case pm.workers[idx] <- Job{Ctx: detached, Event: evt}:
	default:
		golly.DefaultLogger().Errorf("projection queue full (worker %d), dropping event", idx)
	}
}

// aggregateWorkerIndex returns a stable worker slot for the given aggregate ID.
// Uses FNV-32a so distribution is fast, uniform, and allocation-free.
func aggregateWorkerIndex(aggregateID string, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	h.Write([]byte(aggregateID))
	return int(h.Sum32()) % n
}

// handleEvent routes event to relevant projections using dual indexes
func (pm *ProjectionManager) handleEvent(ctx context.Context, evt Event) {
	// Copy handlers while holding lock, then release before processing
	pm.mu.RLock()

	var handlers []ProjectionHandler
	if h, ok := pm.eventHandlers[evt.Type]; ok {
		handlers = append(handlers, h...)
	}
	if h, ok := pm.topicHandlers[evt.Topic]; ok {
		handlers = append(handlers, h...)
	}
	if h, ok := pm.eventHandlers[AllEvents]; ok {
		handlers = append(handlers, h...)
	}
	pm.mu.RUnlock()

	// Process handlers without holding lock
	for pos := range handlers {
		if err := handlers[pos](ctx, evt); err != nil {
			golly.DefaultLogger().WithError(err).Errorf("error in projection handler %v: %#v", handlers[pos], evt)
		}
	}
}

// Get returns a projection by ID.
// Returns an error if the projection is not found.
// Returns nil if the projection is found.
// Returns nil if the projection is found.
func (pm *ProjectionManager) Get(projID string) (Projection, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	proj, ok := pm.projections[projID]
	if !ok {
		return nil, fmt.Errorf("projection %s not found", projID)
	}

	return proj, nil
}

// Rebuild resets a single projection, then processes all events from version 0 upward.
func (pm *ProjectionManager) Rebuild(ctx context.Context, eng *Engine, projID string) error {
	pm.mu.Lock()
	proj, ok := pm.projections[projID]
	pm.mu.Unlock()

	if !ok {
		return fmt.Errorf("projection %s not found", projID)
	}

	if err := proj.Reset(ctx); err != nil {
		return err
	}

	// -1 means no events processed
	if err := proj.SetPosition(ctx, -1); err != nil {
		return err
	}

	return pm.processProjection(ctx, eng, proj, 0, projectionBatchSize)
}

// RunToEnd catches up a single projection from its current position to the end of the stream.
func (pm *ProjectionManager) RunToEnd(ctx context.Context, eng *Engine, projID string) error {
	pm.mu.Lock()
	proj, ok := pm.projections[projID]
	pm.mu.Unlock()

	if !ok {
		return fmt.Errorf("projection not found: %s", projID)
	}

	// Start from the projection's current position + 1
	currentPos := proj.Position(ctx)
	startPos := int(currentPos + 1)

	// Use processProjection which handles filtering and position updates correctly
	return pm.processProjection(ctx, eng, proj, startPos, projectionBatchSize)
}

// processProjection loads events from 'fromGlobalVersion' in batches, calling p.HandleEvent,
// then does ONE p.SetPosition() at the very end (avoiding N+1 position updates).
func (pm *ProjectionManager) processProjection(
	ctx context.Context,
	eng *Engine,
	p Projection,
	fromGlobalVersion int,
	batchSize int,
) error {
	trace("Processing projection %s from version %d", resolveInterfaceName(p), fromGlobalVersion)
	filter := projectionFilters(p, fromGlobalVersion)

	var lastGlobalVersion int64 = -1

	err := eng.LoadEvents(ctx, batchSize, func(events []Event) error {
		if len(events) == 0 {
			return nil
		}

		// Process all events in batch (no position updates per event)
		for i := range events {
			if err := projectEvent(ctx, p, events[i], false); err != nil {
				return err
			}
		}

		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Track last processed version
		lastGlobalVersion = events[len(events)-1].GlobalVersion
		return nil
	}, filter)

	// Set position once at the end (only if we processed events)
	if lastGlobalVersion > -1 {
		if e := p.SetPosition(ctx, lastGlobalVersion); e != nil {
			err = errors.Join(err, e)
		}
	}

	return err
}

func projectionKey(p Projection) string {
	if pi, ok := p.(IDdProjection); ok {
		return pi.ID()
	}
	return ObjectPath(p)
}

func projectionFilters(p Projection, fromGlobalVersion int) EventFilter {
	filter := EventFilter{
		FromGlobalVersion: fromGlobalVersion,
	}

	if tp, ok := p.(TopicsProjection); ok {
		filter.Topics = tp.Topics()
	}

	if ap, ok := p.(AggregateProjection); ok {
		filter.AggregateTypes = golly.Map(ap.AggregateTypes(), ObjectName)
	}
	return filter
}

func projectionTopics(p Projection) []string {
	var topics []string
	if tp, ok := p.(TopicsProjection); ok {
		topics = append(topics, tp.Topics()...)
	}

	if ap, ok := p.(AggregateProjection); ok {
		topics = append(topics, golly.Map(ap.AggregateTypes(), NameToTopicUnicode)...)
	}

	return topics
}

// eventTopics determines what topics an event should be published to
func eventTopic(evt Event) string {

	// If the event has a Topic field, use it
	if evt.Topic != "" {
		return evt.Topic
	}

	// Generate topic from AggregateType
	if evt.AggregateType != "" {
		return NameToTopicUnicode(evt.AggregateType)
	}

	return ""
}

func projectEvent(ctx context.Context, proj Projection, evt Event, setPosition bool) error {
	if err := proj.HandleEvent(ctx, evt); err != nil {
		return err
	}
	if setPosition {
		return proj.SetPosition(ctx, evt.GlobalVersion)
	}
	return nil
}
