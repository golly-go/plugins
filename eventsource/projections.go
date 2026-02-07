package eventsource

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"

	"github.com/golly-go/golly"
)

const (
	projectionBatchSize = 100
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
type ProjectionManager struct {
	mu          sync.RWMutex
	projections map[string]Projection // id -> projection

	eventHandlers map[string][]ProjectionHandler // eventType -> handlers
	topicHandlers map[string][]ProjectionHandler // topic -> handlers

	// Async processing with graceful shutdown
	jobs    chan Job
	stop    chan struct{}
	wg      sync.WaitGroup
	running atomic.Bool // Lockless check for running state
}

// NewProjectionManager creates a ProjectionManager with an empty registry.
func NewProjectionManager() *ProjectionManager {
	return &ProjectionManager{
		projections:   make(map[string]Projection),
		eventHandlers: make(map[string][]ProjectionHandler),
		topicHandlers: make(map[string][]ProjectionHandler),
		jobs:          make(chan Job, 1000), // Buffered for throughput
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

// Start begins async processing of projection events
func (pm *ProjectionManager) Start() {
	if pm.running.Swap(true) {
		return // Already running
	}

	pm.wg.Add(1)

	go pm.run()
}

// Stop gracefully shuts down projection processing, draining all in-flight events
func (pm *ProjectionManager) Stop() {
	if !pm.running.Swap(false) {
		return // Already stopped
	}

	trace("stopping projection manager, draining events")

	close(pm.stop)
	pm.wg.Wait() // Block until all events drained

	trace("projection manager stopped")
}

// run processes events in a single goroutine with drain on shutdown
func (pm *ProjectionManager) run() {
	defer pm.wg.Done()

	trace("starting projection manager")

	for {
		select {
		case job := <-pm.jobs:
			pm.handleEvent(job.Ctx, job.Event)

		case <-pm.stop:
			// Drain remaining events before shutdown
			trace("draining projection events")
			for {
				select {
				case job := <-pm.jobs:
					pm.handleEvent(job.Ctx, job.Event)
				default:
					trace("projection drain complete")
					return
				}
			}
		}
	}
}

// dispatch enqueues an event for async projection processing
func (pm *ProjectionManager) dispatch(ctx context.Context, evt Event) {
	if !pm.running.Load() {
		golly.DefaultLogger().Warnf("projection manager not running, dropping event")
		return
	}

	detached := golly.ToGollyContext(ctx).Detach()

	// Use background context for async processing to prevent cancellation
	// when the originating request completes
	select {
	case pm.jobs <- Job{Ctx: detached, Event: evt}:
		// Enqueued successfully
	default:
		golly.DefaultLogger().Errorf("projection queue full, dropping event")
	}
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
