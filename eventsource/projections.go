package eventsource

import (
	"context"
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

type Projection interface {
	HandleEvent(context.Context, Event) error

	// Position returns the last known position in the global event stream.
	Position(context.Context) int64
	SetPosition(context.Context, int64) error

	// Reset to clear state for rebuild
	Reset(context.Context) error
}

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
	projections map[string]Projection
}

// NewProjectionManager creates a ProjectionManager with an empty registry.
func NewProjectionManager() *ProjectionManager {
	return &ProjectionManager{
		projections: make(map[string]Projection),
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

// Register adds one or more projections to the manager.
func (pm *ProjectionManager) Register(projs ...Projection) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, proj := range projs {
		pm.projections[projectionKey(proj)] = proj
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

// RunOnce catches up a single projection from its current position to the end.
func (pm *ProjectionManager) RunOnce(ctx context.Context, eng *Engine, projID string) error {
	pm.mu.Lock()
	proj, ok := pm.projections[projID]
	pm.mu.Unlock()

	if !ok {
		return fmt.Errorf("projection not found: %s", projID)
	}

	return pm.processProjection(ctx, eng, proj, int(proj.Position(ctx)+1), projectionBatchSize)
}

// RunToEnd catches up all registered projections from their current positions.
func (pm *ProjectionManager) RunToEnd(ctx context.Context, eng *Engine, projID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	proj, ok := pm.projections[projID]
	if !ok {
		return fmt.Errorf("projection not found: %s", projID)
	}

	// Reset projection state before running
	if err := proj.Reset(ctx); err != nil {
		return fmt.Errorf("failed to reset projection: %w", err)
	}

	var lastError error
	err := eng.LoadEvents(ctx, 100, func(events []Event) error {
		for _, evt := range events {
			if err := proj.HandleEvent(ctx, evt); err != nil {
				lastError = err
				proj.SetPosition(ctx, -1) // Mark as failed
				return nil                // Stop processing but don't fail other projections
			}
			proj.SetPosition(ctx, evt.GlobalVersion) // Use GlobalVersion instead of Version
		}
		return nil
	})

	if err != nil {
		return err
	}
	return lastError
}

// processProjection loads events from 'fromGlobalVersion' in batches, calling p.HandleEvent,
// then does ONE p.SetPosition() per batch.
func (pm *ProjectionManager) processProjection(
	ctx context.Context,
	eng *Engine,
	p Projection,
	fromGlobalVersion int,
	batchSize int,
) error {

	golly.Logger().Tracef("Processing projection %s", resolveInterfaceName(p))
	filter := projectionFilters(p, fromGlobalVersion)

	return eng.LoadEvents(ctx, batchSize, func(events []Event) error {
		if len(events) == 0 {
			return nil
		}

		for i := range events {
			if err := p.HandleEvent(ctx, events[i]); err != nil {
				return err
			}
		}

		// Add context cancellation checks
		select {
		case <-ctx.Done():
			return p.SetPosition(ctx, events[len(events)-1].GlobalVersion)
		default:
			// Continue processing
		}

		return p.SetPosition(ctx, events[len(events)-1].GlobalVersion)
	}, filter)
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
