package eventsource

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/golly-go/golly"
)

const (
	projectionBatchSize = 100
)

// Add these interfaces to better define projection types
type BaseProjection interface {
	HandleEvent(*golly.Context, Event) error
	Position() int64
	SetPosition(pos int64) error
	Reset() error
}

type AggregateProjection interface {
	BaseProjection
	AggregateTypes() []string
}

type EventProjection interface {
	BaseProjection
	EventTypes() []string
}

// Optional combined interface for projections that handle both
type CombinedProjection interface {
	AggregateProjection
	EventProjection
}

type IDdProjection interface {
	ID() string
}

type Projection interface {
	HandleEvent(*golly.Context, Event) error
	// Position returns the last known position in the global event stream.
	Position() int64
	SetPosition(pos int64) error

	// Reset to clear state for rebuild
	Reset() error
}

// ProjectionBase is an embeddable helper for common Projection logic.
type ProjectionBase struct {
	position int64 // concurrency-safe via atomic
}

// Defaults: no filtering
func (p *ProjectionBase) AggregateTypes() []string { return nil }
func (p *ProjectionBase) EventTypes() []string     { return nil }

// Position/SetPosition with atomic
func (p *ProjectionBase) Position() int64 {
	return atomic.LoadInt64(&p.position)
}

func (p *ProjectionBase) SetPosition(pos int64) error {
	atomic.StoreInt64(&p.position, pos)
	return nil
}

// Reset sets position to -1
func (p *ProjectionBase) Reset() error {
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

// Register adds one or more projections to the manager.
func (pm *ProjectionManager) Register(projs ...Projection) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for _, proj := range projs {
		pm.projections[projectionKey(proj)] = proj
	}
}

// Rebuild resets a single projection, then processes all events from version 0 upward.
func (pm *ProjectionManager) Rebuild(ctx *golly.Context, eng *Engine, projID string) error {
	pm.mu.Lock()
	proj, ok := pm.projections[projID]
	pm.mu.Unlock()
	if !ok {
		return fmt.Errorf("projection %s not found", projID)
	}

	if err := proj.Reset(); err != nil {
		return err
	}
	// -1 means no events processed
	if err := proj.SetPosition(-1); err != nil {
		return err
	}

	return pm.processProjection(ctx, eng, proj, 0, projectionBatchSize)
}

// RunOnce catches up a single projection from its current position to the end.
func (pm *ProjectionManager) RunOnce(ctx *golly.Context, eng *Engine, projID string) error {
	pm.mu.Lock()
	proj, ok := pm.projections[projID]
	pm.mu.Unlock()

	if !ok {
		return fmt.Errorf("projection not found: %s", projID)
	}

	return pm.processProjection(ctx, eng, proj, int(proj.Position()+1), projectionBatchSize)
}

// RunToEnd catches up all registered projections from their current positions.
func (pm *ProjectionManager) RunToEnd(ctx *golly.Context, eng *Engine, projID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	proj, ok := pm.projections[projID]
	if !ok {
		return fmt.Errorf("projection not found: %s", projID)
	}

	// Reset projection state before running
	if err := proj.Reset(); err != nil {
		return fmt.Errorf("failed to reset projection: %w", err)
	}

	var lastError error
	err := eng.LoadEvents(ctx, 100, func(events []Event) error {
		for _, evt := range events {
			if err := proj.HandleEvent(ctx, evt); err != nil {
				lastError = err
				proj.SetPosition(-1) // Mark as failed
				return nil           // Stop processing but don't fail other projections
			}
			proj.SetPosition(evt.GlobalVersion) // Use GlobalVersion instead of Version
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
	ctx *golly.Context,
	eng *Engine,
	p Projection,
	fromGlobalVersion int,
	batchSize int,
) error {
	// Add context cancellation checks
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		// Continue processing
	}

	var aggregateTypes []string
	var eventTypes []string

	if et, ok := p.(EventProjection); ok {
		eventTypes = et.EventTypes()
	}

	if at, ok := p.(AggregateProjection); ok {
		aggregateTypes = at.AggregateTypes()
	}

	// Build the filter from projection
	filter := EventFilter{
		AggregateTypes:    aggregateTypes,
		EventType:         eventTypes,
		FromGlobalVersion: fromGlobalVersion,
	}

	return eng.LoadEvents(ctx, batchSize, func(events []Event) error {
		if len(events) == 0 {
			return nil
		}

		for i := range events {
			e := events[i]
			if err := p.HandleEvent(ctx, e); err != nil {
				return err
			}
		}

		return p.SetPosition(events[len(events)-1].GlobalVersion)
	}, filter)
}

func projectionKey(p Projection) string {
	if pi, ok := p.(IDdProjection); ok {
		return pi.ID()
	}
	return ObjectPath(p)
}

func projectionSteamConfig(p Projection) (aggs []string, evts []string) {
	if at, ok := p.(AggregateProjection); ok {
		aggs = at.AggregateTypes()
	}

	if et, ok := p.(EventProjection); ok {
		evts = et.EventTypes()
	}

	return
}
