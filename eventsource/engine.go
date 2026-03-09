package eventsource

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
)

// Engine is the main entry point for your event-sourced system.
// It contains:
//   - An EventStore for persistence
//   - A ProjectionManager for building read models
//   - A StreamManager for all event publishing (internal projections and external producers)
type Engine struct {
	store       EventStore
	projections *ProjectionManager
	aggregates  *AggregateRegistry
	streams     *StreamManager

	mu      sync.RWMutex
	running bool
}

// NewEngine allows configuring the engine via Option by building a config first.
func NewEngine(opts ...Option) *Engine {
	cfg := handleOptions(opts...)

	streams := NewStreamManager()

	// Add external producers (like Kafka)
	for i := range cfg.Streams {
		streams.Add(cfg.Streams[i])
	}

	eng := &Engine{
		store:       cfg.Store,
		projections: NewProjectionManager(),
		aggregates:  NewAggregateRegistry(),
		streams:     streams,
	}

	return eng
}

// Store returns the underlying event store (if you need direct access)
func (eng *Engine) Store() EventStore       { return eng.store }
func (eng *Engine) Streams() *StreamManager { return eng.streams }

// Projections returns the underlying projection manager
func (eng *Engine) Projections() *ProjectionManager { return eng.projections }

// Aggregates returns the underlying aggregate registry
func (eng *Engine) Aggregates() *AggregateRegistry { return eng.aggregates }

func (eng *Engine) IsRunning() bool {
	eng.mu.RLock()
	defer eng.mu.RUnlock()
	return eng.running
}

// WithStream adds streams for event publishing (internal or external)
func (eng *Engine) WithStream(streams ...StreamPublisher) *Engine {
	eng.streams.Add(streams...)
	return eng
}

// nextGlobalVersion returns the next global version
func (eng *Engine) nextGlobalVersion(ctx context.Context) (int64, error) {
	return eng.store.IncrementGlobalVersion(ctx)
}

func (eng *Engine) RegisterAggregate(agg Aggregate, events []any, opts ...Option) error {
	eng.aggregates.Register(agg, events)
	return nil
}

// RebuildProjection rebuilds a single projection
func (eng *Engine) RebuildProjection(ctx context.Context, projection any) error {
	return eng.projections.Rebuild(ctx, eng, resolveInterfaceName(projection))
}

// RunProjectionToEnd runs the projection to the end of the event stream
func (eng *Engine) RunProjectionToEnd(ctx context.Context, projection any) error {
	return eng.projections.RunToEnd(ctx, eng, resolveInterfaceName(projection))
}

// RunProjectionOnce runs the projection once
func (eng *Engine) RunProjectionOnce(ctx context.Context, projection any) error {
	return eng.projections.RunToEnd(ctx, eng, resolveInterfaceName(projection))
}

// RegisterProjection registers a projection to the stream manager
// RegisterProjection registers a projection with the projection manager
func (eng *Engine) RegisterProjection(proj Projection) error {
	eng.projections.Register(proj)
	return nil
}

// CommitAggregateChanges applies a series of new events from an aggregate:
//  1. Increments a global version.
//  2. Saves the events to the event store.
//  3. Publishes them to the bus.
//  4. Marks the aggregate's changes as complete.
func (eng *Engine) CommitAggregateChanges(ctx context.Context, agg Aggregate) error {
	changes := agg.Changes().Uncommitted()

	for i := range changes {
		version, err := eng.nextGlobalVersion(ctx)
		if err != nil {
			return err
		}

		changes[i].GlobalVersion = version
	}

	if err := eng.store.Save(ctx, changes.Ptr()...); err != nil {
		return err
	}

	eng.Send(ctx, changes...)
	agg.ClearChanges()

	return nil
}

// Execute handles command execution: replay → validate → perform → process → commit.
//
// If CommitAggregateChanges returns ErrVersionConflict (a concurrent writer raced in
// between Replay and Save), Execute will re-replay to pick up the interleaved events,
// re-number the already-computed pending events, and retry the save — without calling
// Perform again. This is safe for commands with external side effects (e.g. Stripe API
// calls) as Perform is guaranteed to run exactly once.
func (eng *Engine) Execute(ctx context.Context, agg Aggregate, cmd Command) (err error) {
	if eng == nil {
		return fmt.Errorf("engine is nil")
	}

	trace("executing command %s aggregate=%s aggregateID=%s", golly.TypeNoPtr(cmd).Name(), golly.TypeNoPtr(agg).Name(), agg.GetID())

	// === Phase 1: run once — replay, validate, perform, process ===
	if HasValidID(agg) {
		if err = eng.Replay(ctx, agg); err != nil {
			return handleExecutionError(ctx, agg, cmd, err)
		}
	}

	if v, ok := cmd.(CommandValidator); ok {
		if err = v.Validate(ctx, agg); err != nil {
			return handleExecutionError(ctx, agg, cmd, err)
		}
	}

	if err = cmd.Perform(ctx, agg); err != nil {
		return handleExecutionError(ctx, agg, cmd, err)
	}

	if err = agg.ProcessChanges(ctx, agg); err != nil {
		return handleExecutionError(ctx, agg, cmd, err)
	}

	if agg.GetID() == "" {
		return handleExecutionError(ctx, agg, cmd, ErrorNoAggregateID)
	}

	// === Phase 2: commit, retrying only the save on version conflict ===
	const maxSaveRetries = 3
	for attempt := 0; attempt < maxSaveRetries; attempt++ {
		err = eng.CommitAggregateChanges(ctx, agg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrVersionConflict) {
			return err
		}
		if attempt == maxSaveRetries-1 {
			break
		}

		// A concurrent writer (e.g. a Stripe webhook) raced in between our Replay
		// and Save. Capture the pending events, re-replay to get the latest aggregate
		// version, re-number those events, and retry — without calling Perform again.
		pending := agg.Changes().Uncommitted()
		agg.ClearChanges()

		trace("version conflict on attempt %d for %s %s, re-versioning and retrying",
			attempt+1, golly.TypeNoPtr(agg).Name(), agg.GetID())

		if e := eng.Replay(ctx, agg); e != nil {
			return e
		}

		base := agg.Version()
		for i := range pending {
			base++
			pending[i].Version = base
		}
		agg.SetChanges(pending)
		agg.SetVersion(base)
	}

	return err
}

func (eng *Engine) Replay(ctx context.Context, agg Aggregate) error {
	// Quick exits: invalid ID or uncommitted changes
	id := agg.GetID()
	if id == "" || id == uuid.Nil.String() || id == "0" {
		return nil
	}
	if len(agg.Changes()) > 0 {
		return nil
	}

	agg.ClearChanges()

	snap, err := eng.store.LoadSnapshot(ctx, ObjectName(agg), agg.GetID())
	if err == nil && snap != nil {
		if sEvent, err := snap.Hydrate(eng); err == nil {
			// Snapshot loaded & hydrated successfully
			if err := ApplySnapshot(agg, sEvent); err != nil {
				return err
			}
		}
	}

	// 2) Load remaining events from the store
	return eng.store.LoadEventsInBatches(ctx, 100,
		func(pEvents []PersistedEvent) error {
			for i := range pEvents {
				e, err := pEvents[i].Hydrate(eng)
				if err != nil {
					return err
				}

				if err := agg.ReplayOne(agg, e); err != nil {
					return err
				}
			}

			return nil
		},
		EventFilter{
			AggregateType: ObjectName(agg),
			AggregateID:   id,
			FromVersion:   int(agg.Version()) + 1,
		})
}

func (eng *Engine) LoadEvents(
	ctx context.Context,
	batchSize int,
	handle func(events []Event) error,
	filter ...EventFilter,
) error {
	return eng.store.LoadEventsInBatches(ctx, batchSize, func(rawBatch []PersistedEvent) error {
		if len(rawBatch) == 0 {
			return nil
		}

		batch := make([]Event, len(rawBatch))

		for pos := range rawBatch {
			evt, err := rawBatch[pos].Hydrate(eng)
			if err != nil {
				return err
			}
			batch[pos] = evt
		}

		return handle(batch)
	}, filter...)
}

func (eng *Engine) LoadEventTypes(ctx context.Context, batchSize int, eventTypes ...any) (events []Event, err error) {
	types := make([]string, len(eventTypes))
	for i := range eventTypes {
		if t, ok := eventTypes[i].(string); ok {
			types[i] = t
			continue
		}
		types[i] = golly.TypeNoPtr(eventTypes[i]).String()
	}

	var result []Event

	err = eng.LoadEvents(ctx, batchSize, func(events []Event) error {
		result = append(result, events...)
		return nil
	}, EventFilter{EventType: types})

	return result, err
}

func (eng *Engine) Load(ctx context.Context, agg Aggregate) error {
	if err := eng.Replay(ctx, agg); err != nil {
		return err
	}

	if a, ok := agg.(NewRecordChecker); ok {
		if a.IsNewRecord() {
			return ErrorNoEventsFound
		}
	}

	return nil
}

// handleExecutionError processes errors and rolls back if necessary
func handleExecutionError(ctx context.Context, agg Aggregate, cmd Command, err error) error {
	if agg != nil {
		agg.SetChanges(agg.Changes().MarkFailed())
	}

	if r, ok := cmd.(CommandRollback); ok {
		r.Rollback(ctx, agg, err)
	}

	return err
}

// Send publishes events to all registered streams (internal projections and external producers)
func (eng *Engine) Send(ctx context.Context, events ...Event) {
	// Dispatch to projections first (internal)
	for i := range events {
		eng.projections.dispatch(ctx, events[i])
	}

	// Then publish to external streams (Kafka, etc)
	for i := range events {
		topic := eventTopic(events[i])
		if topic == "" {
			continue
		}
		eng.streams.Publish(ctx, topic, events[i])
	}
}

// Start marks the engine running and starts projections and streams
func (eng *Engine) Start() {
	golly.DefaultLogger().Tracef("Starting engine")

	eng.mu.Lock()
	eng.running = true
	eng.mu.Unlock()

	eng.projections.Start() // Start projection async processing
}

// Stop stops the engine, gracefully draining all projection events
func (eng *Engine) Stop() {
	golly.DefaultLogger().Tracef("Stopping engine")

	eng.mu.Lock()
	eng.running = false
	eng.mu.Unlock()

	eng.projections.Stop() // Drain all projection events first

	SetUserInfoFunc(nil) // for now
}

func HasValidID(agg Aggregate) bool {
	return agg.GetID() != "" && agg.GetID() != uuid.Nil.String() && agg.GetID() != "0"
}
