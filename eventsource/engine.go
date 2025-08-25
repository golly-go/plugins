package eventsource

import (
	"context"
	"fmt"
	"sync"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
)

// Engine is the main entry point for your event-sourced system.
// It contains:
//   - An EventStore for persistence
//   - A ProjectionManager for building read models
type Engine struct {
	store       EventStore
	projections *ProjectionManager
	aggregates  *AggregateRegistry

	streams *StreamManager

	mu      sync.RWMutex
	running bool
}

var defaultStream = NewStream(StreamOptions{Name: DefaultStreamName})

// NewEngine allows configuring the engine via Option by building a config first.
func NewEngine(opts ...Option) *Engine {
	cfg := handleOptions(opts...)
	eng := &Engine{
		store:       cfg.Store,
		projections: NewProjectionManager(),
		aggregates:  NewAggregateRegistry(),
		streams:     NewStreamManager(),
	}
	// Always include a default in-memory stream
	eng.streams.Add(defaultStream)

	for i := range cfg.Streams {
		eng.streams.Add(cfg.Streams[i])
	}

	return eng
}

// Store returns the underlying event store (if you need direct access)
func (eng *Engine) Store() EventStore { return eng.store }

// Projections returns the underlying projection manager
func (eng *Engine) Projections() *ProjectionManager { return eng.projections }

// Aggregates returns the underlying aggregate registry
func (eng *Engine) Aggregates() *AggregateRegistry { return eng.aggregates }

func (eng *Engine) IsRunning() bool {
	eng.mu.RLock()
	defer eng.mu.RUnlock()
	return eng.running
}

// WithStream adds external streams for outbound publish.
func (eng *Engine) WithStream(streams ...StreamPublisher) *Engine {
	eng.streams.Add(streams...)
	return eng
}

// On subscribes a handler to the first subscribable stream (default in-memory) to receive all events.
func (eng *Engine) On(handler StreamHandler) error {
	if ok := eng.streams.Subscribe(AllEvents, handler); !ok {
		return fmt.Errorf("no subscribable streams available")
	}
	return nil
}

// Subscribe Deprecated: Use On instead
func (eng *Engine) Subscribe(handler StreamHandler) error {
	return eng.On(handler)
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
	return eng.projections.RunOnce(ctx, eng, resolveInterfaceName(projection))
}

// RegisterProjection registers a projection
func (eng *Engine) RegisterProjection(proj Projection) error {
	eng.projections.Register(proj)

	handler := func(ctx context.Context, evt Event) {
		defer func() {
			if r := recover(); r != nil {
				golly.Logger().Errorf("panic in projection %s: %v", resolveInterfaceName(proj), r)
			}
		}()

		err := proj.HandleEvent(ctx, evt)
		if err != nil {
			golly.Logger().Errorf("error in projection %s: %v", resolveInterfaceName(proj), err)
		}
	}

	if ok := eng.streams.Subscribe(AllEvents, handler); !ok {
		return fmt.Errorf("no subscribable streams available")
	}

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
	agg.Changes().MarkComplete()

	return nil
}

// Execute handles command execution, including loading the aggregate, replaying events, validating, and persisting changes.
func (eng *Engine) Execute(ctx context.Context, agg Aggregate, cmd Command) (err error) {
	if eng == nil {
		return fmt.Errorf("engine is nil")
	}

	trace("executing command %s aggregate=%s aggregateID=%s", golly.TypeNoPtr(cmd).Name(), golly.TypeNoPtr(agg).Name(), agg.GetID())

	if err = eng.Replay(ctx, agg); err != nil {
		return handleExecutionError(ctx, agg, cmd, err)
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

	return eng.CommitAggregateChanges(ctx, agg)
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

// Send publishes events to all configured streams.
func (eng *Engine) Send(ctx context.Context, events ...Event) {
	for i := range events {
		if topic := events[i].Topic; topic != "" {
			eng.streams.Publish(ctx, topic, events[i])
		} else {
			trace("No topic for event aggregate=%s aggregateID=%s version=%d", events[i].AggregateType, events[i].AggregateID, events[i].Version)
		}
	}
}

// Start marks the engine running
func (eng *Engine) Start() {
	golly.Logger().Tracef("Starting engine")

	eng.mu.Lock()
	eng.running = true
	eng.mu.Unlock()

	if eng.streams != nil {
		eng.streams.Start()
	}
}

// Stop marks the engine stopped
func (eng *Engine) Stop() {
	golly.Logger().Tracef("Stopping engine")

	eng.mu.Lock()
	eng.running = false
	eng.mu.Unlock()

	if eng.streams != nil {
		eng.streams.Stop()
	}
}
