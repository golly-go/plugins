package eventsource

import (
	"context"
	"fmt"
	"sync"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
)

var (
	defaultStreamOptions = StreamOptions{
		Name: DefaultStreamName,
	}
)

// Engine is the main entry point for your event-sourced system.
// It contains:
//   - An EventStore for persistence
//   - A StreamManager for live pub-sub
//   - A ProjectionManager for building read models
type Engine struct {
	store       EventStore
	streams     *StreamManager
	projections *ProjectionManager
	aggregates  *AggregateRegistry

	mu      sync.RWMutex
	running bool
}

// NewEngine initializes everything
func NewEngine(store EventStore) *Engine {
	eng := &Engine{
		store:       store,
		streams:     NewStreamManager(),
		projections: NewProjectionManager(),
		aggregates:  NewAggregateRegistry(),
	}
	// Register the default stream
	eng.streams.RegisterStream(NewStream(defaultStreamOptions))
	return eng
}

// Stream returns the named stream or nil if not found
func (eng *Engine) Stream(name string) *Stream {
	s, _ := eng.streams.Get(name)
	return s
}

// Store returns the underlying event store (if you need direct access)
func (eng *Engine) Store() EventStore { return eng.store }

// Streams returns the underlying stream manager
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

// nextGlobalVersion returns the next global version
func (eng *Engine) nextGlobalVersion(ctx context.Context) (int64, error) {
	return eng.store.IncrementGlobalVersion(ctx)
}

func (eng *Engine) RegisterAggregate(agg Aggregate, events []any, opts ...Option) error {
	eng.aggregates.Register(agg, events)

	options := handleOptions(opts...)

	options.Stream.Name = resolveName(agg)

	stream, err := eng.streams.GetOrCreateStream(*options.Stream)
	if err != nil {
		return err
	}

	if eng.IsRunning() {
		stream.Start()
	}

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

// RegisterProjection registers a projection and attaches it to per-aggregate streams
// declared by the projection. If no aggregates are declared, it attaches to the default stream.
func (eng *Engine) RegisterProjection(proj Projection) error {
	eng.projections.Register(proj)

	// Determine aggregates and events declared by the projection
	aggs, _ := projectionSteamConfig(proj)

	// If projection declares aggregates, attach to each aggregate-named stream
	if len(aggs) > 0 {
		isRunning := eng.IsRunning()
		for _, a := range aggs {
			name := resolveName(a)
			stream, ok := eng.streams.Get(name)
			if !ok {
				return fmt.Errorf("stream %s not found", name)
			}

			stream.Project(proj)
			if isRunning {
				stream.Start()
			}

		}
		return nil
	}

	// Fallback: no aggregates declared → attach to default stream
	stream, ok := eng.streams.Get(DefaultStreamName)
	if !ok {
		return fmt.Errorf("default stream not found")
	}
	stream.Project(proj)
	return nil
}

// CommitAggregateChanges applies a series of new events from an aggregate:
//  1. Optionally increments a global version (if you track that in memory or in store).
//  2. Saves the events to the event store.
//  3. Publishes them to the stream manageeng.
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

	fmt.Printf("Execute called with engine: %+v", eng) // Add debug logging
	if eng == nil {
		return fmt.Errorf("engine is nil")
	}

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

// Subscribe with stream configuration
func (eng *Engine) Subscribe(eventType string, handler StreamHandler, opts ...Option) error {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	streamName := DefaultStreamName
	if options.Stream != nil && options.Stream.Name != "" {
		streamName = options.Stream.Name
	}

	return eng.SubscribeToStream(streamName, eventType, handler)
}

func (eng *Engine) SubscribeToStream(streamName, eventType string, handler StreamHandler) error {
	stream, ok := eng.streams.Get(streamName)
	if !ok {
		return fmt.Errorf("stream %s not found", streamName)
	}

	stream.Subscribe(eventType, handler)
	return nil
}

func (eng *Engine) SubscribeAggregate(streamName string, aggregateType string, handler StreamHandler, opts ...Option) error {
	cfg := &Options{}
	for _, opt := range opts {
		opt(cfg)
	}

	stream, err := eng.streams.GetOrCreateStream(*cfg.Stream)
	if err != nil {
		return err
	}

	stream.Aggregate(aggregateType, handler)
	return nil
}

// Send dispatches events to the appropriate stream
func (eng *Engine) Send(ctx context.Context, events ...Event) {
	eng.streams.Send(ctx, events...)
}

// Start starts all streams and begins processing events
func (eng *Engine) Start() {
	golly.Logger().Tracef("Starting engine")

	eng.mu.Lock()
	eng.running = true
	eng.mu.Unlock()

	eng.streams.Start()

}

// Stop gracefully shuts down all streams
func (eng *Engine) Stop() {
	golly.Logger().Tracef("Stopping engine")

	eng.mu.Lock()
	eng.running = false
	eng.mu.Unlock()

	eng.streams.Stop()
}
