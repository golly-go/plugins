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

	mu            sync.RWMutex
	globalVersion int64
	running       bool

	config EngineConfig
}

type EngineConfig struct {
	SnapshotFrequency int
	BatchSize         int
	// Other config options...
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

// nextGlobalVersion returns the next global version
func (eng *Engine) nextGlobalVersion() (int64, error) {
	return eng.store.IncrementGlobalVersion(context.Background())
}

func (eng *Engine) RegisterAggregate(agg Aggregate, events []any) {
	eng.aggregates.Register(agg, events)
}

// RebuildProjection rebuilds a single projection
func (eng *Engine) RebuildProjection(ctx *golly.Context, projection any) error {
	return eng.projections.Rebuild(ctx, eng, resolveInterfaceName(projection))
}

// RunProjectionToEnd runs the projection to the end of the event stream
func (eng *Engine) RunProjectionToEnd(ctx *golly.Context, projection any) error {
	return eng.projections.RunToEnd(ctx, eng, resolveInterfaceName(projection))
}

// RunProjectionOnce runs the projection once
func (eng *Engine) RunProjectionOnce(ctx *golly.Context, projection any) error {
	return eng.projections.RunOnce(ctx, eng, resolveInterfaceName(projection))
}

// RegisterProjection registers a projection with the engine
func (eng *Engine) RegisterProjection(proj Projection, opts ...Option) error {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.Stream == nil {
		options.Stream = &defaultStreamOptions
	}

	stream, err := eng.streams.GetOrCreateStream(*options.Stream)
	if err != nil {
		return fmt.Errorf("failed to register stream: %w", err)
	}

	eng.projections.Register(proj)
	stream.Project(proj)

	if eng.running {
		stream.Start()
	}

	eng.mu.RLock()

	eng.mu.RUnlock()

	return nil
}

// CommitAggregateChanges applies a series of new events from an aggregate:
//  1. Optionally increments a global version (if you track that in memory or in store).
//  2. Saves the events to the event store.
//  3. Publishes them to the stream manageeng.
//  4. Marks the aggregate's changes as complete.
func (eng *Engine) CommitAggregateChanges(ctx *golly.Context, agg Aggregate) error {
	changes := agg.Changes().Uncommitted()

	for i := range changes {
		version, err := eng.nextGlobalVersion()
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
func (eng *Engine) Execute(ctx *golly.Context, agg Aggregate, cmd Command) (err error) {
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

	agg.ProcessChanges(ctx, agg)

	if agg.GetID() == "" {
		return handleExecutionError(ctx, agg, cmd, ErrorNoAggregateID)
	}

	return eng.CommitAggregateChanges(ctx, agg)
}

func (eng *Engine) Replay(ctx *golly.Context, agg Aggregate) error {
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
				agg.ReplayOne(agg, e)
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

func (eng *Engine) Load(ctx *golly.Context, agg Aggregate) error {
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
func handleExecutionError(ctx *golly.Context, agg Aggregate, cmd Command, err error) error {
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
func (eng *Engine) Send(ctx *golly.Context, events ...Event) {
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
