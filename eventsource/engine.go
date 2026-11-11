package eventsource

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"time"

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

	running    atomic.Bool
	maxRetries int
}

// NewEngine allows configuring the engine via Option by building a config first.
func NewEngine(opts ...Option) *Engine {
	cfg := handleOptions(opts...)

	streams := NewStreamManager()

	// Add external producers (like Kafka)
	for i := range cfg.Streams {
		streams.Add(cfg.Streams[i])
	}

	workers := cfg.ProjectionWorkers
	if workers < 1 {
		workers = DefaultProjectionWorkers()
	}
	bufferSize := cfg.ProjectionBufferSize
	if bufferSize < 1 {
		bufferSize = DefaultProjectionBufferSize()
	}

	eng := &Engine{
		store:       cfg.Store,
		projections: NewProjectionManagerWithConfig(workers, bufferSize),
		aggregates:  NewAggregateRegistry(),
		streams:     streams,
		maxRetries:  cfg.MaxRetries,
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
	return eng.running.Load()
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
// between Replay and Save), Execute rebuilds the aggregate from the store, re-runs
// Validate against that state, re-applies the already-computed pending events on top
// with new versions, and retries the save. Perform runs exactly once, so commands with
// external side effects (e.g. Stripe API calls) are safe; if the rebuilt state no longer
// passes Validate, that error is returned and nothing is saved (Rollback runs, so the
// command can compensate).
func (eng *Engine) Execute(ctx context.Context, agg Aggregate, cmd Command) (err error) {
	if eng == nil {
		return fmt.Errorf("engine is nil")
	}

	if traceEnabled() {
		trace("executing command %s aggregate=%s aggregateID=%s", golly.TypeNoPtr(cmd).Name(), golly.TypeNoPtr(agg).Name(), agg.GetID())
	}

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

	// === Phase 2: commit with exponential backoff on version conflict ===
	// Backoff: 4ms → 8ms → 16ms → 32ms → 60ms (capped), ±25% jitter.
	// With default 10 retries the worst-case total wait is ~390ms.
	const (
		retryBase = 4 * time.Millisecond
		retryMax  = 60 * time.Millisecond
	)
	for attempt := 0; attempt < eng.maxRetries; attempt++ {
		err = eng.CommitAggregateChanges(ctx, agg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrVersionConflict) {
			return err
		}
		if attempt == eng.maxRetries-1 {
			break
		}

		// Exponential backoff with ±25% jitter before rebuild + retry.
		delay := retryBase << uint(attempt)
		if delay > retryMax {
			delay = retryMax
		}
		delay += time.Duration(rand.Int63n(int64(delay) / 4))
		time.Sleep(delay)

		if traceEnabled() {
			trace("version conflict on attempt %d for %s %s, rebuilding and retrying",
				attempt+1, golly.TypeNoPtr(agg).Name(), agg.GetID())
		}

		// A concurrent writer raced in between our Replay and Save.
		if e := eng.rebase(ctx, agg, cmd, err, false); e != nil {
			return e
		}
	}

	// === Phase 3: snapshot-corruption recovery (last resort) ===
	// All normal retries exhausted with a persistent version conflict.
	// If the aggregate has a snapshot, the snapshot index may be corrupt
	// (e.g. concurrent flood produced out-of-order or duplicate snapshots).
	// Only in that case do we pay the cost of a full event scan — for
	// snapshot-less aggregates, Replay already reads everything from scratch,
	// so a full rebuild changes nothing and we return the conflict directly.
	snap, snapErr := eng.store.LoadSnapshot(ctx, ObjectName(agg), agg.GetID())
	if snapErr != nil {
		return errors.Join(err, snapErr)
	}

	if snap == nil {
		return err
	}

	trace("snapshot corruption suspected for %s %s — rebuilding from raw events",
		golly.TypeNoPtr(agg).Name(), agg.GetID())

	if e := eng.rebase(ctx, agg, cmd, err, true); e != nil {
		return e
	}

	return eng.CommitAggregateChanges(ctx, agg)
}

// rebase recovers from a version conflict. It rebuilds agg from the store into a
// zero value of its type (state already holds the pending events, so replaying on
// top of it would skip the interleaved ones), re-runs cmd's Validate against that
// state, then re-applies the pending events with versions after the stored head.
// Perform is not re-run. A pending snapshot is dropped and retaken if still due.
//
// fromZero ignores stored snapshots (the corruption-recovery path) and writes a
// clean one of the rebuilt state.
//
// The caller's ID field is carried over to the zero value. An aggregate whose ID
// lives elsewhere and isn't restored by replay cannot be rebuilt this way, so
// conflict is returned unchanged.
func (eng *Engine) rebase(ctx context.Context, agg Aggregate, cmd Command, conflict error, fromZero bool) error {
	id := agg.GetID()
	aggType := ObjectName(agg)

	v := reflect.ValueOf(agg)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return conflict
	}
	fv := reflect.New(v.Elem().Type())
	fresh, ok := fv.Interface().(Aggregate)
	if !ok {
		return conflict
	}
	// Commands often set the ID in Perform rather than the created handler, so carry
	// the ID field over (golly's convention); GetID is checked after replay.
	if f := v.Elem().FieldByName("ID"); f.IsValid() && f.CanSet() {
		fv.Elem().FieldByName("ID").Set(f)
	}

	var err error
	if fromZero {
		err = eng.replayFromZero(ctx, fresh, aggType, id)
	} else {
		err = eng.replayID(ctx, fresh, aggType, id)
	}
	if err != nil {
		return err
	}
	if fresh.GetID() != id {
		return conflict
	}

	pending := agg.Changes().Uncommitted()
	v.Elem().Set(reflect.ValueOf(fresh).Elem())

	if fromZero {
		snapshot := agg.CreateSnapshot(ctx, agg)
		if e := eng.store.SaveSnapshot(ctx, snapshot); e != nil {
			trace("failed to write recovery snapshot for %s %s: %v",
				golly.TypeNoPtr(agg).Name(), id, e)
		}
	}

	if val, ok := cmd.(CommandValidator); ok {
		if err := val.Validate(ctx, agg); err != nil {
			agg.SetChanges(pending)
			return handleExecutionError(ctx, agg, cmd, err)
		}
	}

	head := agg.Version()
	base := head
	n := 0
	for i := range pending {
		if pending[i].Kind == EventKindSnapshot {
			continue
		}
		base++
		pending[i].Version = base
		if err := apply(agg, pending[i]); err != nil {
			return err
		}
		pending[n] = pending[i]
		n++
	}

	agg.SetChanges(pending[:n])
	if ShouldSnapshot(int(head), int(base)) {
		agg.AppendChanges(agg.CreateSnapshot(ctx, agg))
	}

	return nil
}

// replayFromZero rebuilds aggregate state by scanning all events of aggType/id from
// the beginning, skipping snapshots entirely. This is the recovery path for
// corrupt or out-of-order snapshots produced by concurrent write floods.
// It is intentionally expensive — call only as a last resort.
func (eng *Engine) replayFromZero(ctx context.Context, agg Aggregate, aggType, id string) error {
	if id == "" {
		return nil
	}

	agg.ClearChanges()

	return eng.store.LoadEventsInBatches(ctx, 500,
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
			AggregateType:  aggType,
			AggregateID:    id,
			ExcludeKinds:   excludeSnapshots,
			OrderByVersion: true,
		})
}

// excludeSnapshots is shared read-only by every replay filter; stores must not mutate it.
var excludeSnapshots = []EventKind{EventKindSnapshot}

// growReplayChanges reserves room for n more replayed events in one allocation.
func growReplayChanges(agg Aggregate, n int) {
	ch := agg.Changes()
	if cap(ch)-len(ch) >= n {
		return
	}

	grown := make(Events, len(ch), len(ch)+n)
	copy(grown, ch)
	agg.SetChanges(grown)
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

	return eng.replayID(ctx, agg, ObjectName(agg), id)
}

// replayID applies aggType/id's latest snapshot and the events after it to agg. The
// type and id are explicit so a zero-value aggregate can be rebuilt.
func (eng *Engine) replayID(ctx context.Context, agg Aggregate, aggType, id string) error {
	snap, err := eng.store.LoadSnapshot(ctx, aggType, id)
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
			growReplayChanges(agg, len(pEvents))

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
			AggregateType: aggType,
			AggregateID:   id,
			FromVersion:   int(agg.Version()) + 1,
			// Exclude snapshot rows — they were already applied above.
			// During write floods, concurrent snapshots get interleaved into the
			// event stream. Re-applying them via ReplayOne would reset Version
			// backwards, causing permanent OCC conflicts on every save.
			ExcludeKinds: excludeSnapshots,
			// Order by aggregate version, not global_version. The flood wrote events
			// with the same aggregate version but different global_versions, so
			// global_version ordering can walk aggregate version backwards mid-replay.
			OrderByVersion: true,
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
	if len(events) == 0 {
		return
	}

	// one detached context shared by every dispatch/publish in this call;
	// skipped when nothing is running to consume it
	var detached context.Context
	if eng.projections.running.Load() || eng.streams.has() {
		detached = golly.ToGollyContext(ctx).Detach()
	}

	// Dispatch to projections first (internal)
	for i := range events {
		eng.projections.dispatchDetached(detached, &events[i])
	}

	// Then publish to external streams (Kafka, etc)
	for i := range events {
		topic := eventTopic(&events[i])
		if topic == "" {
			continue
		}
		eng.streams.publishDetached(detached, topic, &events[i])
	}
}

// Start marks the engine running and starts projections and streams
func (eng *Engine) Start() {
	golly.DefaultLogger().Tracef("Starting engine")

	eng.running.Store(true)

	eng.projections.Start() // Start projection async processing
}

// Stop stops the engine, gracefully draining all projection events
func (eng *Engine) Stop() {
	golly.DefaultLogger().Tracef("Stopping engine")

	eng.running.Store(false)

	eng.projections.Stop() // Drain all projection events first

	SetUserInfoFunc(nil) // for now
}

func HasValidID(agg Aggregate) bool {
	return agg.GetID() != "" && agg.GetID() != uuid.Nil.String() && agg.GetID() != "0"
}
