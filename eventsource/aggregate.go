package eventsource

import (
	"cmp"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
)

var (
	legacy = true

	eventRType = reflect.TypeOf(Event{})
)

type Applier interface {
	// Repo(golly.Context) Repository
	Apply(Event)
}

type NewRecordChecker interface {
	IsNewRecord() bool
}

type Aggregate interface {
	// Record events to applied later with metadata
	Record(...any)

	// Record events to be applied later with metadata
	RecordWithMetadata(any, map[string]any)

	// Process the events into the aggregation
	ProcessChanges(context.Context, Aggregate) error

	// Replay events
	Replay(Aggregate, []Event) error
	ReplayOne(Aggregate, Event) error

	// Get the ID of the aggregate it is a string so it supports
	// both UUID and INT representations
	GetID() string

	AppendChanges(...Event)
	SetChanges(Events)
	ClearChanges()

	Changes() Events

	Version() int64
	SetVersion(int64)

	CreateSnapshot(ctx context.Context, agg Aggregate) Event
}

type AggregateBase struct {
	AggregateVersion int64 `json:"version" gorm:"column:version"`
	changes          []Event
}

func (ab *AggregateBase) AppendChanges(changes ...Event) { ab.changes = append(ab.changes, changes...) }
func (ab *AggregateBase) SetChanges(changes Events)      { ab.changes = changes }
func (ab *AggregateBase) ClearChanges()                  { ab.changes = Events{} }

func (ab *AggregateBase) Changes() Events { return ab.changes }

func (ab *AggregateBase) SetVersion(version int64) { ab.AggregateVersion = version }
func (ab *AggregateBase) Version() int64           { return ab.AggregateVersion }

func (ab *AggregateBase) ReplayOne(agg Aggregate, event Event) error {
	if err := apply(agg, event); err != nil {
		return err
	}

	// Replayed events are already persisted. Stores may hand back a stale
	// "applied" state (InMemoryStore copies the struct), which would make
	// Uncommitted() re-save them on the next commit.
	event.SetState(EventStateCompleted)

	ab.changes = append(ab.changes, event)
	return nil
}

// Replay applies events in the correct order to rebuild aggregate state.
func (a *AggregateBase) Replay(agg Aggregate, events []Event) error {
	if len(events) == 0 {
		return nil
	}

	// Sort events by version or created_at timestamp
	slices.SortFunc(events, func(a, b Event) int { return cmp.Compare(a.Version, b.Version) })

	for i := range events {
		if err := apply(agg, events[i]); err != nil {
			return err
		}
		// Already persisted; don't let a stale "applied" state make
		// Uncommitted() re-save it.
		events[i].SetState(EventStateCompleted)
	}

	agg.SetChanges(events)
	return nil
}

// Record generates and tracks events for the aggregate, incrementing the version for each event.
// Events are stored as uncommitted changes, ready for processing by the handler.
func (ab *AggregateBase) Record(data ...any) {
	ab.growChanges(len(data))

	for _, d := range data {
		ab.RecordWithMetadata(d, nil)
	}
}

// Record generates and tracks events for the aggregate, incrementing the version for each event.
// Events are stored as uncommitted changes, ready for processing by the handler.
// Allows for additional metadata to be added to the event
func (ab *AggregateBase) RecordWithMetadata(data any, metadata map[string]any) {
	var version int64

	changes := ab.Changes()

	if l := len(changes); l > 0 {
		version = changes[l-1].Version + 1
	} else {
		version = ab.Version() + 1
	}

	event := NewEvent(data, EventStateReady)
	event.Version = version

	ab.changes = append(ab.changes, event)
}

// growChanges reserves room for n more changes so a batch appends without regrowth.
func (ab *AggregateBase) growChanges(n int) {
	if n <= 0 || cap(ab.changes)-len(ab.changes) >= n {
		return
	}

	grown := make([]Event, len(ab.changes), len(ab.changes)+n)
	copy(grown, ab.changes)
	ab.changes = grown
}

// ProcessChanges applies all uncommitted changes to the aggregate.
// Each change is processed if it does not have the READY state set.
// Changes are updated to reflect their applied state and reattached to the aggregate.
func (ab *AggregateBase) ProcessChanges(ctx context.Context, ag Aggregate) error {
	changes := ag.Changes()

	if len(changes) == 0 {
		return nil
	}

	version := ag.Version()
	ent := handlerCache.entry(ag)
	aggType := ObjectName(ag)
	topic := ent.topicFor(ag)

	for pos := range changes {
		change := &changes[pos]
		if !change.InState(EventStateReady) && !change.InState(EventStateRetry) && !change.InState(EventStateFailed) {
			continue
		}

		if err := apply(ag, *change); err != nil {
			return err
		}

		// For now put this here till i can find a better way todo this
		// perhaps we move Identity to be top level in Golly
		applyUserInfo(ctx, change)

		change.AggregateID = ag.GetID()
		change.AggregateType = aggType
		change.Topic = topic

		change.SetState(EventStateApplied)
	}

	ag.SetChanges(changes)

	if ShouldSnapshot(int(version), int(ag.Version())) {
		ag.AppendChanges(ag.CreateSnapshot(ctx, ag))
	}

	return nil
}

// CreateSnapshot creates a new snapshot event for the aggregate.
func (ab *AggregateBase) CreateSnapshot(ctx context.Context, ag Aggregate) Event {
	evt := NewSnapshot(ag)

	applyUserInfo(ctx, &evt)

	return evt
}

func ApplySnapshot(ag Aggregate, event Event) error {
	if event.ID == uuid.Nil {
		return nil
	}

	switch e := event.Data.(type) {
	case *AggregateSnapshotted:
		if err := unmarshal(ag, e.State); err != nil {
			return err
		}

	case AggregateSnapshotted:
		if err := unmarshal(ag, e.State); err != nil {
			return err
		}
	}

	// The snapshot blob may include a serialized `changes` slice from when it was
	// captured. Those events are already committed — clear them so they don't get
	// re-numbered and re-submitted as "uncommitted" on the next save attempt.
	ag.ClearChanges()

	ag.SetVersion(event.Version)

	return nil
}

// apply dynamically routes an event to the appropriate handler method on the aggregate.
//
// This function uses reflection to identify and call a method named "Apply<EventName>",
// where <EventName> is derived from the event's data type. If the appropriate method
// does not exist, the function will fail silently in production but log the absence of
// the handler in development or test environments.
//
// Usage:
// If an event named "RatingUpdated" is passed, the function will attempt to call
// "RatingUpdatedHandler" on the provided aggregate.
//
// To bypass this dynamic behavior, define the "Apply" method explicitly
// on the aggregate to ensure it overrides the reflection-based handler lookup.
//
// Example:
//
//	func (o *OrderAggregate) RatingUpdatedHandler(event Event) {
//	    // Custom application logic here
//	}
//
//	func (o *OrderAggregate) Apply(event Event) {
//		switch event.Data.(type) {
//		case RatingUpdated:
//			o.RatingUpdatedHandler(event)
//		}
//	}
func apply(ag Aggregate, event Event) error {
	if event.Data == nil {
		return fmt.Errorf("event data is nil")
	}
	defer ag.SetVersion(event.Version)

	// custom Apply() takes precedence
	if app, ok := ag.(Applier); ok {
		app.Apply(event)
		return nil
	}

	// derive "<EventName>Handler"
	name := event.Type
	if name == "" {
		name = golly.InfNameNoPackage(event.Data)
	} else {
		// Trim package/path prefixes: e.g. "eventsource.testEvent" or "pkg/path.TestEvent"
		if i := strings.LastIndex(name, "."); i >= 0 {
			name = name[i+1:]
		}
		if j := strings.LastIndex(name, "/"); j >= 0 {
			name = name[j+1:]
		}
	}

	// fetch or build the per-type table once; lookup builds no strings
	ent := handlerCache.entry(ag)
	idx, ok := ent.byEvent[name]
	if !ok {
		// optional noisy logging in dev/test; quiet no-op in prod
		// logMissingHandler(ag, methodName)
		return nil
	}

	v := reflect.ValueOf(ag)
	if v.Kind() != reflect.Ptr {
		// table indexes the POINTER type; value receivers keep the legacy bound-method call
		var args [1]reflect.Value
		args[0] = reflect.ValueOf(event)
		v.Method(idx).Call(args[:])
		return nil
	}

	box := eventBoxPool.Get().(*Event)
	*box = event
	args := [2]reflect.Value{v, reflect.ValueOf(box).Elem()}
	ent.funcs[idx].Call(args[:])
	*box = Event{}
	eventBoxPool.Put(box)

	return nil
}
