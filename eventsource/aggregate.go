package eventsource

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"

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
	RecordWithMetadata(any, Metadata)

	// Process the events into the aggregation
	ProcessChanges(context.Context, Aggregate)

	// Replay events
	Replay(Aggregate, []Event)
	ReplayOne(Aggregate, Event)

	// Get the ID of the aggregate it is a string so it supports
	// both UUID and INT representations
	GetID() string

	AppendChanges(...Event)
	SetChanges(Events)
	ClearChanges()

	Changes() Events

	Version() int64
	SetVersion(int64)
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

func (ab *AggregateBase) ReplayOne(agg Aggregate, event Event) {
	if err := apply(agg, event); err != nil {
		golly.Logger().Error(err)
	}

	agg.AppendChanges(event)
}

// Replay applies events in the correct order to rebuild aggregate state.
func (a *AggregateBase) Replay(agg Aggregate, events []Event) {
	if len(events) == 0 {
		return
	}

	// Sort events by version or created_at timestamp
	sort.Slice(events, func(i, j int) bool { return events[i].Version < events[j].Version })

	for _, evt := range events {
		if err := apply(agg, evt); err != nil {
			golly.Logger().Error(err)
		}
	}

	agg.SetChanges(events)
}

// Record generates and tracks events for the aggregate, incrementing the version for each event.
// Events are stored as uncommitted changes, ready for processing by the handler.
func (ab *AggregateBase) Record(data ...any) {
	for _, d := range data {
		ab.RecordWithMetadata(d, nil)
	}
}

// Record generates and tracks events for the aggregate, incrementing the version for each event.
// Events are stored as uncommitted changes, ready for processing by the handler.
// Allows for additional metadata to be added to the event
func (ab *AggregateBase) RecordWithMetadata(data any, metadata Metadata) {
	var version int64

	changes := ab.Changes()

	if l := len(changes); l > 0 {
		version = changes[l-1].Version + 1
	} else {
		version = ab.Version() + 1
	}

	event := NewEvent(data, EventStateReady, nil)
	event.Version = version

	ab.AppendChanges(event)
}

// ProcessChanges applies all uncommitted changes to the aggregate.
// Each change is processed if it does not have the READY state set.
// Changes are updated to reflect their applied state and reattached to the aggregate.
func (ab *AggregateBase) ProcessChanges(ctx context.Context, ag Aggregate) {
	changes := ag.Changes()

	if len(changes) == 0 {
		return
	}

	version := ag.Version()

	for pos, change := range changes {
		if !change.InState(EventStateReady) && !change.InState(EventStateRetry) && !change.InState(EventStateFailed) {
			continue
		}

		if err := apply(ag, change); err != nil {
			golly.Logger().Error(err)
		}

		// For now put this here till i can find a better way todo this
		// perhaps we move Identity to be top level in Golly
		if identiyFunc != nil {
			change.Identity = identityFunc(ctx)
		}

		if tententIDFunc != nil {
			change.TenantID = tententIDFunc(ctx)
		}

		change.AggregateID = ag.GetID()
		change.AggregateType = ObjectName(ag)

		change.SetState(EventStateApplied)
		changes[pos] = change
	}

	ag.SetChanges(changes)

	if ShouldSnapshot(int(version), int(ag.Version())) {
		ag.AppendChanges(NewSnapshot(ag))
	}
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
		return errors.New("event data is nil")
	}

	defer ag.SetVersion(event.Version)

	if app, ok := ag.(Applier); ok {
		app.Apply(event)
		return nil
	}

	name := golly.InfNameNoPackage(event.Data)

	methodName := fmt.Sprintf("%sHandler", capitalizeFirstCharASCII(name))

	methodValue := reflect.ValueOf(ag).MethodByName(methodName)
	if !methodValue.IsValid() {
		// Optionally log missing handler in dev/test
		if golly.Env().IsDevelopmentOrTest() {
			golly.Logger().Tracef("No handler for %s#%s (method not found)",
				golly.TypeNoPtr(ag).String(), methodName)
		}
		return nil
	}

	methodType := methodValue.Type()
	if methodType.NumIn() != 1 {
		return fmt.Errorf("expected 1 param, got %d for %s#%s",
			methodType.NumIn(), golly.TypeNoPtr(ag).String(), methodName)
	}

	// i really do not like this but it works for now (Its ugly and slightly magic)
	// though i was really tired of the massiiiiiive switch statements for anything that was super
	// large aggregation
	// if you dont like this (It is slower use the Apply() method)
	paramType := methodType.In(0)

	if paramType == eventRType {
		methodValue.Call([]reflect.Value{reflect.ValueOf(event)})
		return nil
	}

	dataType := reflect.ValueOf(event.Data)
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}

	if paramType == dataType.Type() {
		methodValue.Call([]reflect.Value{dataType})
		return nil
	}

	return fmt.Errorf("unexpected param type got %s for %s#%s",
		paramType, golly.TypeNoPtr(ag).String(), methodName)

}
