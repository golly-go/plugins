package eventsource

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
)

var (
	legacy = true
)

type Applier interface {
	// Repo(golly.Context) Repository
	Apply(Event)
}

type NewRecordChecker interface {
	IsNewRecord() bool
}

type Aggregate interface {
	EventStore() EventStore

	// Record events to applied later with metadata
	Record(...any)

	// Record events to be applied later with metadata
	RecordWithMetadata(any, Metadata)

	// Process the events into the aggregation
	ProcessChanges(context.Context, Aggregate)

	// Replay events
	Replay(Aggregate, []Event)

	// Get the ID of the aggregate it is a string so it supports
	// both UUID and INT representations
	GetID() string

	AppendChanges(...Event)
	SetChanges(Events)

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
func (ab *AggregateBase) Changes() Events                { return ab.changes }

func (ab *AggregateBase) SetVersion(version int64) { ab.AggregateVersion = version }
func (ab *AggregateBase) Version() int64           { return ab.AggregateVersion }

// Replay applies events in the correct order to rebuild aggregate state.
func (a *AggregateBase) Replay(agg Aggregate, events []Event) {
	if len(events) == 0 {
		return
	}

	// Sort events by version or created_at timestamp
	sort.Slice(events, func(i, j int) bool { return events[i].Version < events[j].Version })

	for _, evt := range events {
		apply(agg, evt)
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

		apply(ag, change)

		// For now put this here till i can find a better way todo this
		// perhaps we move Identity to be top level in Golly
		if identiyFunc != nil {
			change.Identity = identiyFunc(ctx)
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

// Replay reloads and applies all events for a given aggregate.
func Replay(ctx *golly.Context, agg Aggregate) error {
	id := agg.GetID()

	if id == "" || id == uuid.Nil.String() || id == "0" {
		return nil
	}

	if len(agg.Changes()) > 0 {
		return nil
	}

	estore := agg.EventStore()

	snapshot, _ := estore.LoadSnapshot(ctx, ObjectName(agg), agg.GetID())
	if err := ApplySnapshot(agg, snapshot); err != nil {
		return err
	}

	events, err := estore.
		LoadEvents(ctx, EventFilter{
			AggregateType: ObjectName(agg),
			AggregateID:   agg.GetID(),
			FromVersion:   int(agg.Version()) + 1,
		})

	if err != nil {
		return err
	}

	agg.Replay(agg, events)

	return nil
}

func Load(ctx *golly.Context, agg Aggregate) error {
	if err := Replay(ctx, agg); err != nil {
		return err
	}

	if a, ok := agg.(NewRecordChecker); ok {
		if a.IsNewRecord() {
			return ErrorNoEventsFound
		}
	}

	return nil
}

func capitalizeFirstCharASCII(str string) string {
	if str == "" {
		return ""
	}

	b := []byte(str)
	if b[0] >= 'a' && b[0] <= 'z' {
		b[0] -= 32
	}
	return string(b)
}

func ObjectName(object any) string {
	if legacy {
		return golly.TypeNoPtr(object).String()
	}

	return ObjectPath(object)

}

func ObjectPath(object any) string {
	val := golly.TypeNoPtr(object)

	pieces := strings.Split(val.String(), ".")

	return fmt.Sprintf("%s/%s", val.PkgPath(), pieces[len(pieces)-1])
}

func ApplySnapshot(ag Aggregate, event Event) error {
	if event.ID == uuid.Nil {
		return nil
	}

	switch e := event.Data.(type) {
	case AggregateSnapshottedEvent:
		if err := json.Unmarshal(e.State, ag); err != nil {
			return err
		}

		ag.SetVersion(event.Version)
	}

	return nil
}

// Apply dynamically routes an event to the appropriate handler method on the aggregate.
//
// This function uses reflection to identify and call a method named "Apply<EventName>",
// where <EventName> is derived from the event's data type. If the appropriate method
// does not exist, the function will fail silently in production but log the absence of
// the handler in development or test environments.
//
// Usage:
// If an event named "RatingUpdated" is passed, the function will attempt to call
// "ApplyRatingUpdated" on the provided aggregate.
//
// To bypass this dynamic behavior, define the "Apply" method explicitly
// on the aggregate to ensure it overrides the reflection-based handler lookup.
//
// Example:
//
//	func (o *OrderAggregate) ApplyRatingUpdated(event Event) {
//	    // Custom application logic here
//	}
func apply(ag Aggregate, event Event) {
	if event.Data == nil {
		return
	}

	if app, ok := ag.(Applier); ok {
		fmt.Println("Applier")

		app.Apply(event)
		return
	}

	name := golly.InfNameNoPackage(event.Data)

	methodName := fmt.Sprintf("%sHandler", capitalizeFirstCharASCII(name))
	method := reflect.ValueOf(ag).MethodByName(methodName)

	if !method.IsValid() {
		if golly.Env().IsDevelopmentOrTest() {
			fmt.Printf("No handler for %s#%s %s", golly.TypeNoPtr(ag).String(), methodName, method)
		}
		return
	}

	method.Call([]reflect.Value{reflect.ValueOf(event)})

	ag.SetVersion(event.Version)
}
