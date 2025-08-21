package eventsource

import (
	"github.com/segmentio/encoding/json"

	"github.com/golly-go/golly"
)

const (
	SnapShotIncrement = 100
)

// AggregateSnapshottedEvent represents a snapshot of the aggregate's state.
type AggregateSnapshotted struct {
	State []byte
}

// NewSnapshot creates a snapshot event by wrapping the aggregate as the Data field.
func NewSnapshot(aggregate Aggregate) Event {
	state, err := json.Marshal(aggregate)
	if err != nil {
		golly.Logger().Warnf("Failed to serialize aggregate: %v", err)
		return Event{}
	}

	snapEvent := AggregateSnapshotted{State: state}
	event := NewEvent(snapEvent, EventStateApplied, nil)

	event.AggregateID = aggregate.GetID()
	event.AggregateType = ObjectName(aggregate)
	event.Topic = NameToTopicUnicode(aggregate)

	event.Kind = EventKindSnapshot
	event.Version = aggregate.Version()

	return event
}

// ShouldSnapshot determines if a snapshot should be created based on version increments.
func ShouldSnapshot(oldVersion, newVersion int) bool {
	return (oldVersion / SnapShotIncrement) != (newVersion / SnapShotIncrement)
}
