package eventsource

import (
	"context"
	"fmt"
	"testing"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type TestAggregate struct {
	AggregateBase

	ID               string
	Name             string
	LastAppliedEvent *Event

	es EventStore
}

func (ta *TestAggregate) TestEventHandler(evt Event) {

	fmt.Printf("evt: %#v\n", evt)

	switch event := evt.Data.(type) {
	case testEvent:
		ta.Name = event.name
	}
	ta.LastAppliedEvent = &evt
}

func (ta *TestAggregate) GetID() string     { return ta.ID }
func (ta *TestAggregate) SetID(string)      {}
func (ta *TestAggregate) IsNewRecord() bool { return false }

func TestReplay(t *testing.T) {
	tests := []struct {
		name        string
		events      []Event
		expectOrder []int64
	}{
		{
			name: "Events in order",
			events: []Event{
				{Version: 1},
				{Version: 2},
				{Version: 3},
			},
			expectOrder: []int64{1, 2, 3},
		},
		{
			name: "Events out of order",
			events: []Event{
				{Version: 3},
				{Version: 1},
				{Version: 2},
			},
			expectOrder: []int64{1, 2, 3},
		},
		{
			name: "Single event",
			events: []Event{
				{Version: 1},
			},
			expectOrder: []int64{1},
		},

		{
			name:   "No events",
			events: []Event{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregate := &TestAggregate{}
			aggregate.Replay(aggregate, tt.events)

			changes := aggregate.Changes()

			var versions []int64
			for _, evt := range changes {
				versions = append(versions, evt.Version)
			}

			assert.Equal(t, tt.expectOrder, versions)
		})
	}
}

func TestAggregateBase_Apply(t *testing.T) {
	type testEvent struct{}
	type unhandledEvent struct{}

	tests := []struct {
		name         string
		event        Event
		handlerExist bool
		shouldApply  bool
	}{
		{
			name:         "Handler Exists",
			event:        Event{Type: "TestEvent", Data: testEvent{}},
			handlerExist: true,
			shouldApply:  true,
		},
		{
			name:         "Handler Missing",
			event:        Event{Type: "UnknownEvent", Data: unhandledEvent{}},
			handlerExist: false,
			shouldApply:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aggregate := &TestAggregate{}

			apply(aggregate, tt.event)

			if tt.handlerExist {
				assert.Equal(t, tt.event.Type, aggregate.LastAppliedEvent.Type)
			} else {
				assert.Nil(t, aggregate.LastAppliedEvent)
			}
		})
	}
}

// Test for Record function
func TestRecord(t *testing.T) {
	mockAggregate := &TestAggregate{
		AggregateBase: AggregateBase{AggregateVersion: 3},
	}

	mockData := []any{
		testEvent{name: "FirstEvent"},
		testEvent{name: "SecondEvent"},
	}

	mockAggregate.Record(mockData...)
	changes := mockAggregate.Changes()

	assert.Len(t, changes, 2, "There should be two recorded events")
	assert.Equal(t, int64(4), changes[0].Version, "First event version should increment")
	assert.Equal(t, int64(5), changes[1].Version, "Second event version should increment")
	assert.Equal(t, "eventsource.testEvent", changes[0].Type, "First event type should match")
	assert.Equal(t, "eventsource.testEvent", changes[1].Type, "Second event type should match")
}

// Test for ProcessChanges function
func TestProcessChanges(t *testing.T) {
	mockAggregate := &TestAggregate{
		ID: "aggregate-789",
	}

	mockAggregate.SetChanges([]Event{
		{ID: uuid.New(), State: EventStateFailed},
		{ID: uuid.New(), State: EventStateReady},
		{ID: uuid.New(), State: EventStateRetry},
	})

	mockAggregate.ProcessChanges(golly.NewContext(context.Background()), mockAggregate)
	changes := mockAggregate.Changes()

	assert.Equal(t, EventStateApplied, changes[0].GetState(), "First event should be marked as APPLIED")
	assert.Equal(t, EventStateApplied, changes[1].GetState(), "Second event should remain APPLIED")
	assert.Equal(t, EventStateApplied, changes[2].GetState(), "Third event should be marked as APPLIED")

	assert.Equal(t, mockAggregate.GetID(), changes[0].AggregateID, "Aggregate ID should not be updated")
}
