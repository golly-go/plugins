package eventsource

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type testEvent struct {
	Name string
}

func TestEventBase_SetState(t *testing.T) {
	event := &Event{
		ID:        uuid.New(),
		CreatedAt: time.Now(),
		Type:      "TestEvent",
		State:     "",
	}

	event.SetState(EventStateReady)
	assert.True(t, event.InState(EventStateReady), "Event should have READY state")
}

func TestEventBase_GetState(t *testing.T) {
	event := &Event{
		State: EventStateFailed,
	}

	assert.Equal(t, EventStateFailed, event.GetState(), "Event state should match")
}

func TestNewEvent(t *testing.T) {

	mockData := struct {
		Amount float64
	}{Amount: 100.0}

	aggregate := TestAggregate{ID: "aggregate-123"}

	event := NewEvent(&aggregate, mockData, EventStateReady, nil)

	assert.NotNil(t, event.ID, "Event ID should be generated")
	assert.Equal(t, "struct { Amount float64 }", event.Type, "Event type should match data type")
	assert.Equal(t, "aggregate-123", event.AggregateID, "Aggregate ID should match")
	assert.True(t, event.InState(EventStateReady), "Event should have READY state")
	assert.False(t, event.InState(EventStateFailed), "Event should not have FAILED state")
	assert.Equal(t, mockData, event.Data, "Event data should match input data")
}

func TestEvent_Hydrate(t *testing.T) {
	type TestEvent struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	Aggregates().Register(&TestAggregate{}, []any{
		TestEvent{},
	})

	tests := []struct {
		name         string
		inputData    any
		setupEvent   Event
		shouldError  bool
		expectedData TestEvent
	}{
		{
			name:      "Valid byte data",
			inputData: []byte(`{"name":"John Doe","age":30}`),
			setupEvent: Event{
				ID:            uuid.New(),
				Type:          "eventsource.TestEvent",
				AggregateType: "eventsource.TestAggregate",
			},
			shouldError:  false,
			expectedData: TestEvent{Name: "John Doe", Age: 30},
		},
		{
			name: "Valid map data",
			inputData: map[string]interface{}{
				"name": "Jane Doe",
				"age":  25,
			},
			setupEvent: Event{
				ID:            uuid.New(),
				Type:          "eventsource.TestEvent",
				AggregateType: "eventsource.TestAggregate",
			},
			shouldError:  false,
			expectedData: TestEvent{Name: "Jane Doe", Age: 25},
		},
		{
			name:      "Missing Type",
			inputData: []byte(`{"name":"Invalid","age":40}`),
			setupEvent: Event{
				ID:            uuid.New(),
				Type:          "",
				AggregateType: "eventsource.TestEvent",
			},
			shouldError: true,
		},
		{
			name:      "Unknown Event Type",
			inputData: []byte(`{"name":"Unknown","age":50}`),
			setupEvent: Event{
				ID:            uuid.New(),
				Type:          "UnknownEvent",
				AggregateType: "eventsource.TestAggregate",
			},
			shouldError: true,
		},
		{
			name:      "Unknown Aggregate Type",
			inputData: []byte(`{"name":"Unknown","age":50}`),
			setupEvent: Event{
				ID:            uuid.New(),
				Type:          "eventsource.TestEvent",
				AggregateType: "Unknown",
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setupEvent.Hydrate(tt.inputData)
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedData, tt.setupEvent.Data)
			}
		})
	}
}
