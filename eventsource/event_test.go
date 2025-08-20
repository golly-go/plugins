package eventsource

import (
	"testing"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type testEvent struct {
	name string
}

type MockPersistedEvent struct {
	mock.Mock
}

func (m *MockPersistedEvent) Hydrate(eng *Engine) (Event, error) {
	args := m.Called(eng)
	if evt, ok := args.Get(0).(Event); ok {
		return evt, args.Error(1)
	}
	return Event{}, args.Error(1)
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

	event := NewEvent(mockData, EventStateReady, nil)

	assert.NotNil(t, event.ID, "Event ID should be generated")
	assert.Equal(t, "struct { Amount float64 }", event.Type, "Event type should match data type")
	assert.True(t, event.InState(EventStateReady), "Event should have READY state")
	assert.False(t, event.InState(EventStateFailed), "Event should not have FAILED state")
	assert.Equal(t, mockData, event.Data, "Event data should match input data")
}

func TestEvent_Hydrate(t *testing.T) {
	type TestEvent struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	engine := NewEngine(WithStore(&InMemoryStore{}))
	engine.aggregates.Register(&TestAggregate{}, []any{TestEvent{}})

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
				Type:          "TestEvent",
				AggregateType: "Unknown",
			},
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.setupEvent.Hydrate(engine, tt.inputData, nil)

			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedData, tt.setupEvent.Data)
			}
		})
	}
}

func setupTestEngine() *Engine {
	// Create a registry
	engine := NewEngine(WithStore(&InMemoryStore{}))

	// Register an aggregate + events
	// (If your real code uses the “codec” approach, adapt accordingly)
	engine.aggregates.Register(&TestAggregate{}, []any{
		testEvent{},
	})

	return engine
}

func BenchmarkHydrateData(b *testing.B) {
	engine := setupTestEngine()

	// We’ll simulate different data forms that hydrateData can handle.
	// For simplicity, just pick one or two—json.RawMessage and []byte, for example.
	rawJSON := json.RawMessage(`{"foo":"bar"}`)

	// Prepare an Event struct
	evt := &Event{
		ID:            uuid.New(),
		Type:          ObjectName(testEvent{}),     // "testEventStruct"
		AggregateType: ObjectName(TestAggregate{}), // "testAggregate"
		Kind:          "event",                     // normal event
	}

	// Reset timer to exclude setup time
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// We re-create the event for a fair test, or just re-hydrate the same event
		// If you want to test memory allocations, you might do new Event each time
		// but this is enough for a micro-benchmark of the unmarshal logic.
		evt.Data = nil // so it re-hydrates each iteration

		if err := evt.Hydrate(engine, rawJSON, nil); err != nil {
			b.Fatalf("hydrateData failed: %v", err)
		}
	}
}

type evtX struct {
	A int `json:"a"`
}

type aggX struct {
	AggregateBase
	id string
}

func (a *aggX) GetID() string { return a.id }

func Test_unmarshal_SupportsRawMessageStringBytes(t *testing.T) {
	t.Run("json.RawMessage", func(t *testing.T) {
		var v evtX
		raw := json.RawMessage(`{"a":5}`)
		assert.NoError(t, unmarshal(&v, raw))
		assert.Equal(t, 5, v.A)
	})

	t.Run("string", func(t *testing.T) {
		var v evtX
		assert.NoError(t, unmarshal(&v, `{"a":7}`))
		assert.Equal(t, 7, v.A)
	})

	t.Run("bytes", func(t *testing.T) {
		var v evtX
		assert.NoError(t, unmarshal(&v, []byte(`{"a":9}`)))
		assert.Equal(t, 9, v.A)
	})

	t.Run("unsupported", func(t *testing.T) {
		var v evtX
		err := unmarshal(&v, 123)
		assert.Error(t, err)
	})
}

func Test_EventDataToMap(t *testing.T) {
	m, err := EventDataToMap(evtX{A: 42})
	assert.NoError(t, err)
	assert.Equal(t, "42", m["a"].(json.Number).String())
}

func Test_Event_Hydrate_WithRegistryCodec(t *testing.T) {
	// Setup engine with registry codec for (aggX, evtX)
	eng := NewEngine(WithStore(NewInMemoryStore()))
	eng.aggregates.Register(&aggX{}, []any{evtX{}})

	e := &Event{Type: ObjectName(evtX{}), AggregateType: ObjectName(&aggX{}), Kind: EventKindEvent}
	// Hydrate with raw JSON
	raw := json.RawMessage(`{"a":11}`)
	assert.NoError(t, e.Hydrate(eng, raw, nil))

	data, ok := e.Data.(evtX)
	assert.True(t, ok)
	assert.Equal(t, 11, data.A)
}
