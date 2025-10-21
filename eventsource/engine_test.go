package eventsource

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func createMockEvents(types ...string) []PersistedEvent {
	events := make([]PersistedEvent, 0, len(types))
	for _, typ := range types {
		pe := &MockPersistedEvent{}
		evt := Event{ID: uuid.New(), Type: typ}
		// We assume pe.On("Hydrate", ...) is called with a pointer to the Engine
		pe.On("Hydrate", mock.Anything).Return(evt, nil)
		events = append(events, pe)
	}
	return events
}

func createMockEventsWithHydrateErr(err error) []PersistedEvent {
	pe := &MockPersistedEvent{}
	pe.On("Hydrate", mock.Anything).Return(Event{}, err)
	return []PersistedEvent{pe}
}

func TestEngine_LoadEvents(t *testing.T) {

	type scenario struct {
		name          string
		batchSize     int
		events        []PersistedEvent
		err           error    // If not nil, the store returns this error immediately
		errContains   string   // Substring to check in the returned error
		expectedTypes []string // If we expect successful hydration, check these event types
	}

	cases := []scenario{
		{
			name:          "Success - two events hydrated",
			batchSize:     10,
			events:        createMockEvents("TestEvent1", "TestEvent2"),
			expectedTypes: []string{"TestEvent1", "TestEvent2"},
		},
		{
			name:        "Store error",
			batchSize:   5,
			err:         errors.New("store error"),
			errContains: "store error",
		},
		{
			name:        "Hydrate error on single event",
			batchSize:   1,
			events:      createMockEventsWithHydrateErr(errors.New("hydrate fail")),
			errContains: "hydrate fail",
		},
		{
			name:        "Handle function fails",
			batchSize:   2,
			events:      createMockEvents("TestEvent"),
			err:         errors.New("handle function failed"),
			errContains: "handle function failed",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := context.Background()
			mockStore := &MockStore{}
			eng := NewEngine(WithStore(mockStore))

			// Store successfully returns events in a single batch
			mockStore.On("LoadEventsInBatches", ctx, c.batchSize, mock.Anything, []EventFilter(nil)).
				Return(c.err).
				Run(func(args mock.Arguments) {
					handler := args.Get(2).(func([]PersistedEvent) error)
					// Pass the events to the handler
					_ = handler(c.events)
				})

			// 2) Handle function: collects events or fails if c.handleErr is set
			var received []Event
			handle := func(evts []Event) error {
				if c.err != nil {
					return c.err
				}
				received = append(received, evts...)
				return nil
			}

			// 3) Call the method under test
			err := eng.LoadEvents(ctx, c.batchSize, handle)

			// 4) Assertions
			// If errContains is blank, we expect no error
			if c.err == nil {
				assert.NoError(t, err)
			} else {
				assert.Error(t, err)
				assert.ErrorContains(t, err, c.errContains)
			}

			// If we expect certain event types, verify them
			if len(c.expectedTypes) > 0 {
				require.Len(t, received, len(c.expectedTypes))
				for i, expType := range c.expectedTypes {
					assert.Equal(t, expType, received[i].Type)
				}
			}

			mockStore.AssertExpectations(t)
		})
	}
}

func TestEngine_RegisterProjection(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*Engine) Projection
		wantErr bool
	}{
		{
			name: "Register no-op projection",
			setup: func(e *Engine) Projection {
				return &noOpProjection{}
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := NewEngine(WithStore(NewInMemoryStore()))
			proj := tt.setup(engine)

			err := engine.RegisterProjection(proj)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestEngine_ExecuteCommand(t *testing.T) {
	engine := NewEngine(WithStore(NewInMemoryStore()))
	ctx := golly.NewContext(context.Background())

	agg := TestAggregate{ID: "123"}
	engine.RegisterAggregate(&TestAggregate{}, []any{testEvent{}})

	cmd := TestCommand{name: "test"}
	err := engine.Execute(ctx, &agg, &cmd)

	assert.NoError(t, err)
	assert.True(t, cmd.executed)
	assert.Equal(t, "test", agg.Name)
}

// SendTestProjection for testing Send functionality
type SendTestProjection struct {
	ProjectionBase
	seen *[]string
}

func (tp *SendTestProjection) HandleEvent(ctx context.Context, evt Event) error {
	*tp.seen = append(*tp.seen, evt.Topic+":"+evt.Type)
	return nil
}

func TestEngine_Send_ResolvesTopic(t *testing.T) {
	eng := NewEngine(WithStore(NewInMemoryStore()))
	defer eng.Stop()
	eng.Start()

	var seen []string
	proj := &SendTestProjection{seen: &seen}

	_ = eng.RegisterProjection(proj)

	eng.Send(context.Background(),
		Event{Type: "X", Topic: "X"},
		Event{Data: struct{}{}},
		Event{Type: "Y", Topic: "Y"})

	// Wait for async processing
	time.Sleep(10 * time.Millisecond)

	// Debug: print what we saw
	t.Logf("Seen events: %v", seen)

	foundX, foundY := false, false
	for _, s := range seen {
		if s == "X:X" {
			foundX = true
		}
		if s == "Y:Y" {
			foundY = true
		}
	}
	assert.True(t, foundX)
	assert.True(t, foundY)
}

// ************************************************
// * Benchmarks
// ************************************************

func BenchmarkEngine_LoadEvents(b *testing.B) {
	ctx := context.Background()

	mockStore := &MockStore{}
	eng := NewEngine(WithStore(mockStore))

	// Suppose each batch has 100 events
	peEvents := make([]PersistedEvent, 100)
	// You could define a real or mock for each item. For a quick benchmark, keep it minimal:
	for i := range peEvents {
		mockPE := &MockPersistedEvent{}
		// Return a small event
		mockPE.On("Hydrate", eng).Return(Event{Type: "BenchEvent"}, nil)
		peEvents[i] = mockPE
	}

	// We'll simulate all 100 in one batch for simplicity
	mockStore.On("LoadEventsInBatches", ctx, 100, mock.Anything, []EventFilter(nil)).
		Return(nil).
		Run(func(args mock.Arguments) {
			args.Get(2).(func([]PersistedEvent) error)(peEvents) // single batch
		})

	// Benchmark
	for i := 0; i < b.N; i++ {
		_ = eng.LoadEvents(ctx, 100, func(events []Event) error { return nil })
	}
}
