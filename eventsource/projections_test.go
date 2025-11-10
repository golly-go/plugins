package eventsource

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProjection implements the Projection interface.
// It embeds ProjectionBase for position tracking and
// tracks how many events it handled, optionally returning an error once.
type TestProjection struct {
	ProjectionBase

	id                string
	handledCount      int64 // Change to int64 to match test expectations
	failOnEventNumber int64
	eventCounter      int64
	aggregates        []string
	events            []string
}

func (tp *TestProjection) AggregateTypes() []string { return tp.aggregates }
func (tp *TestProjection) EventTypes() []string     { return tp.events }
func (tp *TestProjection) Topics() []string         { return tp.events }

func (tp *TestProjection) HandleEvent(ctx context.Context, evt Event) error {
	current := atomic.AddInt64(&tp.eventCounter, 1)
	if tp.failOnEventNumber > 0 && current == tp.failOnEventNumber {
		return fmt.Errorf("simulated error on event #%d", current)
	}

	atomic.AddInt64(&tp.handledCount, 1)
	return nil
}

type noOpProjection struct {
	ProjectionBase
}

func (p *noOpProjection) HandleEvent(ctx context.Context, evt Event) error { return nil }
func (p *noOpProjection) Reset(ctx context.Context) error                  { return nil }
func (p *noOpProjection) Topics() []string                                 { return nil }

var _ Projection = (*TestProjection)(nil)

// Optional convenience if you want an ID (instead of ObjectPath)
func (tp *TestProjection) ID() string { return tp.id }

func (tp *TestProjection) handled() int64 {
	return atomic.LoadInt64(&tp.handledCount)
}

func TestProjectionManager_Get(t *testing.T) {

	tests := []struct {
		name        string
		key         string
		setup       func(*ProjectionManager)
		expectedErr bool
	}{
		{
			name: "Get a projection that exists",
			key:  "proj1",
			setup: func(pm *ProjectionManager) {
				proj := &TestProjection{id: "proj1"}
				pm.Register(proj)
			},
			expectedErr: false,
		},
		{
			name:        "Get a projection that does not exist",
			key:         "proj2",
			setup:       func(pm *ProjectionManager) {},
			expectedErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := NewProjectionManager()
			tt.setup(pm)

			proj, err := pm.Get(tt.key)
			if tt.expectedErr {
				require.Error(t, err)
				assert.Nil(t, proj)
				return
			}

			require.NoError(t, err)
			assert.NotNil(t, proj)
		})
	}
}

func TestProjectionManager_RunToEnd(t *testing.T) {
	tests := []struct {
		name          string
		setup         func(*ProjectionManager, *Engine) string // returns projID
		expectedPos   int64
		expectedCount int
		expectErr     bool
		errContains   string
	}{
		{
			name: "Error_in_first_projection's_HandleEvent",
			setup: func(pm *ProjectionManager, eng *Engine) string {
				proj := &TestProjection{
					id:                "proj1",
					failOnEventNumber: 1,
				}
				pm.Register(proj)
				// Add test event to store
				eng.store.Save(context.Background(), &Event{GlobalVersion: 1}, &Event{GlobalVersion: 2})
				return proj.ID()
			},
			expectedPos:   0, // Position stays at initial 0 when error occurs
			expectedCount: 0,
			expectErr:     true,
			errContains:   "simulated error on event #1",
		},
		{
			name: "Successful_processing",
			setup: func(pm *ProjectionManager, eng *Engine) string {
				proj := &TestProjection{id: "proj2"}
				pm.Register(proj)
				// Add multiple events to store
				eng.store.Save(context.Background(),
					&Event{GlobalVersion: 1},
					&Event{GlobalVersion: 2},
				)
				return proj.ID()
			},
			expectedPos:   2,
			expectedCount: 2,
		},
		{
			name: "Projection_not_found",
			setup: func(pm *ProjectionManager, eng *Engine) string {
				return "nonexistent"
			},
			expectedPos:   0,
			expectedCount: 0,
			expectErr:     true,
			errContains:   "projection not found",
		},
		{
			name: "Empty_event_store",
			setup: func(pm *ProjectionManager, eng *Engine) string {
				proj := &TestProjection{id: "proj3"}
				pm.Register(proj)
				return proj.ID()
			},
			expectedPos:   0, // Position stays at initial 0 when no events to process
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := NewProjectionManager()
			eng := NewEngine(WithStore(&InMemoryStore{}))

			projID := tt.setup(pm, eng)
			ctx := golly.NewContext(context.Background())

			err := pm.RunToEnd(ctx, eng, projID)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}

			if proj, ok := pm.projections[projID]; ok {
				assert.Equal(t, int64(tt.expectedPos), proj.Position(ctx), "proj final position")
				assert.Equal(t, int64(tt.expectedCount), proj.(*TestProjection).handled(), "proj handled count")
			}
		})
	}
}

func TestProjectionManager_Rebuild(t *testing.T) {
	type scenario struct {
		name          string
		projName      string
		events        []Event
		failOnEvent   int64
		expectErr     bool
		errContains   string
		finalPosition int64
		handled       int64
	}

	cases := []scenario{
		{
			name:          "Rebuild from empty store",
			projName:      "projA",
			finalPosition: -1,
			handled:       0,
		},
		{
			name:          "Rebuild with multiple events",
			projName:      "projB",
			events:        []Event{{GlobalVersion: 1}, {GlobalVersion: 2}, {GlobalVersion: 3}},
			finalPosition: 3,
			handled:       3,
		},
		{
			name:          "HandleEvent error on first event",
			projName:      "projC",
			events:        []Event{{GlobalVersion: 5}, {GlobalVersion: 6}},
			failOnEvent:   1,
			expectErr:     true,
			errContains:   "simulated error on event #1",
			finalPosition: -1,
			handled:       0,
		},
		{
			name:        "Projection not registered",
			projName:    "unknown",
			expectErr:   true,
			errContains: "projection unknown not found",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := golly.NewContext(context.Background())

			// 1) Setup store & engine
			store := &InMemoryStore{}
			for _, evt := range c.events {
				_ = store.Save(context.Background(), &evt)
			}
			eng := NewEngine(WithStore(store))

			// 2) ProjectionManager + optional registration
			pm := NewProjectionManager()
			var proj *TestProjection
			if c.projName != "unknown" {
				proj = &TestProjection{id: c.projName}
				proj.failOnEventNumber = c.failOnEvent
				pm.Register(proj)
			}

			// 3) Rebuild
			err := pm.Rebuild(ctx, eng, c.projName)

			// 4) Check for error
			if c.expectErr {
				require.Error(t, err)
				if c.errContains != "" {
					assert.Contains(t, err.Error(), c.errContains)
				}
			} else {
				assert.NoError(t, err)
			}

			// 5) Validate final state if we have a real projection
			if proj != nil {
				assert.Equal(t, c.finalPosition, proj.Position(ctx), "final position mismatch")
				assert.Equal(t, c.handled, proj.handled(), "handled mismatch")
			}
		})
	}
}

func TestProjectionManager_List(t *testing.T) {
	t.Run("Empty manager", func(t *testing.T) {
		pm := NewProjectionManager()

		var count int
		for range pm.List() {
			count++
		}

		assert.Equal(t, 0, count, "Empty manager should have no projections")
	})

	t.Run("Single projection", func(t *testing.T) {
		pm := NewProjectionManager()
		proj := &TestProjection{id: "test1"}
		pm.Register(proj)

		var count int
		var ids []string
		var projections []Projection
		for id, p := range pm.List() {
			count++
			ids = append(ids, id)
			projections = append(projections, p)
		}

		assert.Equal(t, 1, count, "Should have one projection")
		assert.NotNil(t, projections[0], "Projection should not be nil")
		assert.NotEmpty(t, ids[0], "ID should not be empty")
	})

	t.Run("Multiple projections", func(t *testing.T) {
		pm := NewProjectionManager()
		proj1 := &TestProjection{id: "test1"}
		proj2 := &TestProjection{id: "test2"}
		proj3 := &noOpProjection{}

		pm.Register(proj1, proj2, proj3)

		var count int
		ids := make(map[string]bool)
		for id := range pm.List() {
			count++
			ids[id] = true
		}

		assert.Equal(t, 3, count, "Should have three projections")
		assert.Equal(t, 3, len(ids), "Should have three unique IDs")
	})

	t.Run("Early break", func(t *testing.T) {
		pm := NewProjectionManager()
		pm.Register(
			&TestProjection{id: "test1"},
			&TestProjection{id: "test2"},
			&TestProjection{id: "test3"},
		)

		var count int
		for range pm.List() {
			count++
			if count == 2 {
				break // Test early termination
			}
		}

		assert.Equal(t, 2, count, "Should stop after second item")
	})

	t.Run("Concurrent safety", func(t *testing.T) {
		pm := NewProjectionManager()
		pm.Register(
			&TestProjection{id: "test1"},
			&TestProjection{id: "test2"},
		)

		// Start iteration
		iter := pm.List()

		// Register another projection while iterating (shouldn't panic)
		pm.Register(&TestProjection{id: "test999"})

		var count int
		for range iter {
			count++
		}

		// Original snapshot should have 2 items, not affected by concurrent registration
		assert.Equal(t, 2, count, "Iterator should use snapshot from List() call time")
	})

	t.Run("Iteration with IDs and types", func(t *testing.T) {
		pm := NewProjectionManager()

		testProj := &TestProjection{id: "test1"}
		noOpProj := &noOpProjection{}

		pm.Register(testProj, noOpProj)

		var testProjCount, noOpProjCount int
		ids := make(map[string]Projection)
		for id, proj := range pm.List() {
			ids[id] = proj
			switch proj.(type) {
			case *TestProjection:
				testProjCount++
			case *noOpProjection:
				noOpProjCount++
			}
		}

		assert.Equal(t, 1, testProjCount, "Should have one TestProjection")
		assert.Equal(t, 1, noOpProjCount, "Should have one noOpProjection")
		assert.Equal(t, 2, len(ids), "Should have two ID-projection pairs")

		// Verify we can access projections by their IDs
		for id, proj := range ids {
			assert.NotEmpty(t, id, "ID should not be empty")
			assert.NotNil(t, proj, "Projection should not be nil")
		}
	})
}

func TestProjectionManager_handleEvent(t *testing.T) {
	ctx := context.Background()

	t.Run("Routes to event type handlers", func(t *testing.T) {
		pm := NewProjectionManager()
		var called bool
		handler := func(ctx context.Context, evt Event) error {
			called = true
			return nil
		}
		pm.eventHandlers["UserCreated"] = []ProjectionHandler{handler}

		pm.handleEvent(ctx, Event{Type: "UserCreated", Topic: "user"})
		assert.True(t, called)
	})

	t.Run("Routes to topic handlers", func(t *testing.T) {
		pm := NewProjectionManager()
		var called bool
		handler := func(ctx context.Context, evt Event) error {
			called = true
			return nil
		}
		pm.topicHandlers["user"] = []ProjectionHandler{handler}

		pm.handleEvent(ctx, Event{Type: "UserCreated", Topic: "user"})
		assert.True(t, called)
	})

	t.Run("Routes to both event and topic handlers", func(t *testing.T) {
		pm := NewProjectionManager()
		var eventCalls, topicCalls int

		eventHandler := func(ctx context.Context, evt Event) error {
			eventCalls++
			return nil
		}
		topicHandler := func(ctx context.Context, evt Event) error {
			topicCalls++
			return nil
		}

		pm.eventHandlers["UserCreated"] = []ProjectionHandler{eventHandler}
		pm.topicHandlers["user"] = []ProjectionHandler{topicHandler}

		pm.handleEvent(ctx, Event{Type: "UserCreated", Topic: "user"})
		assert.Equal(t, 1, eventCalls)
		assert.Equal(t, 1, topicCalls)
	})

	t.Run("No handlers matched", func(t *testing.T) {
		pm := NewProjectionManager()
		pm.eventHandlers["OtherEvent"] = []ProjectionHandler{
			func(ctx context.Context, evt Event) error { return nil },
		}

		// Should not panic
		pm.handleEvent(ctx, Event{Type: "UserCreated", Topic: "user"})
	})

	t.Run("Multiple handlers for same event type", func(t *testing.T) {
		pm := NewProjectionManager()
		var calls int

		handler1 := func(ctx context.Context, evt Event) error {
			calls++
			return nil
		}
		handler2 := func(ctx context.Context, evt Event) error {
			calls++
			return nil
		}

		pm.eventHandlers["UserCreated"] = []ProjectionHandler{handler1, handler2}

		pm.handleEvent(ctx, Event{Type: "UserCreated", Topic: "user"})
		assert.Equal(t, 2, calls)
	})

	t.Run("Handler error does not stop other handlers", func(t *testing.T) {
		pm := NewProjectionManager()
		var calls int

		handler1 := func(ctx context.Context, evt Event) error {
			calls++
			return errors.New("handler1 failed")
		}
		handler2 := func(ctx context.Context, evt Event) error {
			calls++
			return nil
		}

		pm.eventHandlers["UserCreated"] = []ProjectionHandler{handler1, handler2}

		pm.handleEvent(ctx, Event{Type: "UserCreated", Topic: "user"})
		assert.Equal(t, 2, calls, "Both handlers should be called despite error")
	})

	t.Run("Concurrent handleEvent calls are safe", func(t *testing.T) {
		pm := NewProjectionManager()
		var calls atomic.Int64

		handler := func(ctx context.Context, evt Event) error {
			calls.Add(1)
			time.Sleep(time.Millisecond)
			return nil
		}

		pm.eventHandlers["UserCreated"] = []ProjectionHandler{handler}

		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				pm.handleEvent(ctx, Event{Type: "UserCreated", Topic: "user"})
			}()
		}

		wg.Wait()
		assert.Equal(t, int64(10), calls.Load())
	})
}

// Benchmarks

func BenchmarkProjectionManager_handleEvent_SingleEventHandler(b *testing.B) {
	pm := NewProjectionManager()
	handler := func(ctx context.Context, evt Event) error { return nil }
	pm.eventHandlers["UserCreated"] = []ProjectionHandler{handler}

	evt := Event{Type: "UserCreated", Topic: "user"}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.handleEvent(ctx, evt)
	}
}

func BenchmarkProjectionManager_handleEvent_SingleTopicHandler(b *testing.B) {
	pm := NewProjectionManager()
	handler := func(ctx context.Context, evt Event) error { return nil }
	pm.topicHandlers["user"] = []ProjectionHandler{handler}

	evt := Event{Type: "UserCreated", Topic: "user"}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.handleEvent(ctx, evt)
	}
}

func BenchmarkProjectionManager_handleEvent_MultipleHandlers(b *testing.B) {
	pm := NewProjectionManager()
	handler := func(ctx context.Context, evt Event) error { return nil }

	pm.eventHandlers["UserCreated"] = []ProjectionHandler{handler, handler, handler}
	pm.topicHandlers["user"] = []ProjectionHandler{handler, handler}

	evt := Event{Type: "UserCreated", Topic: "user"}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.handleEvent(ctx, evt)
	}
}

func BenchmarkProjectionManager_handleEvent_NoMatch(b *testing.B) {
	pm := NewProjectionManager()
	handler := func(ctx context.Context, evt Event) error { return nil }
	pm.eventHandlers["OtherEvent"] = []ProjectionHandler{handler}
	pm.topicHandlers["other"] = []ProjectionHandler{handler}

	evt := Event{Type: "UserCreated", Topic: "user"}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pm.handleEvent(ctx, evt)
	}
}

func BenchmarkProjectionManager_handleEvent_Parallel(b *testing.B) {
	pm := NewProjectionManager()
	handler := func(ctx context.Context, evt Event) error { return nil }
	pm.eventHandlers["UserCreated"] = []ProjectionHandler{handler}

	evt := Event{Type: "UserCreated", Topic: "user"}
	ctx := context.Background()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pm.handleEvent(ctx, evt)
		}
	})
}
