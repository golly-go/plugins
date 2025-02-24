package eventsource

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

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
			expectedPos:   -1,
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
			expectedPos:   -1,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pm := NewProjectionManager()
			eng := NewEngine(&InMemoryStore{})

			projID := tt.setup(pm, eng)

			err := pm.RunToEnd(golly.NewContext(context.Background()), eng, projID)

			if tt.expectErr {
				require.Error(t, err)
				if tt.errContains != "" {
					assert.Contains(t, err.Error(), tt.errContains)
				}
			} else {
				assert.NoError(t, err)
			}

			if proj, ok := pm.projections[projID]; ok {
				assert.Equal(t, int64(tt.expectedPos), proj.Position(), "proj final position")
				assert.Equal(t, int64(tt.expectedCount), proj.(*TestProjection).handled(), "proj handled count")
			}
		})
	}
}

func TestProjectionManager_RunOnce(t *testing.T) {
	type scenario struct {
		name          string
		projName      string
		initialPos    int64
		events        []Event
		failOnEvent   int64
		expectErr     bool
		errContains   string
		finalPosition int64
		handled       int64
	}

	cases := []scenario{
		{
			name:          "RunOnce from position -1 with 3 new events",
			projName:      "projOne",
			initialPos:    -1,
			events:        []Event{{GlobalVersion: 1}, {GlobalVersion: 2}, {GlobalVersion: 3}},
			finalPosition: 3,
			handled:       3,
		},
		{
			name:          "HandleEvent error on second event",
			projName:      "projErr",
			initialPos:    10,
			events:        []Event{{GlobalVersion: 11}, {GlobalVersion: 12}},
			failOnEvent:   2, // the 2nd event fails
			expectErr:     true,
			errContains:   "simulated error on event #2",
			finalPosition: 10,
			handled:       1,
		},
		{
			name:        "Projection not found",
			projName:    "missing",
			expectErr:   true,
			errContains: "projection not found: missing",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx := golly.NewContext(context.Background())

			// 1) Seed events in store
			store := &InMemoryStore{}
			for _, evt := range c.events {
				_ = store.Save(context.Background(), &evt)
			}
			eng := NewEngine(store)

			// 2) ProjectionManager + register if not missing
			pm := NewProjectionManager()
			var proj *TestProjection
			if c.projName != "missing" {
				proj = &TestProjection{id: c.projName}
				proj.SetPosition(c.initialPos)
				proj.failOnEventNumber = c.failOnEvent
				pm.Register(proj)
			}

			// 3) RunOnce
			err := pm.RunOnce(ctx, eng, c.projName)

			// 4) Check error
			if c.expectErr {
				require.Error(t, err)
				if c.errContains != "" {
					assert.Contains(t, err.Error(), c.errContains)
				}
			} else {
				assert.NoError(t, err)
			}

			// 5) If we have a real projection, verify
			if proj != nil {
				assert.Equal(t, c.finalPosition, proj.Position(), "final position mismatch")
				assert.Equal(t, c.handled, proj.handled(), "handled mismatch")
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
			eng := NewEngine(store)

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
				assert.Equal(t, c.finalPosition, proj.Position(), "final position mismatch")
				assert.Equal(t, c.handled, proj.handled(), "handled mismatch")
			}
		})
	}
}
