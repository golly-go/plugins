package eventsource

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestProjection implements the Projection interface.
// It embeds ProjectionBase for position tracking and
// tracks how many events it handled, optionally returning an error once.
type TestProjection struct {
	ProjectionBase

	id string

	handledCount int64

	// If > 0, the nth event will return an error.
	failOnEventNumber int64

	// track how many events processed so far
	eventCounter int64
}

func (tp *TestProjection) HandleEvent(ctx context.Context, evt Event) error {
	current := atomic.AddInt64(&tp.eventCounter, 1) // which event are we on?
	if tp.failOnEventNumber > 0 && current == tp.failOnEventNumber {
		return fmt.Errorf("simulated error on event #%d", current)
	}

	atomic.AddInt64(&tp.handledCount, 1)
	return nil
}

// Optional convenience if you want an ID (instead of ObjectPath)
func (tp *TestProjection) ID() string     { return tp.id }
func (tp *TestProjection) handled() int64 { return atomic.LoadInt64(&tp.handledCount) }

func TestProjectionManager_RunToEnd(t *testing.T) {
	type scenario struct {
		name         string
		events       []Event
		projCount    int   // How many projections to register
		failOnEvent  int64 // If > 0, the nth event in the first projection fails
		expectErr    bool
		errContains  string
		wantPosition []int64
		wantHandled  []int64
	}

	cases := []scenario{
		{
			name:         "Two projections catch up from 0",
			events:       []Event{{GlobalVersion: 1}, {GlobalVersion: 2}},
			projCount:    2,
			wantPosition: []int64{2, 2},
			wantHandled:  []int64{2, 2},
		},
		{
			name:         "Error in first projection's HandleEvent",
			events:       []Event{{GlobalVersion: 10}},
			projCount:    2,
			failOnEvent:  1,
			expectErr:    true,
			errContains:  "simulated error on event #1",
			wantPosition: []int64{-1, -1},
			wantHandled:  []int64{0, 0},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 1) In-memory store with seeded events
			store := &InMemoryStore{}
			for _, e := range tc.events {
				_ = store.Save(context.Background(), &e)
			}

			// 2) Engine & ProjectionManager
			eng := NewEngine(store)
			pm := NewProjectionManager()

			// 3) Create and register projections
			var projs []*TestProjection
			for i := 0; i < tc.projCount; i++ {
				p := &TestProjection{id: fmt.Sprintf("proj%d", i)}
				p.SetPosition(-1) // no events processed yet

				if i == 0 && tc.failOnEvent > 0 {
					p.failOnEventNumber = tc.failOnEvent
				}
				pm.Register(p)
				projs = append(projs, p)
			}

			// 4) RunToEnd
			err := pm.RunToEnd(context.Background(), eng)

			// 5) Assertions
			if tc.expectErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
			} else {
				assert.NoError(t, err)
			}

			// Validate final positions & handled counts
			for i, p := range projs {
				assert.Equal(t, tc.wantPosition[i], p.Position(), "proj %d final position", i)
				assert.Equal(t, tc.wantHandled[i], p.handled(), "proj %d handled count", i)
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
			err := pm.RunOnce(context.Background(), eng, c.projName)

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
			err := pm.Rebuild(context.Background(), eng, c.projName)

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
