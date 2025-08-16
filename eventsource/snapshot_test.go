package eventsource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type snapAgg struct {
	AggregateBase
	ID    string
	Count int
}

func (s *snapAgg) GetID() string { return s.ID }

func TestShouldSnapshot(t *testing.T) {
	// Crossing boundaries of SnapShotIncrement should trigger
	assert.False(t, ShouldSnapshot(0, 50))
	assert.True(t, ShouldSnapshot(99, 100))
	assert.False(t, ShouldSnapshot(100, 150))
	assert.True(t, ShouldSnapshot(199, 200))
}

func TestNewSnapshotAndApplySnapshot(t *testing.T) {
	a := &snapAgg{ID: "agg1"}
	a.SetVersion(123)
	a.Count = 7

	s := NewSnapshot(a)
	assert.Equal(t, EventKindSnapshot, s.Kind)
	assert.Equal(t, int64(123), s.Version)
	assert.Equal(t, "agg1", s.AggregateID)
	assert.Equal(t, ObjectName(a), s.AggregateType)

	// Apply snapshot back into a new aggregate
	b := &snapAgg{ID: "agg1"}
	err := ApplySnapshot(b, s)
	assert.NoError(t, err)
	assert.Equal(t, int64(123), b.Version())
	// State restored
	assert.Equal(t, 7, b.Count)
}

func TestEngineProcessChangesTriggersSnapshotEvent(t *testing.T) {
	a := &snapAgg{ID: "agg2"}
	a.SetVersion(SnapShotIncrement - 1)
	a.Record(struct{ X int }{X: 1})

	// After processing changes, one additional snapshot event should be appended
	err := a.ProcessChanges(context.Background(), a)
	assert.NoError(t, err)
	events := a.Changes()
	// Last event should be a snapshot due to crossing boundary
	assert.Equal(t, EventKindSnapshot, events[len(events)-1].Kind)
}
