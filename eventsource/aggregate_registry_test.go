package eventsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistry_RegisterAndRetrieve(t *testing.T) {
	reg := NewAggregateRegistry()

	reg.Register(&TestAggregate{}, []any{
		&testEvent{},
	})

	// For the aggregator name and event name, we rely on the same logic used by the registry
	aggName := ObjectName(TestAggregate{})
	evtName := ObjectName(testEvent{})

	// 1) Verify the aggregate is registered
	agg, exists := reg.GetAggregate(aggName)
	require.True(t, exists, "Aggregate should be registered")
	// Check that we got the right type back
	assert.IsType(t, &TestAggregate{}, agg, "expected a *TestAggregate")

	// 2) Verify the event is registered
	codec := reg.GetEventCodec(aggName, evtName)
	assert.NotNil(t, codec, "Event should be registered under the aggregate")

	// Optional: you can also verify the codec works by doing a small round-trip test
	raw, err := codec.MarshalFn(&testEvent{})
	assert.NoError(t, err, "marshalling testEvent should not fail")

	unmarshaled, err := codec.UnmarshalFn(raw)
	require.NoError(t, err, "unmarshalling testEvent should not fail")
	assert.IsType(t, &testEvent{}, unmarshaled)
}

func TestRegistry_GetNonExistent(t *testing.T) {
	reg := NewAggregateRegistry()
	agg, exists := reg.GetAggregate("NonExistentAggregate")
	assert.False(t, exists, "Non-existent aggregate should not be found")
	assert.Nil(t, agg, "Returned aggregate should be nil")
}

func TestRegistry_GetEventTypeNonExistent(t *testing.T) {
	reg := NewAggregateRegistry()

	// Register an aggregate but no events
	reg.Register(&TestAggregate{}, []any{})

	aggName := ObjectName(TestAggregate{})
	evtName := "NonExistentEvent"

	codec := reg.GetEventCodec(aggName, evtName)
	assert.Nil(t, codec, "Non-existent event should not be found")

}

func TestGetAggregate(t *testing.T) {
	registry := NewAggregateRegistry()

	registry.Register(&TestAggregate{}, []any{})

	tests := []struct {
		name        string
		lookupType  string
		expectFound bool
	}{
		{
			name: "Found aggregate",

			lookupType:  "eventsource.TestAggregate",
			expectFound: true,
		},
		{
			name:        "Aggregate not found",
			lookupType:  "garbo.UnknownAggregate",
			expectFound: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg, found := registry.GetAggregate(tt.lookupType)
			assert.Equal(t, tt.expectFound, found)
			if tt.expectFound {
				assert.NotNil(t, agg)
			} else {
				assert.Nil(t, agg)
			}
		})
	}
}
