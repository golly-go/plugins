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
	assert.IsType(t, testEvent{}, unmarshaled)
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

type regAgg struct {
	AggregateBase
	id string
}

func (r *regAgg) GetID() string { return r.id }

func TestAggregateRegistry_RegisterGet(t *testing.T) {
	ar := NewAggregateRegistry()
	a := &regAgg{id: "1"}
	ar.Register(a, []any{"evt1", struct{}{}})

	got, err := ar.Get(ObjectName(a))
	assert.NoError(t, err)
	assert.NotNil(t, got)

	_, err = ar.Get("missing")
	assert.Error(t, err)
}

func TestAggregateRegistry_List(t *testing.T) {
	t.Run("Empty registry", func(t *testing.T) {
		ar := NewAggregateRegistry()

		var count int
		for range ar.List() {
			count++
		}

		assert.Equal(t, 0, count, "Empty registry should have no items")
	})

	t.Run("Single aggregate", func(t *testing.T) {
		ar := NewAggregateRegistry()
		ar.Register(&TestAggregate{}, []any{&testEvent{}})

		var count int
		var items []*AggregateRegistryItem
		for item := range ar.List() {
			count++
			items = append(items, item)
		}

		assert.Equal(t, 1, count, "Should have one item")
		assert.NotNil(t, items[0], "Item should not be nil")
		assert.NotNil(t, items[0].AggregateType, "AggregateType should be set")
		assert.NotNil(t, items[0].Events, "Events map should be initialized")
	})

	t.Run("Multiple aggregates", func(t *testing.T) {
		ar := NewAggregateRegistry()
		ar.Register(&TestAggregate{}, []any{&testEvent{}})
		ar.Register(&regAgg{id: "1"}, []any{"evt1"})
		ar.Register(&regAgg{id: "2"}, []any{"evt2"})

		var count int
		aggregateTypes := make(map[string]bool)
		for item := range ar.List() {
			count++
			typeName := item.AggregateType.String()
			aggregateTypes[typeName] = true
		}

		assert.Equal(t, 2, count, "Should have two unique aggregate types")
		assert.True(t, len(aggregateTypes) >= 2, "Should have at least 2 different types")
	})

	t.Run("Early break", func(t *testing.T) {
		ar := NewAggregateRegistry()
		ar.Register(&TestAggregate{}, []any{&testEvent{}})
		ar.Register(&regAgg{id: "1"}, []any{"evt1"})
		ar.Register(&regAgg{id: "2"}, []any{"evt2"})

		var count int
		for range ar.List() {
			count++
			if count == 1 {
				break // Test early termination
			}
		}

		assert.Equal(t, 1, count, "Should stop after first item")
	})

	t.Run("Concurrent safety", func(t *testing.T) {
		ar := NewAggregateRegistry()
		ar.Register(&TestAggregate{}, []any{&testEvent{}})
		ar.Register(&regAgg{id: "1"}, []any{"evt1"})

		// Start iteration
		iter := ar.List()

		// Register another aggregate while iterating (shouldn't panic)
		ar.Register(&regAgg{id: "999"}, []any{"evt999"})

		var count int
		for range iter {
			count++
		}

		// Original snapshot should have 2 items, not affected by concurrent registration
		assert.Equal(t, 2, count, "Iterator should use snapshot from List() call time")
	})
}
