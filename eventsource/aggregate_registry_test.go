package eventsource

import (
	"reflect"
	"testing"

	"github.com/golly-go/golly/utils"
	"github.com/stretchr/testify/assert"
)

func TestRegistry_RegisterAndRetrieve(t *testing.T) {
	reg := NewAggregateRegistry()

	reg.Register(&TestAggregate{},
		[]any{
			&testEvent{},
		},
	)

	itemName := utils.GetTypeWithPackage(TestAggregate{})
	eventName := utils.GetTypeWithPackage(testEvent{})

	// Test aggregate registration
	item, exists := reg.Get(itemName)
	assert.True(t, exists, "Aggregate should be registered")

	assert.Equal(t, reflect.TypeOf(&TestAggregate{}), item.Type)

	// Test event registration under aggregate
	evtType, evtExists := reg.GetEventType(itemName, eventName)

	assert.True(t, evtExists, "Event should be registered under aggregate")
	assert.Equal(t, reflect.TypeOf(&testEvent{}), evtType)

}

func TestRegistry_GetNonExistent(t *testing.T) {
	reg := NewAggregateRegistry()
	_, exists := reg.Get("NonExistentAggregate")
	assert.False(t, exists, "Non-existent aggregate should not be found")
}

func TestRegistry_GetEventTypeNonExistent(t *testing.T) {
	reg := NewAggregateRegistry()
	itemName := utils.GetTypeWithPackage(TestAggregate{})
	_, exists := reg.GetEventType(itemName, "NonExistentEvent")
	assert.False(t, exists, "Non-existent event should not be found")
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
