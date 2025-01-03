package eventsource

import (
	"fmt"
	"reflect"
	"sync"
)

var (
	aggregateRegistery = NewAggregateRegistry()

	ErrorAggregateTypeNotRegistered = fmt.Errorf("Aggregate type not registered")
)

func Aggregates() *AggregateRegistry {
	return aggregateRegistery
}

// RegistryItem represents an aggregate or projection type and its supported events.
type AggregateRegistryItem struct {
	Type   reflect.Type // Type of the aggregate or projection
	Events sync.Map
}

// Registry manages mappings of types and their associated events using sync.Map.
type AggregateRegistry struct {
	items sync.Map
}

// NewRegistry initializes a new event registry.
func NewAggregateRegistry() *AggregateRegistry {
	return &AggregateRegistry{}
}

func (r *AggregateRegistry) Register(item Aggregate, events []any) {
	var regItem *AggregateRegistryItem

	itemType := reflect.TypeOf(item)

	if itemType.Kind() != reflect.Ptr {
		itemType = reflect.PointerTo(itemType)
	}

	itemName := ObjectName(item)

	if value, ok := r.items.Load(itemName); ok {
		regItem = value.(*AggregateRegistryItem)
	} else {
		regItem = &AggregateRegistryItem{Type: itemType}
	}

	// Register events
	for _, evt := range events {

		evtType := reflect.TypeOf(evt)

		if evtType.Kind() != reflect.Ptr {
			evtType = reflect.PointerTo(evtType)
		}

		eventName := ObjectName(evt)
		regItem.Events.Store(eventName, evtType)
	}

	r.items.Store(itemName, regItem)
}

// Get retrieves a registered item by name.
func (r *AggregateRegistry) Get(name string) (*AggregateRegistryItem, bool) {
	value, exists := r.items.Load(name)
	if !exists {
		return nil, false
	}
	return value.(*AggregateRegistryItem), true
}

// GetEventType retrieves an event type by aggregate/projection and event name.
func (r *AggregateRegistry) GetEventType(itemName, eventName string) (reflect.Type, bool) {
	value, exists := r.items.Load(itemName)
	if !exists {
		return nil, false
	}

	regItem := value.(*AggregateRegistryItem)

	evtValue, exists := regItem.Events.Load(eventName)
	if !exists {
		return nil, false
	}

	return evtValue.(reflect.Type), true
}

// GetAggregate retrieves the top-level aggregate type from the registry.
func (r *AggregateRegistry) GetAggregate(tpe string) (Aggregate, bool) {
	value, exists := r.items.Load(tpe)
	if !exists {
		return nil, false
	}

	regType := value.(*AggregateRegistryItem).Type

	var inf interface{}

	if regType.Kind() == reflect.Ptr {
		inf = reflect.New(regType.Elem()).Interface()
	} else {
		inf = reflect.New(regType).Interface()
	}

	ret, ok := inf.(Aggregate)
	if !ok {
		return nil, false
	}

	return ret, true
}

func (r *AggregateRegistry) Clear() {
	r.items.Clear()
}
