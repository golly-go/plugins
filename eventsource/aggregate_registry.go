package eventsource

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/segmentio/encoding/json"
)

// EventCodec holds closures for marshalling/unmarshalling a particular event type.
type EventCodec struct {
	UnmarshalFn func(any) (any, error)
	MarshalFn   func(any) ([]byte, error)
}

// AggregateRegistryItem represents a single aggregate type (e.g., *Order)
// and its associated event codecs (e.g., "OrderCreated" -> {marshal/unmarshal}).
type AggregateRegistryItem struct {
	AggregateType reflect.Type
	Events        map[string]*EventCodec
}

// AggregateRegistry manages mappings of aggregate names to registry items.
// Internally, it uses a standard map guarded by a sync.RWMutex.
type AggregateRegistry struct {
	mu    sync.RWMutex
	items map[string]*AggregateRegistryItem
}

// NewAggregateRegistry initializes an empty registry with a normal map.
func NewAggregateRegistry() *AggregateRegistry {
	return &AggregateRegistry{
		items: make(map[string]*AggregateRegistryItem),
	}
}

// Register takes an aggregate (e.g., &MyAggregate{}) and a slice of event “samples”.
// For each event sample, it precomputes an EventCodec and stores it in the registry.
func (r *AggregateRegistry) Register(agg Aggregate, eventSamples []any) *AggregateRegistry {
	aggType := reflect.TypeOf(agg)
	if aggType.Kind() != reflect.Ptr {
		// Force pointer type so we can instantiate it later
		aggType = reflect.PointerTo(aggType)
	}

	aggName := ObjectName(agg)

	r.mu.Lock()
	defer r.mu.Unlock()

	// Fetch or create the registry item for this aggregate
	regItem, found := r.items[aggName]
	if !found {
		regItem = &AggregateRegistryItem{
			AggregateType: aggType,
			Events:        make(map[string]*EventCodec),
		}
		r.items[aggName] = regItem
	}

	// Build codecs for each event sample
	for _, evtSample := range eventSamples {
		evtType := reflect.TypeOf(evtSample)
		if evtType.Kind() != reflect.Ptr {
			evtType = reflect.PointerTo(evtType)
		}

		eventName := ObjectName(evtSample)

		unmarshalFn := func(raw any) (any, error) {
			newVal := reflect.New(evtType.Elem())

			if err := unmarshal(newVal.Interface(), raw); err != nil {
				return nil, err
			}

			return newVal.Elem().Interface(), nil
		}

		marshalFn := func(obj any) ([]byte, error) {
			// Optional: type-check to ensure correct type
			if reflect.TypeOf(obj) != evtType {
				return nil, fmt.Errorf("expected type %v, got %v", evtType, reflect.TypeOf(obj))
			}
			return json.Marshal(obj)
		}

		regItem.Events[eventName] = &EventCodec{
			UnmarshalFn: unmarshalFn,
			MarshalFn:   marshalFn,
		}
	}

	return r
}

// GetAggregate instantiates a new aggregate for the given name (e.g., "MyAggregate").
func (r *AggregateRegistry) GetAggregate(aggName string) (Aggregate, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	regItem, ok := r.items[aggName]
	if !ok {
		return nil, false
	}

	aggType := regItem.AggregateType
	var inf interface{}
	if aggType.Kind() == reflect.Ptr {
		inf = reflect.New(aggType.Elem()).Interface()
	} else {
		inf = reflect.New(aggType).Interface()
	}

	agg, ok := inf.(Aggregate)
	return agg, ok
}

// GetEventCodec retrieves the EventCodec for (aggregateName, eventName).
func (r *AggregateRegistry) GetEventCodec(aggName, evtName string) *EventCodec {
	r.mu.RLock()
	defer r.mu.RUnlock()

	regItem, ok := r.items[aggName]
	if !ok {
		return nil
	}
	codec, ok := regItem.Events[evtName]
	return codec
}

// Clear removes all aggregates and event codecs from the registry (helpful in tests).
func (r *AggregateRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Reset the top-level map
	r.items = make(map[string]*AggregateRegistryItem)
}
