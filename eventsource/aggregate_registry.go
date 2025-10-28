package eventsource

import (
	"fmt"
	"iter"
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

// List returns an iterator over all registered aggregate types.
// This can be used with Go 1.23+ range-over-func:
//
//	for agg := range registry.List() {
//	    // use agg
//	}
func (r *AggregateRegistry) List() iter.Seq[*AggregateRegistryItem] {
	r.mu.RLock()
	aggs := make([]*AggregateRegistryItem, 0, len(r.items))
	for _, agg := range r.items {
		aggs = append(aggs, agg)
	}
	r.mu.RUnlock()

	return func(yield func(*AggregateRegistryItem) bool) {
		for _, agg := range aggs {
			if !yield(agg) {
				return
			}
		}
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

		regItem.Events[eventName] = &EventCodec{
			UnmarshalFn: func(raw any) (any, error) {
				newVal := reflect.New(evtType.Elem())

				if err := unmarshal(newVal.Interface(), raw); err != nil {
					return nil, err
				}

				return newVal.Elem().Interface(), nil
			},
			MarshalFn: func(obj any) ([]byte, error) {
				return json.Marshal(obj)
			},
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

// Get provides a compatibility helper returning an aggregate or error.
func (r *AggregateRegistry) Get(aggName string) (Aggregate, error) {
	if agg, ok := r.GetAggregate(aggName); ok {
		return agg, nil
	}
	return nil, fmt.Errorf("aggregate %s not found", aggName)
}

// GetEventCodec retrieves the EventCodec for (aggregateName, eventName).
func (r *AggregateRegistry) GetEventCodec(aggName, evtName string) *EventCodec {
	r.mu.RLock()
	defer r.mu.RUnlock()

	regItem, ok := r.items[aggName]
	if !ok {
		return nil
	}

	return regItem.Events[evtName]
}

// Clear removes all aggregates and event codecs from the registry (helpful in tests).
func (r *AggregateRegistry) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Reset the top-level map
	r.items = make(map[string]*AggregateRegistryItem)
}
