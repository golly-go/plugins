package eventsource

import (
	"fmt"
	"iter"
	"reflect"
	"sync"
	"sync/atomic"

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
// Reads are lock-free: items is an immutable snapshot replaced copy-on-write by
// Register/Clear, which serialize on mu (writers only).
type AggregateRegistry struct {
	mu    sync.Mutex
	items atomic.Pointer[map[string]*AggregateRegistryItem]
}

// NewAggregateRegistry initializes an empty registry with a normal map.
func NewAggregateRegistry() *AggregateRegistry {
	r := &AggregateRegistry{}
	r.items.Store(&map[string]*AggregateRegistryItem{})
	return r
}

// List returns an iterator over all registered aggregate types.
// This can be used with Go 1.23+ range-over-func:
//
//	for agg := range registry.List() {
//	    // use agg
//	}
func (r *AggregateRegistry) List() iter.Seq[*AggregateRegistryItem] {
	items := *r.items.Load()
	aggs := make([]*AggregateRegistryItem, 0, len(items))
	for _, agg := range items {
		aggs = append(aggs, agg)
	}

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

	cur := *r.items.Load()
	next := make(map[string]*AggregateRegistryItem, len(cur)+1)
	for k, v := range cur {
		next[k] = v
	}

	// Copy the item so published snapshots stay immutable
	regItem := &AggregateRegistryItem{
		AggregateType: aggType,
		Events:        make(map[string]*EventCodec, len(eventSamples)),
	}
	if old, found := cur[aggName]; found {
		regItem.AggregateType = old.AggregateType
		regItem.Events = make(map[string]*EventCodec, len(old.Events)+len(eventSamples))
		for k, v := range old.Events {
			regItem.Events[k] = v
		}
	}
	next[aggName] = regItem

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

	r.items.Store(&next)

	return r
}

// GetAggregate instantiates a new aggregate for the given name (e.g., "MyAggregate").
func (r *AggregateRegistry) GetAggregate(aggName string) (Aggregate, bool) {
	regItem, ok := (*r.items.Load())[aggName]
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
	regItem, ok := (*r.items.Load())[aggName]
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
	r.items.Store(&map[string]*AggregateRegistryItem{})
}
