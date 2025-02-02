package eventsource

import (
	"reflect"
	"sync"

	"github.com/golly-go/golly"
)

const (
	AllEvents = "*"
)

const (
	DefaultStreamName = "default"
)

type StreamHandler func(*golly.Context, Event)

// Stream represents a single in-memory event stream with subscriptions.
type Stream struct {
	name string
	mu   sync.RWMutex

	handlers     map[string][]StreamHandler // eventType -> list of handler funcs
	aggregations map[string][]StreamHandler // Aggregations

}

// NewStream initializes a new Stream.
func NewStream(name string) *Stream {
	return &Stream{
		name:         name,
		handlers:     make(map[string][]StreamHandler),
		aggregations: make(map[string][]StreamHandler),
	}
}

// Name returns the name of the stream.
func (s *Stream) Name() string {
	return s.name
}

// Send dispatches an event to all subscribers of event.Type and to any
// subscribers of AllEvents ("*"). Dispatching is done asynchronously.
func (s *Stream) Send(gctx *golly.Context, events ...Event) {
	for _, event := range events {
		eventType := golly.TypeNoPtr(event).String()

		s.mu.RLock()

		combined := []StreamHandler{}
		if sh, ok := s.handlers[eventType]; ok {
			combined = sh
		}

		if ah, ok := s.aggregations[event.AggregateType]; ok {
			combined = append(combined, ah...)
		}

		if wh, ok := s.handlers[AllEvents]; ok {
			combined = append(combined, wh...)
		}

		s.mu.RUnlock()

		// Invoke handlers asynchronously
		for _, handler := range combined {
			handler(gctx, event)
		}
	}
}

func (s *Stream) Aggregate(aggregate any, handler StreamHandler) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()

	var aggregateType string
	switch ag := aggregate.(type) {
	case string:
		aggregateType = ag
	default:
		aggregateType = ObjectName(aggregate)
	}

	s.aggregations[aggregateType] = append(s.aggregations[aggregateType], handler)
	return s
}

// Subscribe registers a handler for a specific event type (or AllEvents).
func (s *Stream) Subscribe(eventType string, handler StreamHandler) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.handlers[eventType] = append(s.handlers[eventType], handler)
	return s
}

// Unsubscribe removes a handler for a specific event type (or AllEvents).
func (s *Stream) Unsubscribe(eventType string, handler StreamHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	handlers := s.handlers[eventType]
	for i, h := range handlers {
		// Compare function pointers via reflection
		if reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer() {
			// Remove from slice
			s.handlers[eventType] = append(handlers[:i], handlers[i+1:]...)
			break
		}
	}
}
