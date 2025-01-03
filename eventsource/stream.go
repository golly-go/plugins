package eventsource

import (
	"reflect"
	"sync"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
)

const (
	AllEvents = "*"
)

const (
	DefaultStreamName = "default"
)

func DefaultStream() *Stream {
	if s, ok := streamManager.Get(DefaultStreamName); ok {
		return s
	}

	return streamManager.Register(DefaultStreamName)
}

type StreamHandler func(golly.Context, Event)

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
func (s *Stream) Send(gctx golly.Context, events ...Event) {
	for _, event := range events {
		eventType := utils.GetTypeWithPackage(event)

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

func (s *Stream) Aggregate(aggregateType string, handler StreamHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.aggregations[aggregateType] = append(s.aggregations[aggregateType], handler)
}

// Subscribe registers a handler for a specific event type (or AllEvents).
func (s *Stream) Subscribe(eventType string, handler StreamHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.handlers[eventType] = append(s.handlers[eventType], handler)
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
