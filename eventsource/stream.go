package eventsource

import (
	"fmt"
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
	queue        *streamQueue               // Per-stream queue
	started      bool
}

// NewStream initializes a new Stream with options
func NewStream(opts StreamOptions) *Stream {
	s := &Stream{
		name:         opts.Name,
		handlers:     make(map[string][]StreamHandler),
		aggregations: make(map[string][]StreamHandler),
	}

	s.queue = newStreamQueue(StreamQueueConfig{
		NumPartitions: opts.NumPartitions,
		BufferSize:    opts.BufferSize,
		Handler:       s.handleStreamEvent,
	})

	return s
}

// Name returns the name of the stream.
func (s *Stream) Name() string {
	return s.name
}

// Send dispatches events to the stream's queue
func (s *Stream) Send(gctx *golly.Context, events ...Event) error {
	if len(events) == 0 {
		return nil
	}

	for _, evt := range events {
		if err := s.queue.enqueue(gctx, evt); err != nil {
			return fmt.Errorf("failed to enqueue event: %w", err)
		}
	}
	return nil
}

// Start begins processing events in the stream
func (s *Stream) Start() {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return
	}
	s.started = true
	s.mu.Unlock()

	s.queue.start()
}

// Stop gracefully shuts down event processing
func (s *Stream) Stop() {
	s.queue.stop()
}

// handleStreamEvent processes events for this stream
func (s *Stream) handleStreamEvent(ctx *golly.Context, event Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	eventType := event.Type
	if eventType == "" {
		eventType = resolveName(event.Data)
	}

	// Collect all relevant handlers
	var handlers []StreamHandler

	// Event type specific handlers
	if h, ok := s.handlers[eventType]; ok {
		handlers = append(handlers, h...)
	}

	// Aggregate type handlers
	if h, ok := s.aggregations[event.AggregateType]; ok {
		handlers = append(handlers, h...)
	}

	// Global handlers
	if h, ok := s.handlers[AllEvents]; ok {
		handlers = append(handlers, h...)
	}

	// Process handlers
	for _, handler := range handlers {
		handler(ctx, event)
	}
}

func (s *Stream) Aggregate(aggregate any, handler StreamHandler) *Stream {
	s.mu.Lock()
	defer s.mu.Unlock()

	aggregateType := resolveName(aggregate)

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

// Project subscribes the projection's aggregates/events to this stream.
func (s *Stream) Project(proj Projection) {
	aggs, events := projectionSteamConfig(proj)

	for _, agg := range aggs {
		s.Aggregate(agg, func(ctx *golly.Context, evt Event) {
			if err := proj.HandleEvent(ctx, evt); err != nil {
				ctx.Logger().Errorf("cannot process projection %s (%s)", projectionKey(proj), err)
			}
		})
	}

	for _, evtType := range events {
		s.Subscribe(evtType, func(ctx *golly.Context, evt Event) {
			if err := proj.HandleEvent(ctx, evt); err != nil {
				ctx.Logger().Errorf("cannot process projection %s (%s)", projectionKey(proj), err)
			}
		})
	}
}

func resolveName(obj any) (name string) {
	switch o := obj.(type) {
	case string:
		name = o
	default:
		name = ObjectName(o)
	}
	return
}

func streamName(cfg *StreamOptions) string {
	if cfg == nil {
		return DefaultStreamName
	}
	return cfg.Name
}
