package eventsource

import (
	"context"
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

// StreamNamed provides a debug/metrics-friendly name for a stream.
type StreamNamed interface{ Name() string }

// StreamPublisher can publish an event to a topic on this stream (in-memory or outbound).
type StreamPublisher interface {
	Publish(ctx context.Context, topic string, event any) error
}

// StreamSubscriber can subscribe a handler to receive events for a topic ("*" for all).
type StreamSubscriber interface {
	Subscribe(topic string, handler StreamHandler)
}

// StreamLifecycle allows external lifecycle control; optional.
type StreamLifecycle interface {
	Start()
	Stop()
}

type StreamHandler func(context.Context, Event)

// Stream represents a single in-memory event stream with subscriptions.
type Stream struct {
	name string
	mu   sync.RWMutex

	handlers map[string][]StreamHandler // topic -> handlers; supports "*"
	queue    *StreamQueue               // Per-stream queue
	started  bool
}

// NewStream initializes a new Stream with options
func NewStream(opts StreamOptions) *Stream {
	s := &Stream{
		name:     opts.Name,
		handlers: make(map[string][]StreamHandler),
	}

	s.queue = NewStreamQueue(StreamQueueConfig{
		Name:          streamName(&opts),
		NumPartitions: opts.NumPartitions,
		BufferSize:    opts.BufferSize,
		Handler:       s.handleStreamEvent,
	})

	return s
}

// Name returns the name of the stream.
func (s *Stream) Name() string { return s.name }

// Publish pushes a single event to a topic on this stream.
func (s *Stream) Publish(ctx context.Context, topic string, evt any) error {
	e := evt.(Event)

	golly.Logger().Tracef("publish event to stream %s topic=%s", s.name, topic)
	return s.queue.Enqueue(ctx, e)
}

// Send dispatches events to the stream's queue (kept for Engine bulk-send compatibility)
func (s *Stream) Send(ctx context.Context, events ...Event) error {
	for _, evt := range events {
		if err := s.queue.Enqueue(ctx, evt); err != nil {
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
	golly.Logger().Tracef("starting stream %s numPartitions=%d", s.name, s.queue.numParts)
	s.queue.Start()
}

// Stop gracefully shuts down event processing
func (s *Stream) Stop() { s.queue.Stop() }

// handleStreamEvent processes events for this stream
func (s *Stream) handleStreamEvent(ctx context.Context, event Event) {
	s.mu.RLock()
	hs := make([]StreamHandler, 0, 8)
	if h, ok := s.handlers[event.Topic]; ok {
		hs = append(hs, h...)
	}
	if h, ok := s.handlers[AllEvents]; ok {
		hs = append(hs, h...)
	}
	s.mu.RUnlock()
	for i := range hs {
		hs[i](ctx, event)
	}
}

// Subscribe registers a handler to receive events for a topic ("*" for all).
func (s *Stream) Subscribe(topic string, handler StreamHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.handlers[topic] = append(s.handlers[topic], handler)
}

// Unsubscribe removes a handler for a topic.
func (s *Stream) Unsubscribe(topic string, handler StreamHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hs := s.handlers[topic]
	for i, h := range hs {
		if reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer() {
			s.handlers[topic] = append(hs[:i], hs[i+1:]...)
			break
		}
	}
}

// Project subscribes the projection to this stream (all events).
func (s *Stream) Project(proj Projection) {
	logger := golly.NewLogger().WithField("stream", s.name)
	h := func(ctx context.Context, evt Event) {
		defer func() {
			if r := recover(); r != nil {
				logger.Dup().Errorf("Recovered from panic in projection %s: %v", projectionKey(proj), r)
			}
		}()
		if err := proj.HandleEvent(ctx, evt); err != nil {
			logger.Dup().Errorf("cannot process projection %s (%s)", projectionKey(proj), err)
		}
	}
	logger.Tracef("registering projection %s stream=%s", projectionKey(proj), s.name)
	s.Subscribe(AllEvents, h)
}

func streamName(cfg *StreamOptions) string {
	if cfg == nil {
		return DefaultStreamName
	}
	return cfg.Name
}
