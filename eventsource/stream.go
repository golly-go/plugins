package eventsource

import (
	"context"
	"errors"
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

var ErrQueueDraining = errors.New("queue is draining and not accepting new events")

type Job struct {
	Ctx   context.Context
	Event Event
}

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
// This is now just a wrapper around InternalStream for backward compatibility.
type Stream struct {
	*InternalStream
}

// NewStream initializes a new Stream with options
func NewStream(opts StreamOptions) *Stream {
	return &Stream{
		InternalStream: NewInternalStream(opts.Name),
	}
}

// InternalStream is a simple stream for projections and in-memory subscriptions
// No complex ordering - just process events as they arrive
type InternalStream struct {
	name     string
	mu       sync.RWMutex
	handlers map[string][]StreamHandler
	jobs     chan Job
	stop     chan struct{}
	stopped  bool
	wg       sync.WaitGroup
}

// NewInternalStream creates a simple internal stream for projections
func NewInternalStream(name string) *InternalStream {
	return &InternalStream{
		name:     name,
		handlers: make(map[string][]StreamHandler),
		jobs:     make(chan Job, 1000),
		stop:     make(chan struct{}),
	}
}

// Name returns the stream name
func (s *InternalStream) Name() string { return s.name }

// Publish enqueues an event for immediate processing
func (s *InternalStream) Publish(ctx context.Context, topic string, evt any) error {
	e := evt.(Event)

	golly.Logger().Tracef("publish event to internal stream %s topic=%s", s.name, topic)

	// Use background context for async processing to prevent cancellation
	// when the originating request completes. Events should be self-contained.
	// We don't check ctx.Done() because events cannot be lost.
	bgCtx := context.Background()

	select {
	case s.jobs <- Job{Ctx: bgCtx, Event: e}:
		return nil
	default:
		return ErrQueueDraining
	}
}

// Subscribe registers a handler for a topic
func (s *InternalStream) Subscribe(topic string, handler StreamHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.handlers[topic] = append(s.handlers[topic], handler)
}

// Unsubscribe removes a handler for a topic
func (s *InternalStream) Unsubscribe(topic string, handler StreamHandler) {
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

// Start begins processing events
func (s *InternalStream) Start() {
	golly.Logger().Tracef("starting internal stream %s", s.name)
	s.wg.Add(1)
	go s.run()
}

// Stop stops processing events
func (s *InternalStream) Stop() {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.mu.Unlock()

	close(s.stop)
	s.wg.Wait() // Wait for the goroutine to finish draining
}

// run processes events in a single goroutine
func (s *InternalStream) run() {
	defer s.wg.Done()
	for {
		select {
		case job := <-s.jobs:
			s.handleEvent(job.Ctx, job.Event)
		case <-s.stop:
			// Drain remaining events
			for {
				select {
				case job := <-s.jobs:
					s.handleEvent(job.Ctx, job.Event)
				default:
					return
				}
			}
		}
	}
}

// handleEvent processes a single event
func (s *InternalStream) handleEvent(ctx context.Context, event Event) {
	s.mu.RLock()
	hs := make([]StreamHandler, 0, 8)

	if h, ok := s.handlers[event.Topic]; ok {
		hs = append(hs, h...)
		golly.Logger().Tracef("Found %d handlers for topic %s", len(h), event.Topic)
	}

	if h, ok := s.handlers[AllEvents]; ok {
		hs = append(hs, h...)
		golly.Logger().Tracef("Found %d handlers for AllEvents (*)", len(h))
	}
	s.mu.RUnlock()

	golly.Logger().Tracef("Total handlers for event %s: %d", event.Topic, len(hs))
	for i := range hs {
		hs[i](ctx, event)
	}
}
