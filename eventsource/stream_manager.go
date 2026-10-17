package eventsource

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/golly-go/golly"
)

// StreamManager manages streams and coordinates dispatch.
type StreamManager struct {
	mu      sync.Mutex // Used only for Add (copy-on-write)
	streams atomic.Pointer[[]StreamPublisher]
}

// NewStreamManager initializes a new StreamManager.
func NewStreamManager() *StreamManager {
	sm := &StreamManager{}
	sm.streams.Store(&[]StreamPublisher{})
	return sm
}

// Add registers streams for publish fanout.
func (sm *StreamManager) Add(streams ...StreamPublisher) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	current := *sm.streams.Load()
	newStreams := make([]StreamPublisher, len(current)+len(streams))
	copy(newStreams, current)
	copy(newStreams[len(current):], streams)

	sm.streams.Store(&newStreams)
}

// Publish publishes events to all streams.
func (sm *StreamManager) Publish(ctx context.Context, topic string, events ...Event) {
	if !sm.has() || len(events) == 0 {
		return
	}

	// Use background context for publishing to prevent cancellation
	// when the originating request completes. Events should be self-contained.
	// Note: We don't use WithTimeout here because many publishers (like Kafka)
	// are async and the timeout would fire before they complete.
	bgCtx := golly.ToGollyContext(ctx).Detach()

	if len(events) == 1 {
		sm.publishDetached(bgCtx, topic, &events[0])
		return
	}

	// box each event once, then fan out stream-major (each stream sees events in order)
	boxed := make([]any, len(events))
	for j := range events {
		boxed[j] = events[j]
	}

	streams := *sm.streams.Load()
	for i := range streams {
		for j := range boxed {
			publishOne(streams[i], bgCtx, topic, boxed[j], &events[j])
		}
	}
}

func publishOne(st StreamPublisher, ctx context.Context, topic string, boxed any, event *Event) {
	if err := st.Publish(ctx, topic, boxed); err != nil {
		golly.DefaultLogger().WithError(err).Errorf("stream publish failed topic=%s eventType=%s aggregateID=%s", topic, event.Type, event.AggregateID)
	}
}

// has reports whether any stream is registered.
func (sm *StreamManager) has() bool { return len(*sm.streams.Load()) > 0 }

// publishDetached fans one event out to every stream with an already-detached ctx.
// The event is boxed once for all streams. A nil ctx means the caller saw no streams.
func (sm *StreamManager) publishDetached(detached context.Context, topic string, event *Event) {
	streams := *sm.streams.Load()
	if len(streams) == 0 || detached == nil {
		return
	}

	boxed := any(*event)
	for i := range streams {
		publishOne(streams[i], detached, topic, boxed, event)
	}
}

// Subscribe subscribes the handler to the first subscribable stream.
func (sm *StreamManager) Subscribe(topic string, handler StreamHandler) bool {
	streams := sm.getStreams()
	if len(streams) == 0 {
		return false
	}

	for i := range streams {
		// Fall back to complex Stream
		if sub, ok := streams[i].(*Stream); ok {
			sub.Subscribe(topic, handler)
			return true
		}
	}

	return false
}

// Start starts streams that implement lifecycle.
func (sm *StreamManager) Start() {
	streams := sm.getStreams()
	for i := range streams {
		if lc, ok := streams[i].(StreamLifecycle); ok {
			lc.Start()
		} else {
			trace("Skiping start on %s", resolveInterfaceName(streams[i]))
		}
	}
}

func (sm *StreamManager) getStreams() []StreamPublisher {
	return *sm.streams.Load()
}

// Stop stops streams that implement lifecycle.
func (sm *StreamManager) Stop() {
	streams := sm.getStreams()
	for i := range streams {
		if lc, ok := streams[i].(StreamLifecycle); ok {
			lc.Stop()
		}
	}
}
