package eventsource

import (
	"context"
	"sync"

	"github.com/golly-go/golly"
)

// StreamManager manages streams and coordinates dispatch.
type StreamManager struct {
	mu      sync.RWMutex
	streams []StreamPublisher
}

// NewStreamManager initializes a new StreamManager.
func NewStreamManager() *StreamManager { return &StreamManager{} }

// Add registers streams for publish fanout.
func (sm *StreamManager) Add(streams ...StreamPublisher) {
	sm.mu.Lock()
	sm.streams = append(sm.streams, streams...)
	sm.mu.Unlock()
}

// Publish publishes events to all streams.
func (sm *StreamManager) Publish(ctx context.Context, topic string, events ...Event) {
	sm.mu.RLock()
	streams := append([]StreamPublisher(nil), sm.streams...)
	sm.mu.RUnlock()

	if len(streams) == 0 || len(events) == 0 {
		return
	}

	// Use background context for publishing to prevent cancellation
	// when the originating request completes. Events should be self-contained.
	// Note: We don't use WithTimeout here because many publishers (like Kafka)
	// are async and the timeout would fire before they complete.
	bgCtx := golly.ToGollyContext(ctx).Detach()

	for i := range streams {
		for j := range events {
			_ = streams[i].Publish(bgCtx, topic, events[j])
		}
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
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	cp := make([]StreamPublisher, len(sm.streams))
	copy(cp, sm.streams)
	return cp
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
