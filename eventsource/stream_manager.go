package eventsource

import (
	"sync"
)

var (
	streamManager = NewStreamManager()
)

// StreamManager manages multiple streams and coordinates dispatch.
type StreamManager struct {
	mu      sync.RWMutex
	streams map[string]*Stream
}

// NewStreamManager initializes a new StreamManager.
func NewStreamManager() *StreamManager {
	return &StreamManager{
		streams: make(map[string]*Stream),
	}
}

// Get retrieves a stream if it exists.
func (sm *StreamManager) Get(name string) (*Stream, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	s, ok := sm.streams[name]
	return s, ok
}

func (sm *StreamManager) Register(name string) *Stream {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	stream := NewStream(name)

	sm.streams[name] = stream

	return stream
}

// SendTo sends an event to a specific named stream.
func (sm *StreamManager) SendTo(streamName string, event Event) {
	sm.mu.RLock()
	stream, ok := sm.streams[streamName]
	sm.mu.RUnlock()

	if ok {
		stream.Send(event)
	}
}

// Send sends an event to all streams.
func (sm *StreamManager) Send(events ...Event) {
	streams := sm.getStreams()

	for _, s := range streams {
		go s.Send(events...)
	}
}

// getStreams returns a copy of the streams slice.
func (sm *StreamManager) getStreams() []*Stream {
	sm.mu.RLock()
	copyOfStreams := make([]*Stream, 0, len(sm.streams))
	for _, s := range sm.streams {
		copyOfStreams = append(copyOfStreams, s)
	}
	sm.mu.RUnlock()
	return copyOfStreams
}
