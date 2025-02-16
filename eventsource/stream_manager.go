package eventsource

import (
	"errors"
	"sync"

	"github.com/golly-go/golly"
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
	stream, exists := sm.streams[name]
	return stream, exists
}

func (sm *StreamManager) RegisterStream(stream *Stream) *Stream {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.streams[stream.Name()] = stream
	return stream
}

// SendTo sends an event to a specific named stream.
func (sm *StreamManager) SendTo(gctx *golly.Context, streamName string, event Event) {
	sm.mu.RLock()
	stream, ok := sm.streams[streamName]
	sm.mu.RUnlock()

	if ok {
		stream.Send(gctx, event)
	}
}

// Send sends an event to all streams.
func (sm *StreamManager) Send(gctx *golly.Context, events ...Event) {
	streams := sm.getStreams()
	for _, s := range streams {
		s.Send(gctx, events...)
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

func (sm *StreamManager) GetOrCreateStream(opts StreamOptions) (*Stream, error) {
	sm.mu.RLock()
	if opts.Name == "" {
		sm.mu.RUnlock()
		return nil, errors.New("stream name cannot be empty")
	}

	if stream, exists := sm.Get(opts.Name); exists {
		sm.mu.RUnlock()
		return stream, nil
	}
	sm.mu.RUnlock()

	if opts.NumPartitions == 0 {
		opts.NumPartitions = defaultPartitions
	}

	if opts.BufferSize == 0 {
		opts.BufferSize = defaultQueueSize
	}

	return sm.RegisterStream(NewStream(opts)), nil
}

func (sm *StreamManager) RegisterProjection(streamName string, proj Projection) error {
	if streamName == "" {
		return errors.New("stream name cannot be empty")
	}

	stream, err := sm.GetOrCreateStream(StreamOptions{Name: streamName})
	if err != nil {
		return err
	}

	stream.Project(proj)
	return nil
}
