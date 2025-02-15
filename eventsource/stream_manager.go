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

	s, ok := sm.streams[name]
	return s, ok
}

func (sm *StreamManager) RegisterStream(s *Stream) *Stream {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, exists := sm.streams[s.name]; exists {
		return s
	}

	sm.streams[s.name] = s
	return s
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

	if !opts.Create {
		return nil, errors.New("stream does not exist and create=false")
	}

	if opts.NumPartitions == 0 {
		opts.NumPartitions = defaultPartitions
	}

	if opts.BufferSize == 0 {
		opts.BufferSize = defaultQueueSize
	}

	stream := sm.RegisterStream(NewStream(opts))
	return stream, nil
}

func (sm *StreamManager) RegisterProjection(streamName string, autoCreate bool, proj Projection) error {

	if streamName == "" {
		return errors.New("stream name cannot be empty")
	}

	stream, err := sm.GetOrCreateStream(StreamOptions{Name: streamName, Create: autoCreate})
	if err != nil {
		return err
	}

	stream.Project(proj)
	return nil
}
