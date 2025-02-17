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

// Send sends an event to all streams.
func (sm *StreamManager) Send(ctx *golly.Context, events ...Event) {
	streams := sm.getStreams()

	for pos := range streams {
		streams[pos].Send(ctx, events...)
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
	if opts.Name == "" {
		return nil, errors.New("stream name cannot be empty")
	}

	sm.mu.RLock()
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

func (sm *StreamManager) RegisterProjection(proj Projection, opts ...Option) error {

	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.Stream == nil {
		options.Stream = &defaultStreamOptions
	}

	golly.Logger().Tracef("Registering projection for stream: %s\n", options.Stream.Name)

	stream, err := sm.GetOrCreateStream(*options.Stream)
	if err != nil {
		return err
	}

	stream.Project(proj)
	return nil
}

func (sm *StreamManager) Start() {
	for _, s := range sm.streams {
		s.Start()
	}
}

func (sm *StreamManager) Stop() {
	for _, s := range sm.streams {
		s.Stop()
	}
}
