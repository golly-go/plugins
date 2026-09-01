package eventsource

import (
	"time"
)

// Option is a function that configures a projection registration or other engine-related setup.
type Option func(*Options)

type StreamOptions struct {
	Name           string
	NumPartitions  uint32
	BufferSize     int
	BlockedTimeout time.Duration
}

// Options holds all possible configuration parameters that can be adjusted via Option functions.
type Options struct {
	Store      EventStore
	Stream     *StreamOptions
	Streams    []StreamPublisher
	MaxRetries int // minimum 10; defaults to 10 if unset or below minimum

	// ProjectionWorkers/ProjectionBufferSize override the projection manager's
	// worker count / total job buffer for this engine. 0 means "use the
	// package-level DefaultProjectionWorkers/DefaultProjectionBufferSize".
	ProjectionWorkers    int
	ProjectionBufferSize int
}

// WithStore configures the Engine to use the provided EventStore
func WithStore(store EventStore) Option {
	return func(o *Options) {
		o.Store = store
	}
}

// deprecated: bus defined options
func WithStreamBlockedTimeout(timeout time.Duration) Option {
	return func(o *Options) {}
}

// deprecated: bus defined options
func WithStreamName(name string) Option {
	return func(o *Options) {}
}

// deprecated: bus defined options
func WithStreamPartitions(n uint32) Option {
	return func(o *Options) {}
}

// deprecated: bus defined options
func WithStreamBufferSize(size int) Option {
	return func(o *Options) {}
}

func WithStreams(streams ...StreamPublisher) Option {
	return func(o *Options) {
		o.Streams = streams
	}
}

// WithMaxRetries sets the maximum number of version-conflict retries in Execute.
// The minimum enforced value is 10.
func WithMaxRetries(n int) Option {
	return func(o *Options) {
		o.MaxRetries = n
	}
}

// WithProjectionWorkers overrides the number of hash-partitioned projection
// worker goroutines for this engine. If unset (or n < 1), the engine falls
// back to DefaultProjectionWorkers.
func WithProjectionWorkers(n int) Option {
	return func(o *Options) {
		o.ProjectionWorkers = n
	}
}

// WithProjectionBufferSize overrides the total job-channel buffer distributed
// across projection workers for this engine. If unset (or n < 1), the engine
// falls back to DefaultProjectionBufferSize.
func WithProjectionBufferSize(n int) Option {
	return func(o *Options) {
		o.ProjectionBufferSize = n
	}
}

func handleOptions(opts ...Option) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}
	if options.MaxRetries < 10 {
		options.MaxRetries = 10
	}
	return options
}
