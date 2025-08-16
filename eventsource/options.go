package eventsource

import "time"

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
	Bus   Bus
	Store EventStore

	Stream *StreamOptions
}

// WithBus configures the Engine to use the provided Bus for pub/sub
func WithBus(bus Bus) Option {
	return func(o *Options) {
		o.Bus = bus
	}
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

func handleOptions(opts ...Option) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	return options
}
