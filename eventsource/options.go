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
	Stream *StreamOptions
}

func WithStreamBlockedTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		if o.Stream == nil {
			o.Stream = &StreamOptions{}
		}
		o.Stream.BlockedTimeout = timeout
	}
}

func WithStreamName(name string) Option {
	return func(o *Options) {
		if o.Stream == nil {
			o.Stream = &StreamOptions{}
		}
		o.Stream.Name = name
	}
}

func WithStreamPartitions(n uint32) Option {
	return func(o *Options) {
		if o.Stream == nil {
			o.Stream = &StreamOptions{}
		}
		o.Stream.NumPartitions = n
	}
}

func WithStreamBufferSize(size int) Option {
	return func(o *Options) {
		if o.Stream == nil {
			o.Stream = &StreamOptions{}
		}
		o.Stream.BufferSize = size
	}
}

func handleOptions(opts ...Option) *Options {
	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.Stream == nil {
		options.Stream = &defaultStreamOptions
	}

	return options
}
