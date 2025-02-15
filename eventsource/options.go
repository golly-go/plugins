package eventsource

// Option is a function that configures a projection registration or other engine-related setup.
type Option func(*Options)

type StreamOptions struct {
	Name          string
	Create        bool
	NumPartitions uint32
	BufferSize    int
}

// Options holds all possible configuration parameters that can be adjusted via Option functions.
type Options struct {
	Stream *StreamOptions
}

// WithStream specifies a stream name for the projection/event registration.
func WithStream(name string, create bool, partitions uint) Option {
	return func(o *Options) {
		if o.Stream == nil {
			o.Stream = &StreamOptions{}
		}
		o.Stream.NumPartitions = uint32(partitions)
		o.Stream.Name = name
		o.Stream.Create = create
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
