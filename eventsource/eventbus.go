package eventsource

import "context"

// Bus is a pluggable publisher for events. Consumers are managed separately.
// Topic semantics are backend-defined; by default we use event type as the topic.
type Bus interface {
	Publish(ctx context.Context, topic string, payload []byte) error
}

// BusRouter maps an event to a topic name for Publish when using the Bus.
// Default: event.Type, falling back to the Go type name of event.Data if Type is empty.
type BusRouter func(Event) string
