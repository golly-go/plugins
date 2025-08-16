package eventsource

import "context"

type Handler func(ctx context.Context, evt Event) error

// Bus is a pluggable event bus that supports multiple handlers per topic and external backends (e.g., Kafka).
// Topic semantics are backend-defined; by default we use event type as the topic.
type Bus interface {
	Start()
	Stop()
	Subscribe(topic string, handler Handler)
	Unsubscribe(topic string, handler Handler)
	Publish(ctx context.Context, topic string, evt Event) error
}

// BusRouter maps an event to a topic name for Publish when using the Bus.
// Default: event.Type, falling back to the Go type name of event.Data if Type is empty.
type BusRouter func(Event) string
