package eventsource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type captureBus struct {
	Bus
	subscribed map[string]int
	published  []string
}

func newCaptureBus() *captureBus {
	return &captureBus{subscribed: make(map[string]int)}
}

func (c *captureBus) Start()                              {}
func (c *captureBus) Stop()                               {}
func (c *captureBus) Subscribe(topic string, h Handler)   { c.subscribed[topic]++ }
func (c *captureBus) Unsubscribe(topic string, h Handler) {}
func (c *captureBus) Publish(ctx context.Context, topic string, evt Event) error {
	c.published = append(c.published, topic+":"+evt.Type)
	return nil
}

func TestEngine_WithOptions_WiresStoreAndBus(t *testing.T) {
	store := NewInMemoryStore()
	bus := newCaptureBus()

	eng := NewEngine(WithStore(store), WithBus(bus))
	assert.Equal(t, store, eng.Store())

	err := eng.Subscribe("a-topic", func(ctx context.Context, evt Event) error { return nil })
	assert.NoError(t, err)
	assert.Equal(t, 1, bus.subscribed["a-topic"]) // subscription went to bus
}

func TestEngine_Send_PublishesToBusWithEventType(t *testing.T) {
	bus := newCaptureBus()
	eng := NewEngine(WithStore(NewInMemoryStore()), WithBus(bus))

	eng.Send(context.Background(), Event{Type: "X"}, Event{Data: struct{}{}}, Event{Type: "Y"})

	// We expect topics to be evt.Type if provided; for the middle event, topic falls back to Go type name
	assert.Contains(t, bus.published, "X:X")
	assert.Contains(t, bus.published, "Y:Y")
	// The fallback topic ends with the type name, assert at least one element contains ':' separator
	foundFallback := false
	for _, p := range bus.published {
		if p != "X:X" && p != "Y:Y" {
			foundFallback = true
			break
		}
	}
	assert.True(t, foundFallback)
}
