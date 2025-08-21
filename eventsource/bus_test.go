package eventsource

import (
	"context"
	"testing"

	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/assert"
)

type noopBus struct{}

func (n *noopBus) Publish(ctx context.Context, topic string, payload []byte) error { return nil }

func TestBus_PublishBytes(t *testing.T) {
	bus := &noopBus{}
	b, _ := json.Marshal(Event{Type: "E"})
	err := bus.Publish(context.Background(), "t", b)
	assert.NoError(t, err)
}
