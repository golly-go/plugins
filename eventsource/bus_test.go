package eventsource

import (
	"context"
	"testing"

	"github.com/segmentio/encoding/json"
	"github.com/stretchr/testify/assert"
)

func TestSyncBus(t *testing.T) {
	bus := NewSyncBus()
	b, _ := json.Marshal(Event{Type: "E"})
	err := bus.Publish(context.Background(), "t", b)
	assert.NoError(t, err)
}
