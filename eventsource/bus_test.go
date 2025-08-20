package eventsource

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSyncBus(t *testing.T) {
	bus := NewSyncBus()
	err := bus.Publish(context.Background(), "t", Event{Type: "E"})
	assert.NoError(t, err)
}
