package eventsource

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func runBusBehaviorTests(t *testing.T, name string, newBus func() Bus) {
	t.Run(name+"_SubscribePublish", func(t *testing.T) {
		bus := newBus()
		bus.Start()
		t.Cleanup(bus.Stop)

		var mu sync.Mutex
		var seen []string
		h1 := func(ctx context.Context, evt Event) error {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, "h1:"+evt.Type)
			return nil
		}
		h2 := func(ctx context.Context, evt Event) error {
			mu.Lock()
			defer mu.Unlock()
			seen = append(seen, "h2:"+evt.Type)
			return nil
		}

		bus.Subscribe("topicA", h1)
		bus.Subscribe("topicA", h2)

		err := bus.Publish(context.Background(), "topicA", Event{Type: "E1"})
		assert.NoError(t, err)

		mu.Lock()
		got := append([]string(nil), seen...)
		mu.Unlock()
		assert.ElementsMatch(t, []string{"h1:E1", "h2:E1"}, got)
	})

	t.Run(name+"_Unsubscribe", func(t *testing.T) {
		bus := newBus()
		h1Count, h2Count := 0, 0
		h1 := func(ctx context.Context, evt Event) error { h1Count++; return nil }
		h2 := func(ctx context.Context, evt Event) error { h2Count++; return nil }

		bus.Subscribe("t", h1)
		bus.Subscribe("t", h2)
		bus.Unsubscribe("t", h1)

		_ = bus.Publish(context.Background(), "t", Event{Type: "E"})
		assert.Equal(t, 0, h1Count)
		assert.Equal(t, 1, h2Count)
	})

	t.Run(name+"_HandlerErrorIgnored", func(t *testing.T) {
		bus := newBus()
		bus.Subscribe("t", func(ctx context.Context, evt Event) error { return assert.AnError })
		err := bus.Publish(context.Background(), "t", Event{Type: "E"})
		assert.NoError(t, err)
	})
}

func TestSyncBus(t *testing.T) {
	runBusBehaviorTests(t, "SyncBus", func() Bus { return NewSyncBus() })
}

func TestInMemoryBus(t *testing.T) {
	runBusBehaviorTests(t, "InMemoryBus", func() Bus { return NewInMemoryBus() })
}
