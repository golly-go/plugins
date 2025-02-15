package eventsource

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStreamQueue_Ordering(t *testing.T) {
	stream := NewStream(StreamOptions{
		Name:          "test",
		NumPartitions: 4,
		BufferSize:    1000,
	})
	ctx := golly.NewContext(context.Background())

	var mu sync.Mutex
	processed := make(map[string][]Event)

	stream.Subscribe("TestEvent", func(ctx *golly.Context, evt Event) {
		mu.Lock()
		processed[evt.AggregateID] = append(processed[evt.AggregateID], evt)
		mu.Unlock()
	})

	stream.Start()
	defer stream.Stop()

	// Test with multiple aggregates
	aggregates := []string{"agg1", "agg2", "agg3"}
	eventsPerAggregate := 100

	// Send events with increasing versions per aggregate
	var wg sync.WaitGroup
	for _, aggID := range aggregates {
		for i := 0; i < eventsPerAggregate; i++ {
			wg.Add(1)
			go func(aggID string, version int) {
				defer wg.Done()
				evt := Event{
					ID:          uuid.New(),
					Type:        "TestEvent",
					AggregateID: aggID,
					Version:     int64(version), // Use version to track order
				}
				err := stream.Send(ctx, evt)
				assert.NoError(t, err)
			}(aggID, i)
		}
	}

	wg.Wait()
	time.Sleep(100 * time.Millisecond) // Allow processing to complete

	mu.Lock()
	defer mu.Unlock()

	// Verify ordering for each aggregate
	for _, aggID := range aggregates {
		events := processed[aggID]
		assert.Len(t, events, eventsPerAggregate)

		// Events should be in version order
		for i := 0; i < len(events)-1; i++ {
			assert.Less(t, events[i].Version, events[i+1].Version,
				"Events for aggregate %s are out of order at position %d", aggID, i)
		}

		// Verify all events went to same partition
		firstPartition := events[0].partitionID
		for _, evt := range events {
			assert.Equal(t, firstPartition, evt.partitionID,
				"Events for same aggregate should go to same partition")
		}
	}
}

func TestStreamQueue_Draining(t *testing.T) {
	stream := NewStream(StreamOptions{
		Name:          "test",
		NumPartitions: 4,
		BufferSize:    100,
	})
	ctx := golly.NewContext(context.Background())

	var processed sync.Map
	var count int32

	stream.Subscribe("TestEvent", func(ctx *golly.Context, evt Event) {
		processed.Store(evt.Data.(int), true)
		atomic.AddInt32(&count, 1)
	})

	stream.Start()

	// Send some events
	for i := 0; i < 100; i++ {
		err := stream.Send(ctx, Event{
			Type: "TestEvent",
			Data: i,
		})
		assert.NoError(t, err)
	}

	// Allow some processing time
	time.Sleep(10 * time.Millisecond)

	// Initiate shutdown
	stream.Stop()

	// Verify all events were processed
	assert.Equal(t, int32(100), atomic.LoadInt32(&count),
		"All events should be processed during drain")

	// Verify new events are rejected
	err := stream.Send(ctx, Event{
		Type: "TestEvent",
		Data: 101,
	})
	assert.ErrorIs(t, err, ErrQueueDraining)
}
