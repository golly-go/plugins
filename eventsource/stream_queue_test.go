package eventsource

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStreamQueue_Ordering(t *testing.T) {
	tests := []struct {
		name               string
		numAggregates      int
		eventsPerAggregate int
	}{
		{"SingleAggregate", 1, 100},
		{"MultipleAggregates", 3, 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			aggregates := make([]string, tt.numAggregates)
			for i := 0; i < tt.numAggregates; i++ {
				aggregates[i] = fmt.Sprintf("agg%d", i)
			}

			var wg sync.WaitGroup
			for _, aggID := range aggregates {
				for i := 0; i < tt.eventsPerAggregate; i++ {
					wg.Add(1)
					go func(aggID string, version int) {
						defer wg.Done()
						evt := Event{
							ID:          uuid.New(),
							Type:        "TestEvent",
							AggregateID: aggID,
							Version:     int64(version),
						}
						err := stream.Send(ctx, evt)
						assert.NoError(t, err)
					}(aggID, i)
				}
			}

			wg.Wait()
			time.Sleep(100 * time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			for _, aggID := range aggregates {
				events := processed[aggID]
				assert.Len(t, events, tt.eventsPerAggregate)

				for i := 0; i < len(events)-1; i++ {
					assert.Less(t, events[i].Version, events[i+1].Version,
						"Events for aggregate %s are out of order at position %d", aggID, i)
				}

				firstPartition := events[0].partitionID
				for _, evt := range events {
					assert.Equal(t, firstPartition, evt.partitionID,
						"Events for same aggregate should go to same partition")
				}
			}
		})
	}
}

func TestStreamQueue_Draining(t *testing.T) {
	tests := []struct {
		name          string
		numEvents     int
		expectedCount int32
	}{
		{"Drain100Events", 100, 100},
		{"Drain50Events", 50, 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			for i := 0; i < tt.numEvents; i++ {
				err := stream.Send(ctx, Event{
					Type: "TestEvent",
					Data: i,
				})
				assert.NoError(t, err)
			}

			time.Sleep(10 * time.Millisecond)

			stream.Stop()

			assert.Equal(t, tt.expectedCount, atomic.LoadInt32(&count),
				"All events should be processed during drain")

			err := stream.Send(ctx, Event{
				Type: "TestEvent",
				Data: tt.numEvents + 1,
			})
			assert.ErrorIs(t, err, ErrQueueDraining)
		})
	}
}
