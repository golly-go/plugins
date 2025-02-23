package eventsource

import (
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
				// Possibly set a larger blockedTimeout if you're skipping in the main code
				// BlockedTimeout: 5 * time.Second,
			})

			var mu sync.Mutex
			processed := make(map[string][]Event)

			// Subscribe to "TestEvent" and collect them
			stream.Subscribe("TestEvent", func(ctx *golly.Context, evt Event) {
				mu.Lock()
				processed[evt.AggregateID] = append(processed[evt.AggregateID], evt)
				mu.Unlock()
			})

			stream.Start()
			defer stream.Stop()

			// Create aggregate IDs
			aggregates := make([]string, tt.numAggregates)
			for i := 0; i < tt.numAggregates; i++ {
				aggregates[i] = fmt.Sprintf("agg%d", i)
			}

			var wg sync.WaitGroup
			// Enqueue events concurrently
			for _, aggID := range aggregates {
				for i := 1; i <= tt.eventsPerAggregate; i++ {
					wg.Add(1)
					go func(aggID string, version int) {

						defer wg.Done()
						evt := Event{
							ID:          uuid.New(),
							Type:        "TestEvent",
							AggregateID: aggID,
							Version:     int64(version),
						}
						err := stream.Send(golly.NewContext(nil), evt) // or Enqueue
						assert.NoError(t, err)
					}(aggID, i)
				}
			}

			// Wait for all sends to finish
			wg.Wait()

			// Give the stream time to reorder/process everything
			time.Sleep(1 * time.Second)

			mu.Lock()
			defer mu.Unlock()

			// Check that each aggregate received all events in ascending GlobalVersion
			for _, aggID := range aggregates {
				events := processed[aggID]
				assert.Len(t, events, tt.eventsPerAggregate,
					"Should have exactly %d events for %s", tt.eventsPerAggregate, aggID)

				for i := 0; i < len(events)-1; i++ {
					if events[i].Version >= events[i+1].Version {
						t.Errorf("Events for aggregate %s are out of order at position %d", aggID, i)
					}
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

			var processed sync.Map
			var count int32

			stream.Subscribe("TestEvent", func(ctx *golly.Context, evt Event) {
				processed.Store(evt.Data.(int), true)
				atomic.AddInt32(&count, 1)
			})

			stream.Start()

			id, _ := uuid.NewV7()
			for i := 1; i <= tt.numEvents; i++ {
				err := stream.Send(golly.NewContext(nil), Event{
					AggregateID: id.String(),
					Type:        "TestEvent",
					Version:     int64(i),
					Data:        i,
				})
				assert.NoError(t, err)
			}

			time.Sleep(100 * time.Millisecond)
			stream.Stop()

			assert.Equal(t, tt.expectedCount, atomic.LoadInt32(&count),
				"All events should be processed during drain")

			err := stream.Send(golly.NewContext(nil), Event{
				Type: "TestEvent",
				Data: tt.numEvents + 1,
			})
			assert.ErrorIs(t, err, ErrQueueDraining)
		})
	}
}

func BenchmarkStreamQueue_StartStop(b *testing.B) {

	cfg := StreamQueueConfig{
		NumPartitions:  4,
		BufferSize:     1000,
		BlockedTimeout: 3 * time.Second,
		Handler:        func(ctx *golly.Context, evt Event) {},
	}

	b.Run("StartStop", func(b *testing.B) {

		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			stream := NewStreamQueue(cfg)

			stream.Start()
			stream.Stop()
		}
	})
}
