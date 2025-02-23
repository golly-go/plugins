package eventsource

import (
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
)

func TestNewStream(t *testing.T) {
	s := NewStream(StreamOptions{
		Name:          "testStream",
		NumPartitions: 4,
		BufferSize:    100,
	})
	assert.Equal(t, "testStream", s.Name())
	assert.Equal(t, uint32(4), s.queue.numParts)
}

func TestStream_Aggregate(t *testing.T) {
	s := NewStream(StreamOptions{
		Name:          "aggTest",
		NumPartitions: 4,
		BufferSize:    100,
	})

	handler := func(ctx *golly.Context, evt Event) {}

	s.Aggregate("MyAggregate", handler)

	s.mu.RLock()
	defer s.mu.RUnlock()

	agHandlers, ok := s.aggregations["MyAggregate"]
	if !ok || len(agHandlers) == 0 {
		t.Fatal("handler not registered for MyAggregate")
	}

	if reflect.ValueOf(agHandlers[0]).Pointer() != reflect.ValueOf(handler).Pointer() {
		t.Errorf("expected the same handler function pointer")
	}
}

func TestStream_Subscribe(t *testing.T) {
	s := NewStream(StreamOptions{Name: "subTest"})

	handler := func(ctx *golly.Context, evt Event) {}

	s.Subscribe("TestEvent", handler)

	s.mu.RLock()
	defer s.mu.RUnlock()

	evHandlers, ok := s.handlers["TestEvent"]
	if !ok || len(evHandlers) == 0 {
		t.Fatal("handler not registered for event type TestEvent")
	}

	if reflect.ValueOf(evHandlers[0]).Pointer() != reflect.ValueOf(handler).Pointer() {
		t.Errorf("expected the same handler function pointer for TestEvent")
	}
}

func TestStream_Unsubscribe(t *testing.T) {
	s := NewStream(StreamOptions{Name: "unsubTest"})

	handler := func(ctx *golly.Context, evt Event) {}
	anotherHandler := func(ctx *golly.Context, evt Event) {}

	s.Subscribe("TestEvent", handler)
	s.Subscribe("TestEvent", anotherHandler)

	s.Unsubscribe("TestEvent", handler)

	s.mu.RLock()
	defer s.mu.RUnlock()

	// Expect only anotherHandler to remain
	evHandlers := s.handlers["TestEvent"]
	if len(evHandlers) != 1 {
		t.Fatalf("expected 1 handler left, got %d", len(evHandlers))
	}

	if reflect.ValueOf(evHandlers[0]).Pointer() == reflect.ValueOf(handler).Pointer() {
		t.Error("handler was not unsubscribed")
	}
	if reflect.ValueOf(evHandlers[0]).Pointer() != reflect.ValueOf(anotherHandler).Pointer() {
		t.Error("expected anotherHandler to remain subscribed")
	}
}

func TestStream_Send(t *testing.T) {
	stream := NewStream(StreamOptions{
		Name:          "test",
		NumPartitions: 4,
		BufferSize:    1000,
	})

	stream.Start()
	defer stream.Stop()

	var handlerCalls int32
	handlerA := func(ctx *golly.Context, evt Event) {
		atomic.AddInt32(&handlerCalls, 1)
	}

	stream.Subscribe("SpecialEvent", handlerA)
	stream.Subscribe("MyAggregate+SpecialEvent", handlerA)
	stream.Subscribe(AllEvents, handlerA)

	events := []Event{
		{Type: "SpecialEvent", AggregateID: "MyAggregate", Version: 1},
		{Type: "SpecialEvent", AggregateID: "MyAggregate", Version: 2},
		{Type: "SomethingElse", AggregateID: "MyAggregate", Version: 1},
	}

	for _, evt := range events {
		err := stream.Send(golly.NewContext(nil), evt)
		assert.NoError(t, err)
	}

	time.Sleep(100 * time.Millisecond) // Allow time for processing

	assert.GreaterOrEqual(t, atomic.LoadInt32(&handlerCalls), int32(3),
		"expected at least 3 calls, got %d", handlerCalls)
}

func TestStream_ConcurrentSend(t *testing.T) {
	s := NewStream(StreamOptions{})

	var counter int32

	handler := func(ctx *golly.Context, evt Event) {
		atomic.AddInt32(&counter, 1)
	}

	s.Subscribe("TestEvent", handler)

	// Start the stream before sending events
	s.Start()

	// Send events concurrently

	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Send(golly.NewContext(nil), Event{Type: "TestEvent", AggregateID: "MyAggregate", Version: int64(i)})

			assert.NoError(t, err)
		}()
	}
	wg.Wait()
	s.Stop()

	assert.Equal(t, int32(100), atomic.LoadInt32(&counter))
}

// func TestStreamPartitioning(t *testing.T) {
// 	// Create stream with 16 partitions
// 	stream := NewStream(StreamOptions{
// 		Name:          "test",
// 		NumPartitions: 16,
// 		BufferSize:    1000,
// 	})

// 	// Track which partition handles each event
// 	partitionMap := make(map[string]uint32)

// 	var mu sync.Mutex

// 	stream.Subscribe("TestEvent", func(ctx *golly.Context, evt Event) {
// 		mu.Lock()
// 		partitionMap[evt.ID.String()] = evt.partitionID
// 		mu.Unlock()
// 	})

// 	stream.Start()
// 	defer stream.Stop()

// 	// Send same event multiple times - should always go to same partition
// 	id, _ := uuid.Parse("123e4567-e89b-12d3-a456-426614174000")
// 	evt := Event{
// 		ID:   id,
// 		Type: "TestEvent",
// 	}

// 	for i := 0; i < 100; i++ {
// 		stream.Send(golly.NewContext(nil), evt)
// 	}

// 	mu.Lock()
// 	defer mu.Unlock()

// 	// Verify all instances went to same partition
// 	firstPartition := partitionMap[evt.ID.String()]
// 	for _, partition := range partitionMap {
// 		assert.Equal(t, firstPartition, partition,
// 			"Same event ID should always map to same partition")
// 	}
// }
