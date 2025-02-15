package eventsource

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	s := NewStream(StreamOptions{Name: "sendTest"})

	var mu sync.Mutex
	calledEvents := make([]string, 0)

	handlerA := func(ctx *golly.Context, evt Event) {
		mu.Lock()
		defer mu.Unlock()
		calledEvents = append(calledEvents, "handlerA-"+evt.Type)
	}

	handlerB := func(ctx *golly.Context, evt Event) {
		mu.Lock()
		defer mu.Unlock()
		calledEvents = append(calledEvents, "handlerB-"+evt.Type)
	}

	// Subscribe to a specific event type
	s.Subscribe("SpecialEvent", handlerA)

	// Subscribe to an aggregate
	s.Aggregate("MyAggregate", handlerB)

	// Subscribe to all events
	s.Subscribe(AllEvents, func(ctx *golly.Context, evt Event) {
		mu.Lock()
		defer mu.Unlock()
		calledEvents = append(calledEvents, "AllEvents-"+evt.Type)
	})

	ctx := golly.NewContext(context.Background())

	// Start the stream before sending events
	s.Start()
	defer s.Stop()

	// Add small delay to ensure handlers are ready
	time.Sleep(10 * time.Millisecond)

	// Send events
	err := s.Send(ctx,
		Event{AggregateType: "MyAggregate", Type: "SpecialEvent"},
		Event{AggregateType: "OtherAggregate", Type: "SomethingElse"},
	)
	require.NoError(t, err)

	// Allow time for processing
	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	assert.GreaterOrEqual(t, len(calledEvents), 3,
		"expected at least 3 calls, got %d: %v", len(calledEvents), calledEvents)

	var hasHandlerA bool
	var hasHandlerB bool
	var hasAllEventsSE bool
	var hasAllEventsElse bool

	for _, ce := range calledEvents {
		switch ce {
		case "handlerA-SpecialEvent":
			hasHandlerA = true
		case "handlerB-SpecialEvent":
			hasHandlerB = true
		case "AllEvents-SpecialEvent":
			hasAllEventsSE = true
		case "AllEvents-SomethingElse":
			hasAllEventsElse = true
		}
	}

	if !hasHandlerA {
		t.Error("handlerA not triggered for SpecialEvent")
	}
	if !hasHandlerB {
		t.Error("handlerB not triggered for MyAggregate + SpecialEvent")
	}
	if !hasAllEventsSE {
		t.Error("all-events handler not triggered for SpecialEvent")
	}
	if !hasAllEventsElse {
		t.Error("all-events handler not triggered for SomethingElse")
	}
}

func TestStream_ConcurrentSend(t *testing.T) {
	s := NewStream(StreamOptions{
		Name:          "concurrentTest",
		NumPartitions: 4,
		BufferSize:    1000,
	})
	var counter int32

	handler := func(ctx *golly.Context, evt Event) {
		atomic.AddInt32(&counter, 1)
	}
	s.Subscribe("TestEvent", handler)

	// Start the stream before sending events
	s.Start()
	defer s.Stop()

	// Send events concurrently
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := s.Send(golly.NewContext(context.Background()),
				Event{Type: "TestEvent"})
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Allow time for processing
	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, int32(100), atomic.LoadInt32(&counter))
}

func TestStreamPartitioning(t *testing.T) {
	// Create stream with 16 partitions
	stream := NewStream(StreamOptions{
		Name:          "test",
		NumPartitions: 16,
		BufferSize:    1000,
	})

	ctx := golly.NewContext(context.Background())

	// Track which partition handles each event
	partitionMap := make(map[string]uint32)
	var mu sync.Mutex

	stream.Subscribe("TestEvent", func(ctx *golly.Context, evt Event) {
		mu.Lock()
		partitionMap[evt.ID.String()] = evt.partitionID
		mu.Unlock()
	})

	stream.Start()
	defer stream.Stop()

	// Send same event multiple times - should always go to same partition
	id, _ := uuid.Parse("123e4567-e89b-12d3-a456-426614174000")
	evt := Event{
		ID:   id,
		Type: "TestEvent",
	}

	for i := 0; i < 100; i++ {
		stream.Send(ctx, evt)
	}

	mu.Lock()
	defer mu.Unlock()

	// Verify all instances went to same partition
	firstPartition := partitionMap[evt.ID.String()]
	for _, partition := range partitionMap {
		assert.Equal(t, firstPartition, partition,
			"Same event ID should always map to same partition")
	}
}
