package eventsource

import (
	"context"
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
	// InternalStream doesn't have partitions anymore - it's simple
}

func TestStream_Subscribe(t *testing.T) {
	s := NewStream(StreamOptions{Name: "subTest"})

	handler := func(ctx context.Context, evt Event) {}

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

	handler := func(ctx context.Context, evt Event) {}
	anotherHandler := func(ctx context.Context, evt Event) {}

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
	handlerA := func(ctx context.Context, evt Event) { atomic.AddInt32(&handlerCalls, 1) }

	stream.Subscribe("SpecialEvent", handlerA)
	stream.Subscribe(AllEvents, handlerA)

	events := []Event{
		{Type: "SpecialEvent", AggregateID: "MyAggregate", Version: 1},
		{Type: "SpecialEvent", AggregateID: "MyAggregate", Version: 2},
		{Type: "SomethingElse", AggregateID: "MyAggregate", Version: 1},
	}

	for _, evt := range events {
		// Publish with topic == evt.Type
		topic := evt.Type
		assert.NoError(t, stream.Publish(golly.NewContext(nil), topic, evt))
	}

	time.Sleep(100 * time.Millisecond) // Allow time for processing

	assert.GreaterOrEqual(t, atomic.LoadInt32(&handlerCalls), int32(3),
		"expected at least 3 calls, got %d", handlerCalls)
}

func TestStream_ConcurrentSend(t *testing.T) {
	s := NewStream(StreamOptions{})

	var counter int32

	handler := func(ctx context.Context, evt Event) { atomic.AddInt32(&counter, 1) }

	s.Subscribe("TestEvent", handler)

	// Start the stream before sending events
	s.Start()

	// Send events concurrently
	var wg sync.WaitGroup
	for i := 1; i <= 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = s.Publish(context.Background(), "TestEvent", Event{Type: "TestEvent", AggregateID: "MyAggregate", Version: int64(i), Topic: "TestEvent"})
		}(i)
	}
	wg.Wait()
	s.Stop()

	assert.Equal(t, int32(100), atomic.LoadInt32(&counter))
}
