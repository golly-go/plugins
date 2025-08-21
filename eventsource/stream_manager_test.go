package eventsource

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
)

// Test for getStreams
func TestGetStreams(t *testing.T) {
	sm := NewStreamManager()
	sm.Add(NewStream(StreamOptions{Name: "stream1", NumPartitions: 1}))
	sm.Add(NewStream(StreamOptions{Name: "stream2", NumPartitions: 1}))

	result := sm.getStreams()
	assert.Equal(t, 2, len(result))
}

func TestStreamManager_AddAndPublish(t *testing.T) {
	sm := NewStreamManager()
	inmem := NewStream(StreamOptions{Name: DefaultStreamName, NumPartitions: 1, BufferSize: 128})
	sm.Add(inmem)

	var calls int32
	ok := sm.Subscribe("*", func(ctx context.Context, evt Event) { atomic.AddInt32(&calls, 1) })
	assert.True(t, ok, "expected to subscribe on in-memory stream")

	sm.Start()
	defer sm.Stop()

	events := []Event{{Type: "A"}, {Type: "B"}, {Type: "C"}}
	for i := range events {
		// Engine.Send would set topic; we mimic here
		sm.Publish(golly.NewContext(nil), events[i].Type, events[i])
	}

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
}

type lifeStream struct{ started, stopped int32 }

func (l *lifeStream) Publish(ctx context.Context, topic string, evt any) error { return nil }
func (l *lifeStream) Start()                                                   { atomic.AddInt32(&l.started, 1) }
func (l *lifeStream) Stop()                                                    { atomic.AddInt32(&l.stopped, 1) }

func TestStreamManager_StartStopLifecycle(t *testing.T) {
	sm := NewStreamManager()
	ls := &lifeStream{}
	sm.Add(ls)

	sm.Start()
	sm.Stop()

	assert.Equal(t, int32(1), atomic.LoadInt32(&ls.started))
	assert.Equal(t, int32(1), atomic.LoadInt32(&ls.stopped))
}
