package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

type fakeReader struct {
	msgs      []kafka.Message
	fetchIdx  int
	committed int
	mu        sync.Mutex
	ctx       context.Context
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	default:
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fetchIdx >= len(r.msgs) {
		return kafka.Message{}, context.Canceled
	}
	m := r.msgs[r.fetchIdx]
	r.fetchIdx++
	return m, nil
}

func (r *fakeReader) CommitMessages(ctx context.Context, _ ...kafka.Message) error {
	r.mu.Lock()
	r.committed++
	r.mu.Unlock()
	return nil
}
func (r *fakeReader) Close() error { return nil }

type fakeWriter struct {
	mu   sync.Mutex
	sent []kafka.Message
}

func (w *fakeWriter) WriteMessages(_ context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.sent = append(w.sent, msgs...)
	return nil
}
func (w *fakeWriter) Close() error { return nil }

func TestConsumers_Subscribe_Publish_SuccessCommits(t *testing.T) {
	fr := &fakeReader{msgs: []kafka.Message{{Topic: "t", Value: []byte(`{"eventType":"E"}`)}}, ctx: context.Background()}
	fw := &fakeWriter{}

	c := NewConsumers(
		WithBrokers([]string{"test"}),
		WithReaderFunc(func(topics []string, groupID string) readerIface { return fr }),
		WithWriterFunc(func() writerIface { return fw }),
	)

	done := make(chan struct{})
	c.Subscribe(func(ctx context.Context, payload []byte) error { close(done); return nil }, SubscribeWithTopic("t"))
	c.Start()
	defer c.Stop()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("handler not invoked")
	}

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 1, fr.committed, "should commit on success")
}

func TestConsumers_Subscribe_HandlerErrorNoCommit(t *testing.T) {
	fr := &fakeReader{msgs: []kafka.Message{{Topic: "t", Value: []byte(`{"eventType":"E"}`)}}, ctx: context.Background()}
	fw := &fakeWriter{}

	c := NewConsumers(
		WithBrokers([]string{"test"}),
		WithReaderFunc(func(topics []string, groupID string) readerIface { return fr }),
		WithWriterFunc(func() writerIface { return fw }),
	)

	done := make(chan struct{})
	c.Subscribe(func(ctx context.Context, payload []byte) error { close(done); return assert.AnError }, SubscribeWithTopic("t"))
	c.Start()
	defer c.Stop()

	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatalf("handler not invoked")
	}

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, 0, fr.committed, "must not commit on handler error")
}

func TestConsumers_PublishBeforeStartErrors(t *testing.T) {
	c := NewConsumers()
	err := c.Publish(context.Background(), "t", []byte("x"))
	assert.Error(t, err)
}

func TestConsumers_UnsubscribeStopsHandler(t *testing.T) {
	// two independent readers (one per sub)
	fr1 := &fakeReader{msgs: []kafka.Message{
		{Topic: "t", Value: []byte(`E1`)},
	}, ctx: context.Background()}
	fr2 := &fakeReader{msgs: []kafka.Message{
		{Topic: "t", Value: []byte(`E2`)},
		{Topic: "t", Value: []byte(`E2b`)},
	}, ctx: context.Background()}

	ri := 0
	c := NewConsumers(
		WithBrokers([]string{"test"}),
		WithReaderFunc(func(topics []string, groupID string) readerIface {
			if ri == 0 {
				ri++
				return fr1
			}
			return fr2
		}),
		WithWriterFunc(func() writerIface { return &fakeWriter{} }),
	)

	h1 := func(ctx context.Context, payload []byte) error { return nil }
	h2 := func(ctx context.Context, payload []byte) error { return nil }
	c.Subscribe(h1, SubscribeWithTopic("t"))
	c.Subscribe(h2, SubscribeWithTopic("t"))
	c.Start()
	defer c.Stop()

	// Allow first messages to be processed, then unsubscribe h1 so second message isn't committed by h1
	time.Sleep(10 * time.Millisecond)
	c.Unsubscribe("t", h1)
	time.Sleep(20 * time.Millisecond)

	assert.LessOrEqual(t, fr1.committed, 1, "after unsubscribe, first reader should not commit additional messages")
	assert.GreaterOrEqual(t, fr2.committed, 1, "second reader continues committing")
}

func TestConsumers_HandlerPanic_NoCommit(t *testing.T) {
	fr := &fakeReader{msgs: []kafka.Message{{Topic: "t", Value: []byte(`E`)}}, ctx: context.Background()}
	c := NewConsumers(
		WithBrokers([]string{"test"}),
		WithReaderFunc(func(topics []string, groupID string) readerIface { return fr }),
		WithWriterFunc(func() writerIface { return &fakeWriter{} }),
	)

	c.Subscribe(func(ctx context.Context, payload []byte) error { panic("boom") }, SubscribeWithTopic("t"))
	c.Start()
	defer c.Stop()

	time.Sleep(20 * time.Millisecond)
	// Panic recovered by wrapper; success=false, so no commit
	assert.Equal(t, 0, fr.committed)
}

func TestConsumers_Publish_UsesKeyFunc(t *testing.T) {
	fw := &fakeWriter{}
	c := NewConsumers(
		WithKeyFunc(func(topic string, payload any) []byte { return []byte("key-") }),
		WithWriterFunc(func() writerIface { return fw }),
	)

	c.Start()
	defer c.Stop()
	_ = c.Publish(context.Background(), "topic-x", []byte("payload"))
	time.Sleep(5 * time.Millisecond)

	fw.mu.Lock()
	defer fw.mu.Unlock()
	if assert.Len(t, fw.sent, 1) {
		assert.Equal(t, "topic-x", fw.sent[0].Topic)
		assert.Equal(t, []byte("key-"), fw.sent[0].Key)
	}
}

type errWriter struct{}

func (w *errWriter) WriteMessages(context.Context, ...kafka.Message) error {
	return errors.New("write failed")
}
func (w *errWriter) Close() error { return nil }

func TestConsumers_Publish_WriterError(t *testing.T) {
	c := NewConsumers(
		WithBrokers([]string{"test"}),
		WithWriterFunc(func() writerIface { return &errWriter{} }),
	)
	c.Start()
	defer c.Stop()

	err := c.Publish(context.Background(), "t", []byte("x"))
	assert.Error(t, err)
}

func BenchmarkConsumers_Publish(b *testing.B) {
	fw := &fakeWriter{}
	c := NewConsumers(
		WithBrokers([]string{"bench"}),
		WithWriterFunc(func() writerIface { return fw }),
	)
	c.Start()
	defer c.Stop()

	ctx := context.Background()
	payload := []byte("payload")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Publish(ctx, "bench", payload)
	}
}
