package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeBroker is a single-partition log with one committed offset. Each client
// it hands out starts at the committed offset, like a group rejoin.
type fakeBroker struct {
	mu        sync.Mutex
	log       []*kgo.Record
	committed int64 // next offset to consume
	commits   []int64
	clients   int
	open      atomic.Int32
}

func newFakeBroker(n int) *fakeBroker {
	b := &fakeBroker{}
	for i := 0; i < n; i++ {
		b.log = append(b.log, &kgo.Record{Topic: "orders", Partition: 0, Offset: int64(i), Value: []byte{byte(i)}})
	}
	return b
}

func (b *fakeBroker) newClient() (consumerClient, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.clients++
	b.open.Add(1)
	return &fakeClient{b: b, pos: b.committed}, nil
}

func (b *fakeBroker) committedOffset() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.committed
}

type fakeClient struct {
	b      *fakeBroker
	pos    int64
	closed atomic.Bool
}

// PollFetches returns everything from pos onwards, or blocks until ctx is done.
func (c *fakeClient) PollFetches(ctx context.Context) kgo.Fetches {
	c.b.mu.Lock()
	recs := c.b.log[min(c.pos, int64(len(c.b.log))):]
	c.b.mu.Unlock()
	if len(recs) == 0 {
		<-ctx.Done()
		return kgo.NewErrFetch(ctx.Err())
	}
	c.pos += int64(len(recs))
	return kgo.Fetches{{Topics: []kgo.FetchTopic{{Topic: "orders", Partitions: []kgo.FetchPartition{{Partition: 0, Records: recs}}}}}}
}

func (c *fakeClient) CommitRecords(_ context.Context, rs ...*kgo.Record) error {
	c.b.mu.Lock()
	defer c.b.mu.Unlock()
	for _, r := range rs {
		if r.Offset+1 > c.b.committed {
			c.b.committed = r.Offset + 1
		}
	}
	c.b.commits = append(c.b.commits, c.b.committed)
	return nil
}

func (c *fakeClient) Close() {
	if c.closed.CompareAndSwap(false, true) {
		c.b.open.Add(-1)
	}
}

// failingConsumer fails each offset in failures that many times.
type failingConsumer struct {
	mu       sync.Mutex
	failures map[int64]int
	seen     []int64
}

func (c *failingConsumer) Handler(_ context.Context, m Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seen = append(c.seen, m.Offset)
	if c.failures[m.Offset] > 0 {
		c.failures[m.Offset]--
		return errors.New("boom")
	}
	return nil
}

func (c *failingConsumer) SubscribeOptions() SubscribeOptions {
	return SubscribeOptions{GroupID: "g"}
}

func (c *failingConsumer) offsets() []int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int64(nil), c.seen...)
}

// startFakeHandle starts a handle on broker through a real ConsumerManager so
// Unsubscribe/Stop exercise the production shutdown path.
func startFakeHandle(t *testing.T, broker *fakeBroker, c Consumer, backoff func(int) time.Duration) (*ConsumerManager, *Subscription) {
	t.Helper()
	cm := NewConsumerManager(Config{})
	cm.ctx, cm.cancel = context.WithCancel(context.Background())
	cm.running.Store(true)
	t.Cleanup(func() { _ = cm.Stop() })

	h := &consumerHandle{
		consumer:  c,
		opts:      c.SubscribeOptions(),
		topics:    []string{"orders"},
		topicStr:  "orders",
		groupID:   c.SubscribeOptions().GroupID,
		newClient: broker.newClient,
		backoff:   backoff,
		done:      make(chan struct{}),
	}
	cm.consumers["fake"] = h
	require.NoError(t, cm.startConsumer("fake", h))
	return cm, &Subscription{id: "fake", cm: cm}
}

func TestHandlerErrorRestartsAndRedelivers(t *testing.T) {
	broker := newFakeBroker(4)
	c := &failingConsumer{failures: map[int64]int{2: 2}}

	var attempts []int
	var amu sync.Mutex
	backoff := func(retries int) time.Duration {
		amu.Lock()
		attempts = append(attempts, retries)
		amu.Unlock()
		return time.Millisecond
	}

	_, sub := startFakeHandle(t, broker, c, backoff)

	require.Eventually(t, func() bool { return broker.committedOffset() == 4 }, 5*time.Second, time.Millisecond)
	require.NoError(t, sub.Stop())

	// 0,1 ok then 2 fails (commit 0..1), restart: 2 fails again, restart: 2,3 ok.
	assert.Equal(t, []int64{0, 1, 2, 2, 2, 3}, c.offsets())
	broker.mu.Lock()
	assert.Equal(t, []int64{2, 4}, broker.commits, "failed record must never be committed")
	assert.Equal(t, 3, broker.clients, "one initial client plus one per restart")
	broker.mu.Unlock()

	// The first restart follows progress (0,1 handled) so uses retries=0; the
	// second made no progress, so it backs off further.
	amu.Lock()
	assert.Equal(t, []int{0, 1}, attempts)
	amu.Unlock()

	assert.Zero(t, broker.open.Load(), "every client must be closed")
}

func TestHandlerErrorBackoffResetsAfterSuccess(t *testing.T) {
	broker := newFakeBroker(4)
	c := &failingConsumer{failures: map[int64]int{1: 2, 3: 2}}

	var attempts []int
	var amu sync.Mutex
	_, sub := startFakeHandle(t, broker, c, func(retries int) time.Duration {
		amu.Lock()
		attempts = append(attempts, retries)
		amu.Unlock()
		return time.Millisecond
	})

	require.Eventually(t, func() bool { return broker.committedOffset() == 4 }, 5*time.Second, time.Millisecond)
	require.NoError(t, sub.Stop())

	// [0 ok,1 fail] -> 0; [1 fail] -> 1; [1,2 ok,3 fail] resets -> 0; [3 fail] -> 1; [3 ok].
	amu.Lock()
	assert.Equal(t, []int{0, 1, 0, 1}, attempts)
	amu.Unlock()
	broker.mu.Lock()
	assert.Equal(t, []int64{1, 3, 4}, broker.commits)
	broker.mu.Unlock()
}

func TestCalculateBackoffGrowsAndCaps(t *testing.T) {
	h := &consumerHandle{}
	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
	for i, w := range want {
		assert.Equal(t, w, h.calculateBackoff(i), "retries=%d", i)
	}
	// Used to overflow 1<<retries into a zero/negative wait.
	for _, r := range []int{34, 63, 64, 1000} {
		assert.Equal(t, maxRetryBackoff, h.calculateBackoff(r), "retries=%d", r)
	}
	assert.Equal(t, baseRetryBackoff, h.retryBackoff(0))
}

func TestCancelDuringRestartBackoffExitsPromptly(t *testing.T) {
	broker := newFakeBroker(1)
	c := &failingConsumer{failures: map[int64]int{0: 1 << 30}}
	inBackoff := make(chan struct{}, 1)
	cm, _ := startFakeHandle(t, broker, c, func(int) time.Duration {
		select {
		case inBackoff <- struct{}{}:
		default:
		}
		return time.Hour
	})

	select {
	case <-inBackoff:
	case <-time.After(5 * time.Second):
		t.Fatal("handler error did not reach backoff")
	}

	start := time.Now()
	require.NoError(t, cm.Stop()) // cancels the manager context, waits for run
	assert.Less(t, time.Since(start), time.Second)
	assert.Zero(t, broker.open.Load(), "client leaked")
	assert.Equal(t, 1, broker.clients, "no restart while cancelled")
	assert.EqualValues(t, 0, broker.committedOffset())
}

func TestUnsubscribeDuringRestartBackoffExitsPromptly(t *testing.T) {
	broker := newFakeBroker(1)
	c := &failingConsumer{failures: map[int64]int{0: 1 << 30}}
	inBackoff := make(chan struct{}, 1)
	_, sub := startFakeHandle(t, broker, c, func(int) time.Duration {
		select {
		case inBackoff <- struct{}{}:
		default:
		}
		return time.Hour
	})
	<-inBackoff

	start := time.Now()
	require.NoError(t, sub.Stop())
	assert.Less(t, time.Since(start), time.Second)
	assert.Zero(t, broker.open.Load(), "client leaked")
}
