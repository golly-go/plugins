package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/segmentio/encoding/json"
	"github.com/twmb/franz-go/pkg/kgo"
)

type benchPayload struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// newOfflineClient returns a client whose seed broker is unreachable, so
// Produce only buffers.
func newOfflineClient(tb testing.TB) *kgo.Client {
	tb.Helper()
	c, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:1"),
		kgo.MaxBufferedRecords(1<<30),
		kgo.ProduceRequestTimeout(time.Hour),
	)
	if err != nil {
		tb.Fatalf("new client: %v", err)
	}
	tb.Cleanup(c.Close)
	return c
}

func nopPromise(*kgo.Record, error) {}

func benchKeyFunc(_ string, _ any) []byte { return benchKey }

var (
	benchKey = []byte("k")
	benchVal = &benchPayload{ID: 1, Name: "x"}
)

func publishCases() []struct {
	name string
	cfg  Config
} {
	return []struct {
		name string
		cfg  Config
	}{
		{"plain", Config{}},
		{"prefix", Config{TopicPrefix: "dev"}},
		{"prefix_keyfunc", Config{TopicPrefix: "dev", KeyFunc: benchKeyFunc}},
	}
}

func BenchmarkProducerPublish(b *testing.B) {
	for _, tc := range publishCases() {
		b.Run(tc.name, func(b *testing.B) {
			p := NewProducer(newOfflineClient(b), tc.cfg)
			ctx := context.Background()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := p.Publish(ctx, "orders", benchVal); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestProducerPublishAllocs(t *testing.T) {
	ctx := context.Background()

	// Floor: encode + record + kgo's own Produce cost. Publish may add nothing.
	floor := func(c *kgo.Client) float64 {
		return testing.AllocsPerRun(200, func() {
			v, err := json.Marshal(benchVal)
			if err != nil {
				t.Fatal(err)
			}
			c.Produce(ctx, &kgo.Record{Topic: "orders", Value: v}, nopPromise)
		})
	}

	for _, tc := range publishCases() {
		t.Run(tc.name, func(t *testing.T) {
			p := NewProducer(newOfflineClient(t), tc.cfg)
			if err := p.Publish(ctx, "orders", benchVal); err != nil { // warm topic cache
				t.Fatal(err)
			}
			got := testing.AllocsPerRun(200, func() {
				if err := p.Publish(ctx, "orders", benchVal); err != nil {
					t.Fatal(err)
				}
			})
			if want := floor(newOfflineClient(t)); got > want {
				t.Fatalf("Publish allocs = %v, want <= %v (encode+record+kgo)", got, want)
			}
		})
	}
}

func newBenchHandle(prefix string, c Consumer) *consumerHandle {
	return &consumerHandle{consumer: c, config: Config{TopicPrefix: prefix}}
}

type nopConsumer struct{ n int }

func (c *nopConsumer) Handler(_ context.Context, m Message) error {
	c.n += len(m.Value)
	return nil
}
func (c *nopConsumer) SubscribeOptions() SubscribeOptions { return SubscribeOptions{} }

func benchRecords() map[string]*kgo.Record {
	return map[string]*kgo.Record{
		"noheaders": {Topic: "dev-orders", Key: benchKey, Value: []byte("{}"), Timestamp: time.Unix(1, 0)},
		"headers": {Topic: "dev-orders", Key: benchKey, Value: []byte("{}"), Timestamp: time.Unix(1, 0),
			Headers: []kgo.RecordHeader{{Key: "a", Value: []byte("1")}, {Key: "b", Value: []byte("2")}}},
	}
}

func BenchmarkProcessRecord(b *testing.B) {
	for name, rec := range benchRecords() {
		for _, prefix := range []string{"", "dev"} {
			b.Run(name+"/prefix="+prefix, func(b *testing.B) {
				h := newBenchHandle(prefix, &nopConsumer{})
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := h.processRecord(ctx, rec, nil); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func TestProcessRecordAllocs(t *testing.T) {
	for name, rec := range benchRecords() {
		for _, prefix := range []string{"", "dev"} {
			t.Run(name+"/prefix="+prefix, func(t *testing.T) {
				h := newBenchHandle(prefix, &nopConsumer{})
				ctx := context.Background()
				got := testing.AllocsPerRun(200, func() {
					if err := h.processRecord(ctx, rec, nil); err != nil {
						t.Fatal(err)
					}
				})
				if got > 0 {
					t.Fatalf("processRecord allocs = %v, want 0", got)
				}
			})
		}
	}
}
