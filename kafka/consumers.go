package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"golang.org/x/exp/constraints"
)

// minimal interfaces for testability
type readerIface interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

type writerIface interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

// Handler processes a raw message payload
// Return error to avoid committing the message

type Handler func(ctx context.Context, payload []byte) error

// Consumers manages per-subscription consumers and a shared producer.
// Generic: no dependency on eventsource types.

type Consumers struct {
	cfg Config

	writer writerIface

	mu   sync.RWMutex
	subs map[string][]*subscription // topic -> list of subs

	started atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

type subscription struct {
	topic   string
	groupID string
	handler Handler
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewConsumers(opts ...Option) *Consumers {
	cfg := Config{
		ReadMinBytes:   1 << 10,
		ReadMaxBytes:   1 << 20,
		ReadMaxWait:    250 * time.Millisecond,
		CommitInterval: 0, // manual commit
		WriteTimeout:   5 * time.Second,
		AllowAutoTopic: true,
	}
	for _, o := range opts {
		o(&cfg)
	}
	c := &Consumers{
		cfg:  cfg,
		subs: make(map[string][]*subscription),
	}
	return c
}

func (b *Consumers) Start() {
	if !b.started.CompareAndSwap(false, true) {
		return
	}
	b.ctx, b.cancel = context.WithCancel(context.Background())
	if b.cfg.WriterFunc != nil {
		b.writer = b.cfg.WriterFunc()
	} else {
		b.writer = b.newWriter()
	}

	// snapshot current subs and start each
	b.mu.RLock()
	for _, list := range b.subs {
		for _, sub := range list {
			b.startSubLocked(sub)
		}
	}
	b.mu.RUnlock()
}

func (b *Consumers) Stop() {
	if !b.started.CompareAndSwap(true, false) {
		return
	}
	if b.cancel != nil {
		b.cancel()
	}

	b.mu.RLock()
	for _, list := range b.subs {
		for _, sub := range list {
			if sub.cancel != nil {
				sub.cancel()
			}
		}
	}
	b.mu.RUnlock()

	// Wait for all readers to exit via context cancellation
	b.wg.Wait()

	if b.writer != nil {
		_ = b.writer.Close()
	}
}

func (b *Consumers) Subscribe(topic string, handler Handler) {
	b.mu.Lock()
	sub := &subscription{topic: topic, groupID: b.deriveGroupID(topic, handler), handler: handler}
	b.subs[topic] = append(b.subs[topic], sub)
	started := b.started.Load()
	b.mu.Unlock()

	if started {
		b.startSubLocked(sub)
	}
}

func (b *Consumers) Unsubscribe(topic string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	list := b.subs[topic]
	for i := range list {
		if reflect.ValueOf(list[i].handler).Pointer() == reflect.ValueOf(handler).Pointer() {
			if list[i].cancel != nil {
				list[i].cancel()
			}
			b.subs[topic] = append(list[:i], list[i+1:]...)
			break
		}
	}
}

func (b *Consumers) Publish(ctx context.Context, topic string, payload []byte) error {
	if b.writer == nil {
		return fmt.Errorf("kafka consumers: writer not initialized (call Start first)")
	}
	var key []byte
	if b.cfg.KeyFunc != nil {
		key = b.cfg.KeyFunc(topic, payload)
	}
	msg := kafka.Message{Topic: topic, Key: key, Value: payload}
	return b.writer.WriteMessages(ctx, msg)
}

func (b *Consumers) startSubLocked(sub *subscription) {
	if sub.ctx != nil {
		return // already started
	}
	sub.ctx, sub.cancel = context.WithCancel(b.ctx)
	b.wg.Add(1)
	go b.runReader(sub)
}

func (b *Consumers) runReader(sub *subscription) {
	defer b.wg.Done()
	var reader readerIface
	if b.cfg.ReaderFunc != nil {
		reader = b.cfg.ReaderFunc(sub.topic, sub.groupID)
	} else {
		reader = b.newReader(sub.topic, sub.groupID)
	}
	defer reader.Close()

	var backoff time.Duration = 200 * time.Millisecond

	for {
		select {
		case <-sub.ctx.Done():
			return
		default:
		}
		m, err := reader.FetchMessage(sub.ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			// exponential backoff with cap
			backoff = time.Duration(math.Min(float64(backoff*2), float64(5*time.Second)))
			time.Sleep(backoff)
			continue
		}
		backoff = 200 * time.Millisecond

		success := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					golly.Logger().Errorf("kafka: handler panic recovered: %v", r)
				}
			}()
			if err := sub.handler(sub.ctx, m.Value); err != nil {
				// do not commit on failure; leave message for retry. Consider DLQ here.
				golly.Logger().Warnf("kafka: handler error on %s: %v", sub.topic, err)
				return
			}
			success = true
		}()

		if !success {
			continue
		}

		if err := reader.CommitMessages(sub.ctx, m); err != nil {
			golly.Logger().Warnf("kafka: commit failed on %s: %v", sub.topic, err)
		}
	}
}

func (b *Consumers) newWriter() writerIface {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(b.cfg.Brokers...),
		Balancer:               &kafka.Murmur2Balancer{},
		AllowAutoTopicCreation: b.cfg.AllowAutoTopic,
		Async:                  false,
		WriteTimeout:           b.cfg.WriteTimeout,
		RequiredAcks:           kafka.RequireAll,
	}
	if b.cfg.UserName != "" && b.cfg.Password != "" {
		w.Transport = &kafka.Transport{
			DialTimeout: 20 * time.Second,
			IdleTimeout: 45 * time.Second,
			TLS:         &tls.Config{MinVersion: tls.VersionTLS12},
			SASL: plain.Mechanism{
				Username: b.cfg.UserName,
				Password: b.cfg.Password,
			},
		}
	}
	return w
}

func (b *Consumers) newReader(topic, groupID string) readerIface {
	dialer := &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true, KeepAlive: 15 * time.Second, ClientID: b.cfg.ClientID}
	if b.cfg.UserName != "" && b.cfg.Password != "" {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		dialer.SASLMechanism = plain.Mechanism{Username: b.cfg.UserName, Password: b.cfg.Password}
	}
	balancer := kafka.RangeGroupBalancer{}
	rc := kafka.ReaderConfig{
		Brokers:               b.cfg.Brokers,
		GroupID:               groupID,
		GroupTopics:           []string{topic},
		MinBytes:              max(1, b.cfg.ReadMinBytes),
		MaxBytes:              max(1, b.cfg.ReadMaxBytes),
		MaxWait:               b.cfg.ReadMaxWait,
		Dialer:                dialer,
		WatchPartitionChanges: true,
		JoinGroupBackoff:      5 * time.Second,
		ReadBackoffMin:        250 * time.Millisecond,
		ReadBackoffMax:        10 * time.Second,
		ReadBatchTimeout:      max(b.cfg.ReadMaxWait, 1*time.Second),
		GroupBalancers:        []kafka.GroupBalancer{balancer},
		StartOffset:           kafka.FirstOffset,
	}
	if b.cfg.StartFromLatest {
		rc.StartOffset = kafka.LastOffset
	}
	// manual commit: keep default CommitInterval (1s) only if user explicitly requests auto-commit
	if b.cfg.CommitInterval > 0 {
		rc.CommitInterval = b.cfg.CommitInterval
	}
	return kafka.NewReader(rc)
}

func (b *Consumers) deriveGroupID(topic string, handler Handler) string {
	base := b.cfg.GroupID
	if base == "" {
		base = "eventsource"
	}
	ptr := reflect.ValueOf(handler).Pointer()
	return fmt.Sprintf("%s:%s:%x", base, topic, ptr)
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}
