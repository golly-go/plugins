package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/golly-go/golly"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
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
	topics  []string
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

// ApplyOptions merges the provided options into the existing consumer configuration.
func (b *Consumers) ApplyOptions(opts ...Option) {
	for _, o := range opts {
		o(&b.cfg)
	}
}

func (b *Consumers) Start() error {
	if !b.started.CompareAndSwap(false, true) {
		return nil
	}

	b.ctx, b.cancel = context.WithCancel(context.Background())
	if b.cfg.WriterFunc != nil {
		b.writer = b.cfg.WriterFunc()
		goto start
	}

	if len(b.cfg.Brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured; set kafka.brokers in config or configure the plugin")
	}

	b.writer = b.newWriter()

start:
	// snapshot current subs and start each
	b.mu.RLock()
	for _, list := range b.subs {
		for _, sub := range list {
			b.startSubLocked(sub)
		}
	}
	b.mu.RUnlock()
	return nil
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

// Subscribe requires topics and groupID; when groupID is omitted we derive a readable, stable value from the handler path and topics.
func (b *Consumers) Subscribe(handler Handler, opts ...SubOption) error {
	sc := SubConfig{}
	for _, o := range opts {
		o(&sc)
	}

	topics := sanitizeTopics(sc.Topics)
	if len(topics) == 0 {
		return fmt.Errorf("kafka: subscribe requires at least one topic")
	}

	if sc.GroupID == "" && len(topics) > 1 {
		return fmt.Errorf("kafka: subscribe requires a groupID to have multiple topics")
	}

	trace("kafka: subscribing to %#v", sc)

	b.mu.Lock()
	sub := &subscription{topics: topics, groupID: sc.GroupID, handler: handler}
	for _, t := range topics {
		b.subs[t] = append(b.subs[t], sub)
	}

	started := b.started.Load()
	b.mu.Unlock()

	if started {
		b.startSubLocked(sub)
	}
	return nil
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
			// remove this subscription from all topic indices
			for _, t := range list[i].topics {
				cur := b.subs[t]
				for j := range cur {
					if cur[j] == list[i] {
						b.subs[t] = append(cur[:j], cur[j+1:]...)
						break
					}
				}
			}
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
	defer func() {
		if r := recover(); r != nil {
			golly.Logger().Errorf("kafka: consumer panic recovered: %v", r)
		}
		trace("Kafka %s consumer stopped", strings.Join(sub.topics, ","))
	}()

	var reader readerIface
	if b.cfg.ReaderFunc == nil && len(b.cfg.Brokers) == 0 {
		golly.Logger().Errorf("kafka: consumers: no brokers configured; cannot start reader for %s; set kafka.brokers or configure plugin", strings.Join(sub.topics, ","))
		return
	}

	if b.cfg.ReaderFunc != nil {
		reader = b.cfg.ReaderFunc(sub.topics, sub.groupID)
	} else {
		reader = b.newReader(sub.topics, sub.groupID)
	}
	defer reader.Close()

	golly.Logger().Debugf("[KAFKA]: starting consumer for %s", strings.Join(sub.topics, ","))

	var backoff time.Duration = 200 * time.Millisecond

	handler := func(sub *subscription, m kafka.Message) (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("kafka: handler panic recovered: %v", r)
			}
		}()

		if err := sub.handler(sub.ctx, m.Value); err != nil {
			// do not commit on failure; leave message for retry. Consider DLQ here.
			return err
		}
		return nil
	}

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
			jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
			delay := backoff/2 + jitter

			trace("kafka %#v fetch message error: %v, retrying in %s (%#v)", sub.topics, err, delay, m)

			time.Sleep(delay)
			continue
		}

		backoff = 200 * time.Millisecond

		err = handler(sub, m)
		if err != nil {
			golly.Logger().Errorf("kafka: handler error on %s: %v", m.Topic, err)
			continue
		}

		if sub.groupID == "" {
			continue
		}

		if err := reader.CommitMessages(sub.ctx, m); err != nil {
			golly.Logger().Warnf("kafka: commit failed on %s: %v", m.Topic, err)
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

func (b *Consumers) newReader(topics []string, groupID string) readerIface {
	dialer := &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true, KeepAlive: 15 * time.Second, ClientID: b.cfg.ClientID}
	if b.cfg.UserName != "" && b.cfg.Password != "" {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		dialer.SASLMechanism = plain.Mechanism{Username: b.cfg.UserName, Password: b.cfg.Password}
	}

	var gtopics []string

	if groupID != "" {
		groupID = sanitizeGroupID(groupID)
		gtopics = topics
	}

	var topic string
	var balancers []kafka.GroupBalancer
	if groupID == "" {
		topic = topics[0]
	}

	if groupID != "" {
		balancers = []kafka.GroupBalancer{kafka.RangeGroupBalancer{}}
	}

	rc := kafka.ReaderConfig{
		WatchPartitionChanges:  true,
		PartitionWatchInterval: 10 * time.Second,
		Brokers:                b.cfg.Brokers,
		GroupID:                groupID,
		GroupTopics:            gtopics,
		Topic:                  topic,
		MinBytes:               max(1, b.cfg.ReadMinBytes),
		MaxBytes:               max(1, b.cfg.ReadMaxBytes),
		MaxWait:                b.cfg.ReadMaxWait,
		Dialer:                 dialer,
		JoinGroupBackoff:       5 * time.Second,
		ReadBackoffMin:         250 * time.Millisecond,
		ReadBackoffMax:         10 * time.Second,
		ReadBatchTimeout:       max(b.cfg.ReadMaxWait, 1*time.Second),
		GroupBalancers:         balancers,
		QueueCapacity:          100,
		MaxAttempts:            5,
		// Logger: &KafkaLogger{Logger: golly.Logger().WithFields(logrus.Fields{
		// 	"component": "kafka",
		// 	"consumer":  "consumer",
		// 	"topics":    strings.Join(topics, ","),
		// 	"groupID":   groupID,
		// })},
	}

	if groupID != "" {
		rc.StartOffset = kafka.FirstOffset
		if b.cfg.StartFromLatest {
			rc.StartOffset = kafka.LastOffset
		}
	}

	// manual commit: keep default CommitInterval (1s) only if user explicitly requests auto-commit
	if b.cfg.CommitInterval > 0 {
		// rc.CommitInterval = b.cfg.CommitInterval
	}
	return kafka.NewReader(rc)
}

// sanitizeGroupID keeps only Kafka-safe characters and compresses whitespace
func sanitizeGroupID(s string) string {
	b := strings.Builder{}
	b.Grow(len(s))
	lastDash := false
	for _, r := range s {
		if r == '/' || r == ':' || r == '.' || r == '-' || r == '_' || r == '@' || r == '+' {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		// normalize others to single dash
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	res := b.String()
	res = strings.Trim(res, "-")
	if res == "" {
		return "consumer"
	}
	if len(res) > 255 {
		return res[:255]
	}
	return res
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func trace(message string, args ...interface{}) {
	golly.Logger().Tracef("[KAFKA] "+message, args...)
}

func deriveGroupID(topics []string, handler Handler) string {
	// default derivation: function name path + topics (sanitized)
	ptr := reflect.ValueOf(handler).Pointer()
	fn := runtime.FuncForPC(ptr)

	if fn == nil {
		return ""
	}

	return sanitizeGroupID(fn.Name())
}

type KafkaLogger struct {
	Logger *logrus.Entry
}

func (l *KafkaLogger) Printf(message string, args ...interface{}) {
	l.Logger.Tracef("[KAFKA] "+message, args...)
}
