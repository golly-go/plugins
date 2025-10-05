package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

// PublisherAPI is the minimal interface used by application code.
// Returning this interface from GetPublisher allows easy test stubbing.
type PublisherAPI interface {
	Publish(ctx context.Context, topic string, payload any) error
}

// Publisher provides a lightweight Kafka producer.
// It is independent of eventsource types and only deals with raw payloads.
type Publisher struct {
	cfg    Config
	writer writerIface
}

func NewPublisher(cfg Config, opts ...Option) *Publisher {
	for _, o := range opts {
		o(&cfg)
	}
	p := &Publisher{cfg: cfg}
	return p
}

// Start initializes the underlying writer.
func (p *Publisher) Start() error {
	if p.writer != nil {
		return nil
	}
	if len(p.cfg.Brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured; set kafka.brokers in config or configure the plugin")
	}

	if p.cfg.WriterFunc != nil {
		p.writer = p.cfg.WriterFunc()
		return nil
	}

	p.writer = p.newWriter()
	return nil
}

// Stop closes the underlying writer.
func (p *Publisher) Stop() {
	if p.writer != nil {
		_ = p.writer.Close()
		p.writer = nil
	}
}

// Publish sends a message to the given topic.
// Requires Start() to have been called.
func (p *Publisher) Publish(ctx context.Context, topic string, payload any) error {
	if p.writer == nil {
		return fmt.Errorf("kafka publisher: writer not initialized (call Start first)")
	}
	var key []byte = []byte(uuid.New().String())
	if p.cfg.KeyFunc != nil {
		key = p.cfg.KeyFunc(topic, payload)
	}
	topics := sanitizeTopics(topic)
	if len(topics) == 0 {
		return fmt.Errorf("kafka: publish requires a topic")
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafka: publish: %w", err)
	}

	msg := kafka.Message{Topic: topics[0], Key: key, Value: payloadBytes}

	// Retry loop for transient errors (e.g., leader not available)
	var attempt int
	var backoff = p.cfg.PublishBackoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}
	maxRetries := p.cfg.PublishMaxRetries
	for {
		err = p.writer.WriteMessages(ctx, msg)
		if err == nil {
			return nil
		}

		attempt++
		if attempt > maxRetries || ctx.Err() != nil {
			trace("kafka: publish error (giving up): %v", err)
			return err
		}

		// jittered exponential backoff up to 5s
		backoff = time.Duration(math.Min(float64(backoff*2), float64(5*time.Second)))
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		delay := backoff/2 + jitter
		trace("kafka: publish retry %d in %s: %v", attempt, delay, err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *Publisher) newWriter() writerIface {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(p.cfg.Brokers...),
		Balancer:               &kafka.Murmur2Balancer{},
		AllowAutoTopicCreation: p.cfg.AllowAutoTopic,
		Async:                  false,
		WriteTimeout:           p.cfg.WriteTimeout,
		RequiredAcks:           kafka.RequireAll,
	}

	if p.cfg.UserName != "" && p.cfg.Password != "" {
		w.Transport = &kafka.Transport{
			DialTimeout: 20 * time.Second,
			IdleTimeout: 45 * time.Second,
			TLS:         &tls.Config{MinVersion: tls.VersionTLS12},
			SASL: plain.Mechanism{
				Username: p.cfg.UserName,
				Password: p.cfg.Password,
			},
		}
	}
	return w
}

// Compile-time interface checks
var _ PublisherAPI = (*Publisher)(nil)
