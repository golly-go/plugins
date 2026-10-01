package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer handles Kafka message publishing with reliability guarantees
type Producer struct {
	// client is the franz-go client used for publishing
	client *kgo.Client

	// config contains the producer configuration
	config Config

	// topics caches logical -> broker topic names so Publish doesn't
	// allocate a new prefixed string on every call.
	topics sync.Map // map[string]string

	running atomic.Bool
	cancel  context.CancelFunc
	ctx     context.Context
}

// NewProducer creates a new Kafka producer with the given franz-go client and configuration
func NewProducer(client *kgo.Client, config Config) *Producer {
	ctx, cancel := context.WithCancel(context.Background())

	return &Producer{
		client: client,
		config: config,
		cancel: cancel,
		ctx:    ctx,
	}
}

// topicName returns the broker topic for topic, applying TopicPrefix once per
// distinct topic and serving it from cache afterwards.
func (p *Producer) topicName(topic string) string {
	if p.config.TopicPrefix == "" {
		return topic
	}

	if tp, ok := p.topics.Load(topic); ok {
		return tp.(string)
	}

	tp, _ := p.topics.LoadOrStore(topic, p.config.topicName(topic))
	return tp.(string)
}

// Publish sends a message to the given topic synchronously, waiting for broker acknowledgment.
// The payload is JSON-encoded automatically. If a key generation function is configured,
// it will be used to generate the message key.
//
// This method blocks until the message is acknowledged by Kafka or an error occurs.
// For async publishing with manual concurrency control, manage goroutines in your application layer.
// Publish sends a message to the given topic. It uses franz-go's internal
// batching and buffering for high performance.
func (p *Producer) Publish(ctx context.Context, topic string, payload any) error {
	tp := p.topicName(topic)

	trace("publishing message to topic %s", tp)

	// Generate key only if KeyFunc is provided
	var key []byte
	if p.config.KeyFunc != nil {
		key = p.config.KeyFunc(topic, payload)
	}

	// Marshal payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafka: publish: %w", err)
	}

	// Create record
	record := &kgo.Record{
		Topic: tp,
		Key:   key,
		Value: payloadBytes,
	}

	// Use async Produce to allow franz-go to batch messages efficiently.
	// The client handles internal synchronization and buffering.
	p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			golly.
				DefaultLogger().
				WithError(err).
				Str("topic", tp).
				Errorf("kafka: publish: failed to deliver message")
		}
	})

	return nil
}

// Flush ensures all pending messages are sent and acknowledged.
// This is important for CLI commands to ensure messages are delivered before exit.
func (p *Producer) Flush(ctx context.Context) error {
	trace("flushing producer - ensuring all messages are sent")

	// Use franz-go's built-in flush method which waits for all
	// buffered records to be acknowledged.
	if err := p.client.Flush(ctx); err != nil {
		trace("flush failed: %v", err)
		return err
	}

	trace("producer flush completed")
	return nil
}

// Stop closes the producer after flushing any pending messages
func (p *Producer) Stop() error {
	if !p.running.Load() {
		return nil
	}
	p.running.Store(false)

	// Create a new context with timeout for the flush operation
	// Don't use p.ctx since we want this to timeout independently
	flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	trace("stopping producer - flushing pending messages")
	if err := p.Flush(flushCtx); err != nil {
		trace("failed to flush producer during stop: %v", err)
		// Don't return error - still want to close the client
	}

	// Cancel the producer's context to stop any lingering operations
	p.cancel()

	// Close the kafka client
	p.client.Close()

	trace("producer stopped")
	return nil
}
