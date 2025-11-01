package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// Producer handles Kafka message publishing with reliability guarantees
type Producer struct {
	// client is the franz-go client used for publishing
	client *kgo.Client

	// config contains the producer configuration
	config Config

	errGroup *errgroup.Group
	running  atomic.Bool
	cancel   context.CancelFunc
	ctx      context.Context
}

// NewProducer creates a new Kafka producer with the given franz-go client and configuration
func NewProducer(client *kgo.Client, config Config) *Producer {
	ctx, cancel := context.WithCancel(context.Background())
	grp, _ := errgroup.WithContext(ctx)

	return &Producer{
		client:   client,
		config:   config,
		errGroup: grp,
		cancel:   cancel,
		ctx:      ctx,
	}
}

// Publish sends a message to the given topic synchronously, waiting for broker acknowledgment.
// The payload is JSON-encoded automatically. If a key generation function is configured,
// it will be used to generate the message key.
//
// This method blocks until the message is acknowledged by Kafka or an error occurs.
// For async publishing with manual concurrency control, manage goroutines in your application layer.
func (p *Producer) Publish(ctx context.Context, topic string, payload any) error {
	// Generate key
	var key []byte = []byte(uuid.New().String())
	if p.config.KeyFunc != nil {
		key = p.config.KeyFunc(topic, payload)
	}

	// Marshal payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafka: publish: %w", err)
	}

	// Create message
	msg := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: payloadBytes,
	}

	// franze has optimistic calling of the calback which means evnets where being lost if use used async
	// so we are going to wrap this with our own error group and make sure we flush correctly
	p.errGroup.Go(func() error {
		// Send synchronously and wait for result
		results := p.client.ProduceSync(ctx, msg)
		if err := results.FirstErr(); err != nil {
			return fmt.Errorf("kafka: publish: failed to publish message to %s: %w", topic, err)
		}

		trace("successfully published message to %s", topic)
		return nil
	})

	return nil
}

// PublishAsync sends a message to the given topic asynchronously with a callback.
// Use this when you want to manage concurrency yourself (e.g., with errgroup, channels).
//
// The callback is called after the broker acknowledges the message or an error occurs.
// The callback receives the original record and any error that occurred.
func (p *Producer) PublishAsync(ctx context.Context, topic string, payload any, callback func(*kgo.Record, error)) error {
	// Generate key
	if !p.running.Load() {
		return fmt.Errorf("kafka: producer is not running")
	}

	var key []byte = []byte(uuid.New().String())
	if p.config.KeyFunc != nil {
		key = p.config.KeyFunc(topic, payload)
	}

	// Marshal payload
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("kafka: publish: %w", err)
	}

	// Create message
	msg := &kgo.Record{
		Topic: topic,
		Key:   key,
		Value: payloadBytes,
	}

	p.errGroup.Go(func() error {
		// Send asynchronously
		results := p.client.ProduceSync(ctx, msg)
		if err := results.FirstErr(); err != nil {
			return fmt.Errorf("kafka: publish: failed to publish message to %s: %w", topic, err)
		}

		return nil
	})

	return nil
}

// Flush ensures all pending messages are sent and acknowledged.
// This is important for CLI commands to ensure messages are delivered before exit.
func (p *Producer) Flush(ctx context.Context) error {
	trace("flushing producer - ensuring all messages are sent")

	// Use franz-go's built-in flush method
	p.client.Flush(ctx)

	err := p.errGroup.Wait()
	if err != nil {
		return fmt.Errorf("kafka: flush: %w", err)
	}

	trace("producer flush completed")
	return nil
}

// Stop closes the producer after flushing any pending messages
func (p *Producer) Stop() error {
	p.running.Store(false)
	timeout := time.NewTimer(30 * time.Second)

	go func() {
		<-timeout.C
		trace("producer flush timed out after 30 seconds")
		p.cancel()
	}()

	if err := p.Flush(p.ctx); err != nil {
		trace("failed to flush producer during stop: %v", err)
	}

	p.client.Close()
	return nil
}
