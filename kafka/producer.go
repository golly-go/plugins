package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer handles Kafka message publishing with reliability guarantees
type Producer struct {
	// client is the franz-go client used for publishing
	client *kgo.Client

	// config contains the producer configuration
	config Config
}

// NewProducer creates a new Kafka producer with the given franz-go client and configuration
func NewProducer(client *kgo.Client, config Config) *Producer {
	return &Producer{
		client: client,
		config: config,
	}
}

// Publish sends a message to the given topic asynchronously for better performance.
// The payload is JSON-encoded automatically. If a key generation function is configured,
// it will be used to generate the message key.
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

	// Send asynchronously - much faster!
	p.client.Produce(ctx, msg, func(record *kgo.Record, err error) {
		if err != nil {
			golly.Logger().Errorf("[kafka] publish: failed to publish message to %s: %v", record.Topic, err)
			return
		}
		trace("successfully published message to %s", record.Topic)
	})

	return nil
}

// Flush ensures all pending messages are sent and acknowledged.
// This is important for CLI commands to ensure messages are delivered before exit.
func (p *Producer) Flush(ctx context.Context) error {
	trace("flushing producer - ensuring all messages are sent")

	// Use franz-go's built-in flush method
	p.client.Flush(ctx)

	trace("producer flush completed")
	return nil
}

// Stop closes the producer after flushing any pending messages
func (p *Producer) Stop() error {
	// Flush any pending messages before closing
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := p.Flush(ctx); err != nil {
		trace("failed to flush producer during stop: %v", err)
	}

	p.client.Close()
	return nil
}
