package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer handles Kafka message publishing
type Producer struct {
	client *kgo.Client
	config Config
}

// NewProducer creates a new Kafka producer
func NewProducer(client *kgo.Client, config Config) *Producer {
	return &Producer{
		client: client,
		config: config,
	}
}

// Publish sends a message to the given topic
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

	// Send with retry logic
	return p.publishWithRetry(ctx, msg)
}

// publishWithRetry handles retry logic for publishing
func (p *Producer) publishWithRetry(ctx context.Context, msg *kgo.Record) error {
	var attempt int
	backoff := p.config.RetryBackoff
	if backoff <= 0 {
		backoff = 100 * time.Millisecond
	}

	for {
		// Send message
		result := p.client.ProduceSync(ctx, msg)

		// Check for errors
		if result.FirstErr() == nil {
			return nil
		}

		attempt++
		if attempt > p.config.MaxRetries || ctx.Err() != nil {
			return fmt.Errorf("kafka: publish failed after %d attempts: %w", attempt, result.FirstErr())
		}

		// Exponential backoff
		time.Sleep(backoff)
		backoff = time.Duration(float64(backoff) * 1.5)
		if backoff > 5*time.Second {
			backoff = 5 * time.Second
		}
	}
}

// Flush ensures all pending messages are sent
func (p *Producer) Flush(ctx context.Context) error {
	// franz-go handles flushing automatically with ProduceSync
	// This method exists for interface compatibility
	return nil
}

// Stop closes the producer
func (p *Producer) Stop() error {
	p.client.Close()
	return nil
}
