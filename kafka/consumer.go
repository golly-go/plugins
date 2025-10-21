package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/golly-go/golly"
	"github.com/twmb/franz-go/pkg/kgo"
)

// SubscribeOptions configures how a consumer subscribes to topics
type SubscribeOptions struct {
	// GroupID is the consumer group ID. If empty, the consumer will not be part of a group
	// and will start from the latest offset. For event sourcing, use a group ID to track offsets.
	GroupID string

	// StartPosition determines where to start consuming from when there's no committed offset
	StartPosition StartPosition
}

// Message represents a Kafka message with essential data
type Message struct {
	// Topic is the Kafka topic this message was received from
	Topic string

	// Partition is the partition number within the topic
	Partition int32

	// Offset is the message offset within the partition
	Offset int64

	// Key is the message key (can be nil)
	Key []byte

	// Value is the message payload
	Value []byte

	// Headers contains additional metadata as key-value pairs
	Headers map[string][]byte

	// Timestamp is when the message was produced to Kafka
	Timestamp time.Time
}

// Consumer defines the interface for Kafka consumers
type Consumer interface {
	// Handler processes a single Kafka message. If this returns an error,
	// the message will be retried according to the retry policy.
	Handler(ctx context.Context, msg *Message) error

	// SubscribeOptions returns the configuration for how this consumer
	// should subscribe to topics (group ID, starting position, etc.)
	SubscribeOptions() SubscribeOptions
}

// ConsumerBase handles the Kafka reading loop with panic protection
type ConsumerBase struct {
	// handler is the business logic consumer that processes messages
	handler Consumer

	// client is the franz-go client for this consumer
	client *kgo.Client

	// topic is the Kafka topic this consumer is subscribed to
	topic string

	// opts contains the subscription configuration
	opts SubscribeOptions

	// groupID is the consumer group ID (cached from opts for convenience)
	groupID string
}

// ProcessEvent runs the Kafka reading loop with panic protection.
// This method polls Kafka for messages and calls the consumer's Handler for each message.
// It handles panics gracefully and provides basic error handling.
func (cb *ConsumerBase) ProcessEvent(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			golly.Logger().Errorf("panic in consumer: %v", r)
		}
	}()

	// Create consumer group or simple consumer based on options
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Fetch messages from Kafka
			fetches := cb.client.PollFetches(ctx)
			if fetches.Err() != nil {
				err := fetches.Err()
				if err == context.Canceled {
					return err
				}
				// TODO: Add retry logic and logging
				continue
			}

			// Process each message
			fetches.EachRecord(func(record *kgo.Record) {
				// Convert kgo.Record to our Message
				msg := &Message{
					Topic:     record.Topic,
					Partition: record.Partition,
					Offset:    record.Offset,
					Key:       record.Key,
					Value:     record.Value,
					Headers:   make(map[string][]byte),
					Timestamp: record.Timestamp,
				}

				// Copy headers
				for _, h := range record.Headers {
					msg.Headers[h.Key] = h.Value
				}

				// Call the handler
				if err := cb.handler.Handler(ctx, msg); err != nil {
					// TODO: Handle handler errors (DLQ, retry, etc.)
					return
				}

				// Commit offset if using consumer group
				if cb.opts.GroupID != "" {
					// TODO: Implement offset committing
				}
			})
		}
	}
}

// ConsumerManager manages multiple consumers and their lifecycle
type ConsumerManager struct {
	// client is the shared franz-go client for the manager
	client *kgo.Client

	// consumers maps subscription IDs to their ConsumerBase instances
	consumers map[string]*ConsumerBase

	// ctx is the context for all consumers (cancelled on Stop)
	ctx context.Context

	// cancel is the cancel function for the context
	cancel context.CancelFunc

	// wg tracks all running consumer goroutines
	wg sync.WaitGroup

	// mu protects the consumers map from concurrent access
	mu sync.RWMutex
}

// NewConsumerManager creates a new consumer manager with the given franz-go client
func NewConsumerManager(client *kgo.Client) *ConsumerManager {
	return &ConsumerManager{
		client:    client,
		consumers: make(map[string]*ConsumerBase),
	}
}

// Start initializes the context and starts all registered consumers in goroutines.
// This method is safe to call multiple times.
func (cm *ConsumerManager) Start() error {
	cm.ctx, cm.cancel = context.WithCancel(context.Background())

	cm.mu.RLock()
	for subscriptionID, consumer := range cm.consumers {
		cm.wg.Add(1)
		go func(id string, c *ConsumerBase) {
			defer cm.wg.Done()
			c.ProcessEvent(cm.ctx)
		}(subscriptionID, consumer)
	}
	cm.mu.RUnlock()

	return nil
}

// Stop gracefully stops all consumers by cancelling the context and waiting for
// all consumer goroutines to finish. This method is safe to call multiple times.
func (cm *ConsumerManager) Stop() error {
	// Stop accepting new events
	cm.cancel()

	// Wait for all consumers to finish processing
	cm.wg.Wait()

	// Close the client
	cm.client.Close()
	return nil
}

// Subscribe adds a new consumer subscription to a topic. The consumer's SubscribeOptions
// determine how the subscription is configured (group ID, starting position, etc.).
// This method should be called before Start().
func (cm *ConsumerManager) Subscribe(topic string, consumer Consumer) error {
	opts := consumer.SubscribeOptions()
	subscriptionID := generateSubscriptionID(topic, opts)

	// Create a dedicated client for this consumer
	clientOpts := []kgo.Opt{
		kgo.ConsumeTopics(topic),
	}

	// Configure consumer group if specified
	if opts.GroupID != "" {
		clientOpts = append(clientOpts, kgo.ConsumerGroup(opts.GroupID))
	}

	switch opts.StartPosition {
	case StartFromLatest:
		clientOpts = append(clientOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	case StartFromEarliest:
		clientOpts = append(clientOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	client, err := kgo.NewClient(clientOpts...)
	if err != nil {
		return fmt.Errorf("failed to create consumer client: %w", err)
	}

	base := &ConsumerBase{
		handler: consumer,
		client:  client,
		topic:   topic,
		opts:    opts,
		groupID: opts.GroupID,
	}

	cm.mu.Lock()
	cm.consumers[subscriptionID] = base
	cm.mu.Unlock()

	// If already started, start this consumer immediately
	if cm.ctx != nil {
		cm.wg.Add(1)
		go func() {
			defer cm.wg.Done()
			base.ProcessEvent(cm.ctx)
		}()
	}

	return nil
}

// generateSubscriptionID creates a unique ID for a subscription based on topic and group ID
func generateSubscriptionID(topic string, opts SubscribeOptions) string {
	// TODO: Implement proper ID generation
	return topic + "-" + opts.GroupID
}
