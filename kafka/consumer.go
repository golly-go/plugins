package kafka

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
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

type consumerHandle struct {
	client   *kgo.Client
	consumer Consumer
	opts     SubscribeOptions
	topic    string
	groupID  string
}

// ConsumerBase handles the Kafka reading loop with panic protection
type ConsumerBase struct{}

// ProcessEvent runs the Kafka reading loop with panic protection.
// This method polls Kafka for messages and calls the consumer's Handler for each message.
// It handles panics gracefully and provides basic error handling.
func consumerLoop(ctx context.Context, handle *consumerHandle) error {
	defer func() {
		if r := recover(); r != nil {
			golly.Logger().Errorf("panic in consumer: %v", r)
		}
	}()

	// Create consumer group or simple consumer based on options
	trace("consumer starting poll loop for topic %s group %s", handle.topic, handle.groupID)

	select {
	case <-ctx.Done():
		trace("context already canceled before entering poll loop for %s", handle.topic)
		return ctx.Err()
	default:
		trace("context is active, entering poll loop for %s", handle.topic)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Fetch messages from Kafka
			trace("polling Kafka for topic %s group %s...", handle.topic, handle.groupID)
			fetches := handle.client.PollFetches(ctx)
			trace("PollFetches returned for topic %s group %s", handle.topic, handle.groupID)
			if fetches.Err() != nil {
				err := fetches.Err()
				if err == context.Canceled {
					return err
				}

				golly.Logger().Errorf("[kafka] error polling topic %s group %s: %v", handle.topic, handle.groupID, err)
				// TODO: Add retry logic with backoff
				continue
			}

			trace("polled %d records from Kafka", fetches.NumRecords())

			// Process each message
			fetches.EachRecord(func(record *kgo.Record) {
				trace("processing message from Kafka: %s", record.Topic)

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
				if err := handle.consumer.Handler(ctx, msg); err != nil {
					// Log the error but continue processing
					golly.Logger().Errorf("[kafka] handler error for topic %s partition %d offset %d: %v",
						record.Topic, record.Partition, record.Offset, err)
					// TODO: Implement retry logic and DLQ
					return
				}

				// Commit offset after successful processing (at-least-once delivery)
				if handle.groupID != "" {
					if err := handle.client.CommitRecords(ctx, record); err != nil {
						golly.Logger().Warnf("[kafka] failed to commit offset for topic %s partition %d offset %d: %v",
							record.Topic, record.Partition, record.Offset, err)
					}
				}
			})
		}
	}
}

// ConsumerManager manages multiple consumers and their lifecycle
type ConsumerManager struct {
	// client is the shared franz-go client for the manager
	client *kgo.Client

	// config contains broker addresses and other settings needed for creating consumer clients
	config Config

	// consumers maps subscription IDs to their ConsumerBase instances
	consumers map[string]*consumerHandle

	// ctx is the context for all consumers (cancelled on Stop)
	ctx context.Context

	// cancel is the cancel function for the context
	cancel context.CancelFunc

	// wg tracks all running consumer goroutines
	wg sync.WaitGroup

	// mu protects the consumers map from concurrent access
	mu sync.RWMutex

	running atomic.Bool
}

// NewConsumerManager creates a new consumer manager with the given franz-go client and config
func NewConsumerManager(client *kgo.Client, config Config) *ConsumerManager {
	return &ConsumerManager{
		client:    client,
		config:    config,
		consumers: make(map[string]*consumerHandle),
	}
}

func (cm *ConsumerManager) IsRunning() bool { return cm.running.Load() }

// Start initializes the context and starts all registered consumers in goroutines.
// This method is safe to call multiple times.
func (cm *ConsumerManager) Start() error {
	if !cm.running.CompareAndSwap(false, true) {
		return nil
	}

	cm.ctx, cm.cancel = context.WithCancel(context.Background())

	cm.mu.RLock()
	trace("[KAFKA] starting %d consumers from manager", len(cm.consumers))
	for subscriptionID, handle := range cm.consumers {
		trace("[KAFKA] starting consumer %s (topic=%s group=%s)", subscriptionID, handle.topic, handle.groupID)
		cm.StartConsumer(subscriptionID, handle)
	}
	cm.mu.RUnlock()

	return nil
}

// Stop gracefully stops all consumers by cancelling the context and waiting for
// all consumer goroutines to finish. This method is safe to call multiple times.
func (cm *ConsumerManager) Stop() error {
	if !cm.running.CompareAndSwap(true, false) {
		return nil
	}

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
		kgo.SeedBrokers(cm.config.Brokers...),
		kgo.ConsumeTopics(topic),
	}

	// Configure consumer group if specified
	if opts.GroupID != "" {
		clientOpts = append(clientOpts,
			kgo.ConsumerGroup(opts.GroupID),
			kgo.DisableAutoCommit(), // We commit manually after successful processing
			kgo.SessionTimeout(30*time.Second),
			kgo.HeartbeatInterval(3*time.Second),
			kgo.RebalanceTimeout(60*time.Second),
		)
	}

	// Add TLS if enabled
	if cm.config.TLSEnabled {
		clientOpts = append(clientOpts, kgo.DialTLS())
	}

	// Configure SASL authentication
	if cm.config.SASL != "" {
		sasl, err := saslMechanism(cm.config)
		if err != nil {
			return fmt.Errorf("failed to configure SASL authentication: %w", err)
		}
		clientOpts = append(clientOpts, sasl)
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

	// Verify connection by pinging Kafka
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return fmt.Errorf("failed to connect to Kafka for topic %s group %s: %w", topic, opts.GroupID, err)
	}
	trace("successfully connected consumer for topic %s group %s", topic, opts.GroupID)

	handle := &consumerHandle{
		consumer: consumer,
		opts:     opts,
		topic:    topic,
		groupID:  opts.GroupID,
		client:   client,
	}
	cm.mu.Lock()
	cm.consumers[subscriptionID] = handle
	cm.mu.Unlock()

	// If already started, start this consumer immediately
	if cm.ctx != nil {
		return cm.StartConsumer(subscriptionID, handle)
	}

	return nil
}

func (cm *ConsumerManager) StartConsumer(subscriptionID string, handle *consumerHandle) error {
	cm.wg.Add(1)
	go func(handle *consumerHandle) {
		defer cm.wg.Done()
		if err := consumerLoop(cm.ctx, handle); err != nil {
			golly.Logger().Errorf("error processing event for subscription %s group %s: %v", handle.topic, handle.groupID, err)
		}
	}(handle)

	return nil
}

// generateSubscriptionID creates a unique ID for a subscription based on topic and group ID
func generateSubscriptionID(topic string, opts SubscribeOptions) string {
	// TODO: Implement proper ID generation
	return topic + "-" + opts.GroupID
}
