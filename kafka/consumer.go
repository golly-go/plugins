package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	pollTimeout      = 10 * time.Second
	maxRetryBackoff  = 30 * time.Second
	baseRetryBackoff = 1 * time.Second
)

// SubscribeOptions configures how a consumer subscribes to topics.
type SubscribeOptions struct {
	GroupID       string        // Consumer group ID for load balancing and offset tracking
	StartPosition StartPosition // Where to start consuming from (only for new groups)
}

// Message represents a Kafka message consumed from a topic.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Timestamp time.Time
}

// Consumer defines the interface for consuming Kafka messages.
// Implementations must be safe for concurrent use.
type Consumer interface {
	// Handler is called for each message. Return an error to prevent offset commit
	// and trigger redelivery after restart (at-least-once semantics).
	Handler(ctx context.Context, msg *Message) error

	// SubscribeOptions returns the subscription configuration.
	SubscribeOptions() SubscribeOptions
}

// consumerHandle wraps a Consumer with its Kafka client and configuration.
type consumerHandle struct {
	client   *kgo.Client      // Kafka client (created lazily)
	consumer Consumer         // User's consumer implementation
	opts     SubscribeOptions // Subscription options
	topic    string           // Topic name
	groupID  string           // Consumer group ID
}

// run is the main polling loop for a consumer. It continuously polls Kafka for messages,
// processes them using the consumer's Handler, and commits offsets after successful processing.
func (h *consumerHandle) run(ctx context.Context) error {
	defer h.closeClient()

	log := h.logger()
	log.Tracef("consumer starting (topic=%s group=%s)", h.topic, h.groupID)

	retries := 0
	for {
		// Check if we should exit
		if err := ctx.Err(); err != nil {
			return err
		}

		// Poll for messages with timeout
		fetches, err := h.poll(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}

			// Handle transient errors with exponential backoff
			retries++
			backoff := h.calculateBackoff(retries)
			log.Warnf("poll error, retrying in %v: %v", backoff, err)
			time.Sleep(backoff)
			continue
		}

		// Reset retry counter on successful poll
		retries = 0

		// Process fetched records
		if err := h.processFetches(ctx, fetches, log); err != nil {
			return err
		}
	}
}

// poll fetches messages from Kafka with a timeout.
func (h *consumerHandle) poll(ctx context.Context) (kgo.Fetches, error) {
	pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)
	defer cancel()

	fetches := h.client.PollFetches(pollCtx)

	// Check for fetch-level errors
	if err := fetches.Err(); err != nil {
		// Ignore our internal poll timeout - it just means no data
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return kgo.Fetches{}, nil
		}
		return kgo.Fetches{}, err
	}

	return fetches, nil
}

// processFetches handles all records in a fetch result.
func (h *consumerHandle) processFetches(ctx context.Context, fetches kgo.Fetches, log *logrus.Entry) error {
	if fetches.NumRecords() == 0 {
		return nil
	}

	// Process each record
	fetches.EachRecord(func(record *kgo.Record) {
		if err := h.processRecord(ctx, record, log); err != nil {
			// Error already logged in processRecord
			return
		}
	})

	// Log partition-level errors
	fetches.EachError(func(topic string, partition int32, err error) {
		log.Errorf("partition error (topic=%s partition=%d): %v", topic, partition, err)
	})

	return nil
}

// processRecord handles a single Kafka record: converts it to a Message,
// calls the user's Handler, and commits the offset on success.
func (h *consumerHandle) processRecord(ctx context.Context, record *kgo.Record, log *logrus.Entry) error {
	msg := h.convertRecord(record)

	// Call user handler
	if err := h.consumer.Handler(ctx, msg); err != nil {
		log.Errorf("handler error (partition=%d offset=%d): %v", record.Partition, record.Offset, err)
		// At-least-once: do not commit on error; record will be redelivered
		return err
	}

	// Commit offset after successful processing (if in a consumer group)
	if h.groupID != "" {
		if err := h.client.CommitRecords(ctx, record); err != nil {
			log.Warnf("failed to commit offset (partition=%d offset=%d): %v",
				record.Partition, record.Offset, err)
		}
	}

	return nil
}

// convertRecord transforms a kgo.Record into our Message type.
func (h *consumerHandle) convertRecord(record *kgo.Record) *Message {
	msg := &Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       record.Key,
		Value:     record.Value,
		Headers:   make(map[string][]byte, len(record.Headers)),
		Timestamp: record.Timestamp,
	}

	for _, header := range record.Headers {
		msg.Headers[header.Key] = header.Value
	}

	return msg
}

// calculateBackoff computes exponential backoff with a maximum cap.
func (h *consumerHandle) calculateBackoff(retries int) time.Duration {
	backoff := baseRetryBackoff * time.Duration(1<<uint(retries))
	if backoff > maxRetryBackoff {
		backoff = maxRetryBackoff
	}
	return backoff
}

// closeClient safely closes the Kafka client and handles panics.
func (h *consumerHandle) closeClient() {
	if r := recover(); r != nil {
		golly.Logger().Errorf("[kafka] panic in consumer (topic=%s group=%s): %v", h.topic, h.groupID, r)
	}
	if h.client != nil {
		h.client.Close()
	}
}

// logger creates a structured logger for this consumer.
func (h *consumerHandle) logger() *logrus.Entry {
	return golly.Logger().WithFields(logrus.Fields{
		"topic": h.topic,
		"group": h.groupID,
	})
}

// ConsumerManager manages the lifecycle of all Kafka consumers.
type ConsumerManager struct {
	config    Config
	consumers map[string]*consumerHandle
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.RWMutex
	running   atomic.Bool
}

// NewConsumerManager creates a new consumer manager.
func NewConsumerManager(config Config) *ConsumerManager {
	return &ConsumerManager{
		config:    config,
		consumers: make(map[string]*consumerHandle),
	}
}

// IsRunning returns true if the manager is currently running.
func (cm *ConsumerManager) IsRunning() bool {
	return cm.running.Load()
}

// Start begins consuming messages from all registered consumers.
// Returns an error if any consumer fails to start.
func (cm *ConsumerManager) Start() error {
	if !cm.running.CompareAndSwap(false, true) {
		return nil // Already running
	}

	cm.ctx, cm.cancel = context.WithCancel(context.Background())

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	golly.Logger().Infof("[kafka] starting %d consumers", len(cm.consumers))

	// Start all consumers, collecting any errors
	var firstErr error
	for id, handle := range cm.consumers {
		if err := cm.startConsumer(id, handle); err != nil {
			golly.Logger().WithError(err).Errorf("[kafka] failed to start consumer %s", id)
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// Stop gracefully stops all consumers and waits for them to finish.
func (cm *ConsumerManager) Stop() error {
	if !cm.running.CompareAndSwap(true, false) {
		return nil // Already stopped
	}

	golly.Logger().Info("[kafka] stopping consumers")

	// Cancel context to signal all consumers to stop
	if cm.cancel != nil {
		cm.cancel()
	}

	// Wait for all consumer goroutines to exit
	cm.wg.Wait()

	// Close any remaining clients
	cm.mu.RLock()
	for _, consumer := range cm.consumers {
		if consumer.client != nil {
			consumer.client.Close()
		}
	}
	cm.mu.RUnlock()

	golly.Logger().Info("[kafka] all consumers stopped")
	return nil
}

// Subscribe registers a consumer for a topic. The consumer will start
// when the manager's Start() method is called.
func (cm *ConsumerManager) Subscribe(topic string, consumer Consumer) error {
	opts := consumer.SubscribeOptions()
	subscriptionID := generateSubscriptionID(topic, opts)

	handle := &consumerHandle{
		consumer: consumer,
		opts:     opts,
		topic:    topic,
		groupID:  opts.GroupID,
		client:   nil, // Created lazily when consumer starts
	}

	cm.mu.Lock()
	cm.consumers[subscriptionID] = handle
	cm.mu.Unlock()

	golly.Logger().Tracef("[kafka] registered consumer (topic=%s group=%s)", topic, opts.GroupID)

	// If manager is already running, start this consumer immediately
	if cm.ctx != nil && cm.running.Load() {
		return cm.startConsumer(subscriptionID, handle)
	}

	return nil
}

// startConsumer creates the Kafka client and spawns the consumer goroutine.
func (cm *ConsumerManager) startConsumer(id string, handle *consumerHandle) error {
	// Create Kafka client if not already created
	if handle.client == nil {
		client, err := createConsumerClient(handle, cm.config)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		handle.client = client
	}

	// Spawn consumer goroutine
	cm.wg.Add(1)
	go func(h *consumerHandle) {
		defer cm.wg.Done()
		if err := h.run(cm.ctx); err != nil && !errors.Is(err, context.Canceled) {
			golly.Logger().Errorf("[kafka] consumer error (topic=%s group=%s): %v",
				h.topic, h.groupID, err)
		}
	}(handle)

	golly.Logger().Infof("[kafka] consumer started (topic=%s group=%s)", handle.topic, handle.groupID)
	return nil
}

// createConsumerClient builds a franz-go client configured for consuming.
func createConsumerClient(handle *consumerHandle, config Config) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.ConsumeTopics(handle.topic),
		kgo.FetchMaxWait(60 * time.Second),
		kgo.RequestTimeoutOverhead(60 * time.Second),
	}

	// Enable franz-go internal logging at trace level
	if golly.Logger().Level == logrus.TraceLevel {
		opts = append(opts, kgo.WithLogger(
			kgo.BasicLogger(golly.Logger().WriterLevel(logrus.TraceLevel), kgo.LogLevelInfo, nil),
		))
	}

	// Configure consumer group options
	if handle.groupID != "" {
		opts = append(opts,
			kgo.ConsumerGroup(handle.groupID),
			kgo.DisableAutoCommit(),
			kgo.SessionTimeout(30*time.Second),
			kgo.HeartbeatInterval(3*time.Second),
			kgo.RebalanceTimeout(60*time.Second),
		)
	}

	// Configure starting offset position (only applies to new consumer groups)
	switch handle.opts.StartPosition {
	case StartFromLatest:
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	case StartFromEarliest:
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	// Create client using shared factory (handles brokers, auth, TLS)
	return createClient(config, opts...)
}

// generateSubscriptionID creates a unique ID for a topic+group subscription.
func generateSubscriptionID(topic string, opts SubscribeOptions) string {
	return topic + "-" + opts.GroupID
}
