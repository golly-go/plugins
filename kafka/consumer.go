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

// SubscribeOptions configures how a consumer subscribes to topics
type SubscribeOptions struct {
	GroupID       string
	StartPosition StartPosition
}

type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   map[string][]byte
	Timestamp time.Time
}

type Consumer interface {
	Handler(ctx context.Context, msg *Message) error
	SubscribeOptions() SubscribeOptions
}

type consumerHandle struct {
	client   *kgo.Client      // Created lazily when consumer starts
	consumer Consumer         // User's consumer implementation
	opts     SubscribeOptions // Configuration
	topic    string           // Topic to consume
	groupID  string           // Consumer group ID
}

// ====== core poll loop ======

const (
	pollTimeout       = 10 * time.Second // how long we allow PollFetches to block
	rebalanceLogEvery = 30 * time.Second // throttle some logs
)

// run is the main loop for a single consumer
func (h *consumerHandle) run(ctx context.Context) error {
	defer func() {
		if r := recover(); r != nil {
			golly.Logger().Errorf("[kafka] panic in consumer (topic=%s group=%s): %v", h.topic, h.groupID, r)
		}
		// ensure client is closed when this consumer stops
		h.client.Close()
	}()

	log := golly.Logger().WithFields(logrus.Fields{
		"topic":   h.topic,
		"group":   h.groupID,
		"service": "kafka-consumer",
	})

	trace := func(msg string, args ...any) {
		log.Trace("[KAFKA] " + fmt.Sprintf(msg, args...))
	}

	retries := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		// Bound how long PollFetches can block so “hangs” show up as timeouts in logs
		pollCtx, cancel := context.WithTimeout(ctx, pollTimeout)

		fetches := h.client.PollFetches(pollCtx)
		cancel()

		// Handle fetch-level error (e.g. timeouts, auth, group errors)
		if err := fetches.Err(); err != nil {
			// If this is just our poll timeout, keep going unless the *parent* ctx is dead
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				continue
			}

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				// Respect parent context cancellation / timeout
				return err
			}

			log.Errorf("[KAFKA] PollFetches error: %v", err)
			// You may want backoff here
			retries++

			time.Sleep(time.Second * time.Duration(retries))
			continue
		}

		retries = 0

		if fetches.NumRecords() == 0 {
			continue
		}

		// Process each record
		fetches.EachRecord(func(record *kgo.Record) {
			trace("processing record (partition=%d offset=%d)", record.Partition, record.Offset)

			msg := &Message{
				Topic:     record.Topic,
				Partition: record.Partition,
				Offset:    record.Offset,
				Key:       record.Key,
				Value:     record.Value,
				Headers:   make(map[string][]byte, len(record.Headers)),
				Timestamp: record.Timestamp,
			}

			for _, h := range record.Headers {
				msg.Headers[h.Key] = h.Value
			}

			if err := h.consumer.Handler(ctx, msg); err != nil {
				log.Errorf("[KAFKA] handler error (partition=%d offset=%d): %v",
					record.Partition, record.Offset, err)
				// At-least-once: do not commit on error; record will be redelivered after restart
				return
			}

			// Commit only if we’re in a group
			if h.groupID != "" {
				if err := h.client.CommitRecords(ctx, record); err != nil {
					log.Warnf("[KAFKA] failed to commit offset (partition=%d offset=%d): %v",
						record.Topic, record.Partition, record.Offset, err)
				}
			}
		})

		fetches.EachError(func(topic string, partition int32, err error) {
			log.Errorf("[KAFKA] partition error (topic=%s partition=%d): %v", topic, partition, err)
		})
	}
}

// ====== manager ======

type ConsumerManager struct {
	config Config

	consumers map[string]*consumerHandle

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.RWMutex

	running atomic.Bool
}

func NewConsumerManager(config Config) *ConsumerManager {
	return &ConsumerManager{
		config:    config,
		consumers: make(map[string]*consumerHandle),
	}
}

func (cm *ConsumerManager) IsRunning() bool { return cm.running.Load() }

func (cm *ConsumerManager) Start() error {
	if !cm.running.CompareAndSwap(false, true) {
		return nil
	}

	cm.ctx, cm.cancel = context.WithCancel(context.Background())

	cm.mu.RLock()
	defer cm.mu.RUnlock()

	trace("[KAFKA] starting %d consumers from manager", len(cm.consumers))
	for subscriptionID, handle := range cm.consumers {
		trace("[KAFKA] starting consumer %s (topic=%s group=%s)", subscriptionID, handle.topic, handle.groupID)
		cm.startConsumer(subscriptionID, handle)
	}

	return nil
}

func (cm *ConsumerManager) Stop() error {
	if !cm.running.CompareAndSwap(true, false) {
		return nil
	}

	if cm.cancel != nil {
		cm.cancel()
	}

	// Wait for all consumer goroutines to exit; each will close its own client in run()
	cm.wg.Wait()

	// If you still use cm.client somewhere, you can close it here;
	// otherwise you can drop cm.client entirely.

	for _, consumer := range cm.consumers {
		consumer.client.Close()
	}

	return nil
}

func (cm *ConsumerManager) Subscribe(topic string, consumer Consumer) error {
	opts := consumer.SubscribeOptions()
	subscriptionID := generateSubscriptionID(topic, opts)

	// Just register the consumer - don't create client yet
	// Client will be created when the consumer service actually starts
	handle := &consumerHandle{
		consumer: consumer,
		opts:     opts,
		topic:    topic,
		groupID:  opts.GroupID,
		client:   nil, // Created lazily in startConsumer()
	}

	cm.mu.Lock()
	cm.consumers[subscriptionID] = handle
	cm.mu.Unlock()

	trace("registered consumer for topic %s group %s (client will be created when service starts)", topic, opts.GroupID)

	// If manager is already running, start it immediately
	if cm.ctx != nil && cm.running.Load() {
		return cm.startConsumer(subscriptionID, handle)
	}

	return nil
}

func (cm *ConsumerManager) startConsumer(subscriptionID string, handle *consumerHandle) error {
	// Create the Kafka client now (lazy initialization)
	if handle.client == nil {
		client, err := createConsumerClient(handle, cm.config)
		if err != nil {
			return fmt.Errorf("failed to create consumer client for %s: %w", subscriptionID, err)
		}
		handle.client = client
		trace("created Kafka client for consumer %s (topic=%s group=%s)", subscriptionID, handle.topic, handle.groupID)
	}

	cm.wg.Add(1)
	go func(h *consumerHandle) {
		defer cm.wg.Done()
		if err := h.run(cm.ctx); err != nil && !errors.Is(err, context.Canceled) {
			golly.Logger().Errorf("[kafka] consumer error (topic=%s group=%s): %v", h.topic, h.groupID, err)
		}
	}(handle)
	return nil
}

// createConsumerClient creates a Kafka client configured for the given consumer handle
func createConsumerClient(handle *consumerHandle, config Config) (*kgo.Client, error) {
	// Build consumer-specific options

	clientOpts := []kgo.Opt{
		kgo.ConsumeTopics(handle.topic),
		kgo.FetchMaxWait(60 * time.Second),
		kgo.RequestTimeoutOverhead(60 * time.Second),
		kgo.WithLogger(
			kgo.BasicLogger(golly.Logger().WriterLevel(logrus.TraceLevel), kgo.LogLevelInfo, nil),
		),
	}

	// Configure consumer group if specified
	if handle.groupID != "" {
		clientOpts = append(clientOpts,
			kgo.ConsumerGroup(handle.groupID),
			kgo.DisableAutoCommit(), // We commit manually after successful processing
			kgo.SessionTimeout(30*time.Second),
			kgo.HeartbeatInterval(3*time.Second),
			kgo.RebalanceTimeout(60*time.Second),
		)
	}

	// Configure starting offset position
	switch handle.opts.StartPosition {
	case StartFromLatest:
		clientOpts = append(clientOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	case StartFromEarliest:
		clientOpts = append(clientOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}

	// Use shared createClient function which handles brokers, auth, TLS
	client, err := createClient(config, clientOpts...)
	if err != nil {
		return nil, err
	}

	// Quick connectivity sanity check
	if err := client.Ping(context.Background()); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ping Kafka for topic %s group %s: %w", handle.topic, handle.groupID, err)
	}

	trace("successfully connected consumer for topic %s group %s", handle.topic, handle.groupID)
	return client, nil
}

func generateSubscriptionID(topic string, opts SubscribeOptions) string {
	return topic + "-" + opts.GroupID
}
