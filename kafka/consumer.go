package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golly-go/golly"
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

// Header represents a Kafka message header.
type Header struct {
	Key   string
	Value []byte
}

// Message represents a Kafka message consumed from a topic.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
	Timestamp time.Time
}

// GetHeader returns the value of the first header with the given key.
func (m Message) GetHeader(key string) []byte {
	for _, h := range m.Headers {
		if h.Key == key {
			return h.Value
		}
	}
	return nil
}

// Consumer defines the interface for consuming Kafka messages.
// Implementations must be safe for concurrent use.
type Consumer interface {
	// Handler is called for each message. Return an error to prevent offset commit
	// and trigger redelivery after restart (at-least-once semantics).
	Handler(ctx context.Context, msg Message) error

	// SubscribeOptions returns the subscription configuration.
	SubscribeOptions() SubscribeOptions
}

// consumerHandle wraps a Consumer with its Kafka client and configuration.
// A single handle can span multiple topics — one kgo.Client subscribed to
// several topics at once is far cheaper (one set of broker connections, one
// group-coordination session) than one client per topic, and franz-go
// supports it natively. Records still carry their own Message.Topic, so a
// Handler that cares which topic a message came from can still branch on it.
type consumerHandle struct {
	client   *kgo.Client      // Kafka client (created lazily)
	consumer Consumer         // User's consumer implementation
	opts     SubscribeOptions // Subscription options
	topics   []string         // Topic names this client consumes
	groupID  string           // Consumer group ID
	tracker  any              // Optional caller-supplied label, used only for logging

	// ctx/cancel are derived from the manager's context when this handle is
	// started. Cancelling them stops only this subscription - unlike the
	// manager's own context, which stops every subscription at once.
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed when run() returns, so unsubscribe can wait for cleanup
}

// run is the main polling loop for a consumer. It continuously polls Kafka for messages,
// processes them using the consumer's Handler, and commits offsets after successful processing.
func (h *consumerHandle) run(ctx context.Context) error {
	defer h.closeClient()

	log := h.logger()
	log.Tracef("consumer starting (topics=%s group=%s)", h.topicList(), h.groupID)

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

			timer := time.NewTimer(backoff)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
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
func (h *consumerHandle) processFetches(ctx context.Context, fetches kgo.Fetches, log *golly.Entry) error {
	if fetches.NumRecords() == 0 {
		return nil
	}

	// Process all records in the fetch
	for _, fetch := range fetches {
		for _, record := range fetch.Topics {
			for _, partition := range record.Partitions {
				for _, r := range partition.Records {
					if err := h.processRecord(ctx, r, log); err != nil {
						// On error, we stop processing this batch to preserve offset order
						return err
					}
				}
			}
		}
	}

	// Commit all records in this fetch at once for efficiency
	if h.groupID != "" {
		if err := h.client.CommitRecords(ctx, fetches.Records()...); err != nil {
			log.Warnf("failed to commit offsets for batch: %v", err)
			// We don't return error here because the messages were processed successfully.
			// The next commit will include these offsets anyway.
		}
	}

	// Log partition-level errors
	fetches.EachError(func(topic string, partition int32, err error) {
		log.Errorf("partition error (topic=%s partition=%d): %v", topic, partition, err)
	})

	return nil
}

// processRecord handles a single Kafka record: converts it to a Message,
// calls the user's Handler. Offsets are committed in batch by processFetches.
func (h *consumerHandle) processRecord(ctx context.Context, record *kgo.Record, log *golly.Entry) error {
	msg := h.convertRecord(record)

	// Call user handler
	if err := h.consumer.Handler(ctx, msg); err != nil {
		log.Errorf("handler error (partition=%d offset=%d): %v", record.Partition, record.Offset, err)
		// At-least-once: do not commit on error; record will be redelivered
		return err
	}

	return nil
}

// convertRecord transforms a kgo.Record into our Message type.
func (h *consumerHandle) convertRecord(record *kgo.Record) Message {
	msg := Message{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       record.Key,
		Value:     record.Value,
		Timestamp: record.Timestamp,
	}

	if len(record.Headers) > 0 {
		msg.Headers = make([]Header, len(record.Headers))
		for i, header := range record.Headers {
			msg.Headers[i] = Header{
				Key:   header.Key,
				Value: header.Value,
			}
		}
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
		golly.DefaultLogger().Errorf("[kafka] panic in consumer (topics=%s group=%s): %v", h.topicList(), h.groupID, r)
	}
	if h.client != nil {
		h.client.Close()
	}
}

// topicList joins this handle's topics for logging.
func (h *consumerHandle) topicList() string {
	return strings.Join(h.topics, ",")
}

// logger creates a structured logger for this consumer.
func (h *consumerHandle) logger() *golly.Entry {
	return golly.DefaultLogger().WithFields(golly.Fields{
		"topics":  h.topicList(),
		"group":   h.groupID,
		"tracker": h.tracker,
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
	nextID    atomic.Uint64
}

// Subscription represents a single call to Subscribe. Use Stop to tear down
// just that subscription - its Kafka client, its goroutine, and its
// bookkeeping in the manager - without touching any other subscription,
// even one registered for the same topic and group.
//
// Callers that Subscribe on behalf of something with its own lifecycle
// (a websocket/SSE connection, a job, a request) must Stop the returned
// Subscription when that thing goes away. Nothing does this automatically:
// each Subscribe call gets its own independent Kafka client, so failing to
// Stop it leaks that client and its goroutines for the remaining lifetime
// of the process.
type Subscription struct {
	id string
	cm *ConsumerManager
}

// Stop cancels this subscription and blocks until its goroutine has exited
// and its Kafka client has closed. Safe to call more than once, and safe to
// call on a nil *Subscription.
func (s *Subscription) Stop() error {
	if s == nil || s.cm == nil {
		return nil
	}
	return s.cm.unsubscribe(s.id)
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

	golly.DefaultLogger().Infof("[kafka] starting %d consumers", len(cm.consumers))

	// Start all consumers, collecting any errors
	var errs []error
	for id, handle := range cm.consumers {
		if err := cm.startConsumer(id, handle); err != nil {
			golly.DefaultLogger().Errorf("[kafka] failed to start consumer %s", err)
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

// Stop gracefully stops all consumers and waits for them to finish.
func (cm *ConsumerManager) Stop() error {
	if !cm.running.CompareAndSwap(true, false) {
		return nil // Already stopped
	}

	golly.DefaultLogger().Info("[kafka] stopping consumers")

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

	golly.DefaultLogger().Info("[kafka] all consumers stopped")
	return nil
}

// Subscribe registers a consumer for one or more topics. All given topics
// are consumed by a single Kafka client under one consumer-group session -
// pass every topic a given consumer cares about in one call rather than
// calling Subscribe once per topic, which would otherwise create a
// separate client (and its own broker connections/goroutines) per topic
// for no reason when it's the same consumer and group. The consumer will
// start immediately if the manager is already running, or when Start() is
// called otherwise.
//
// Each call creates an independent subscription, even if one already exists
// for the same topics and group - e.g. groupless subscriptions (used for
// fan-out consumers like pushing a topic to many websocket/SSE connections)
// always have an empty GroupID and would otherwise be indistinguishable.
// Use the returned Subscription to stop this one later.
func (cm *ConsumerManager) Subscribe(consumer Consumer, topics ...string) (*Subscription, error) {
	return cm.subscribe(nil, consumer, topics...)
}

// Unsubscribe stops sub. Equivalent to sub.Stop(); provided for callers that
// prefer an explicit verb over a method on the returned handle.
func (cm *ConsumerManager) Unsubscribe(sub *Subscription) error {
	if sub == nil {
		return nil
	}
	return cm.unsubscribe(sub.id)
}

func (cm *ConsumerManager) subscribe(tracker any, consumer Consumer, topics ...string) (*Subscription, error) {
	opts := consumer.SubscribeOptions()
	// Suffix with a counter so this is unique per call, not per topic+group -
	// generateSubscriptionID alone is just a human-readable label for logs.
	id := fmt.Sprintf("%s#%d", generateSubscriptionID(topics, opts), cm.nextID.Add(1))

	handle := &consumerHandle{
		consumer: consumer,
		opts:     opts,
		topics:   topics,
		groupID:  opts.GroupID,
		tracker:  tracker,
		client:   nil, // Created lazily when consumer starts
		done:     make(chan struct{}),
	}

	cm.mu.Lock()
	cm.consumers[id] = handle
	cm.mu.Unlock()

	golly.DefaultLogger().Tracef("[kafka] registered consumer (topics=%s group=%s tracker=%v)", handle.topicList(), opts.GroupID, tracker)

	// If manager is already running, start this consumer immediately
	if cm.ctx != nil && cm.running.Load() {
		if err := cm.startConsumer(id, handle); err != nil {
			cm.mu.Lock()
			delete(cm.consumers, id)
			cm.mu.Unlock()
			return nil, err
		}
	}

	return &Subscription{id: id, cm: cm}, nil
}

// unsubscribe stops one subscription by id: it cancels only that
// subscription's own context (every other subscription is untouched),
// removes it from the manager's bookkeeping, and waits for its goroutine -
// and the Kafka client it owns - to fully shut down.
func (cm *ConsumerManager) unsubscribe(id string) error {
	cm.mu.Lock()
	handle, ok := cm.consumers[id]
	if ok {
		delete(cm.consumers, id)
	}
	cm.mu.Unlock()

	if !ok {
		return nil // already stopped, or unknown id
	}

	if handle.cancel != nil {
		handle.cancel()
		<-handle.done
	}

	golly.DefaultLogger().Infof("[kafka] consumer stopped (topics=%s group=%s tracker=%v)", handle.topicList(), handle.groupID, handle.tracker)
	return nil
}

// startConsumer creates the Kafka client and spawns the consumer goroutine.
// The handle gets its own context, derived from the manager's, so it can be
// cancelled independently by unsubscribe without stopping every other
// consumer. Cancelling the manager's own context (via Stop) still cascades
// to every handle, since they're all descendants of it.
func (cm *ConsumerManager) startConsumer(id string, handle *consumerHandle) error {
	// Create Kafka client if not already created
	if handle.client == nil {
		client, err := createConsumerClient(handle, cm.config)
		if err != nil {
			return fmt.Errorf("failed to create client: %w", err)
		}
		handle.client = client
	}

	handle.ctx, handle.cancel = context.WithCancel(cm.ctx)

	// Spawn consumer goroutine
	cm.wg.Add(1)
	go func(h *consumerHandle) {
		defer cm.wg.Done()
		defer close(h.done)
		if err := h.run(h.ctx); err != nil && !errors.Is(err, context.Canceled) {
			golly.DefaultLogger().Errorf("[kafka] consumer error (topics=%s group=%s): %v",
				h.topicList(), h.groupID, err)
		}
	}(handle)

	golly.DefaultLogger().Infof("[kafka] consumer started (topics=%s group=%s tracker=%v)", handle.topicList(), handle.groupID, handle.tracker)
	return nil
}

// createConsumerClient builds a franz-go client configured for consuming.
func createConsumerClient(handle *consumerHandle, config Config) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.ConsumeTopics(handle.topics...),
		kgo.FetchMaxWait(60 * time.Second),
		kgo.RequestTimeoutOverhead(60 * time.Second),
	}

	// Enable franz-go internal logging at trace level
	if golly.DefaultLogger().Level() == golly.LogLevelTrace {
		opts = append(opts, kgo.WithLogger(
			kgo.BasicLogger(&logWriter{golly.DefaultLogger(), golly.LogLevelTrace}, kgo.LogLevelInfo, nil),
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

// generateSubscriptionID creates a unique ID for a topics+group subscription.
func generateSubscriptionID(topics []string, opts SubscribeOptions) string {
	return strings.Join(topics, ",") + "-" + opts.GroupID
}

type logWriter struct {
	lg    *golly.Logger
	level golly.Level
}

func (w *logWriter) Write(p []byte) (n int, err error) {
	w.lg.Log(w.level, string(p))
	return len(p), nil
}
