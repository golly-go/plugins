package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/golly-go/golly"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Consumer interface for handling Kafka messages
type Consumer interface {
	Handler(ctx context.Context, msg *kgo.Record) error
	SubscribeOptions() SubscribeOptions
}

// SubscribeOptions configures how to subscribe to a topic
type SubscribeOptions struct {
	GroupID           string // required for offset tracking, empty for websockets
	StartFromLatest   bool   // for websocket notifications (no group)
	StartFromEarliest bool   // for event sourcing replay (with group)
}

// ConsumerBase handles the Kafka reading loop with panic protection
type ConsumerBase struct {
	handler Consumer
	client  *kgo.Client
	topic   string
	opts    SubscribeOptions
	groupID string
}

// ProcessEvent runs the Kafka reading loop with panic protection
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
				// Call the handler
				if err := cb.handler.Handler(ctx, record); err != nil {
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

// ConsumerManager manages multiple consumers
type ConsumerManager struct {
	client    *kgo.Client
	consumers map[string]*ConsumerBase
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	mu        sync.RWMutex
}

// NewConsumerManager creates a new consumer manager
func NewConsumerManager(client *kgo.Client) *ConsumerManager {
	return &ConsumerManager{
		client:    client,
		consumers: make(map[string]*ConsumerBase),
	}
}

// Start begins processing consumers
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

// Stop gracefully stops all consumers
func (cm *ConsumerManager) Stop() error {
	// Stop accepting new events
	cm.cancel()

	// Wait for all consumers to finish processing
	cm.wg.Wait()

	// Close the client
	cm.client.Close()
	return nil
}

// Subscribe adds a new consumer subscription
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

	// Configure starting offset
	if opts.StartFromLatest {
		clientOpts = append(clientOpts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	} else if opts.StartFromEarliest {
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

// generateSubscriptionID creates a unique ID for a subscription
func generateSubscriptionID(topic string, opts SubscribeOptions) string {
	// TODO: Implement proper ID generation
	return topic + "-" + opts.GroupID
}
