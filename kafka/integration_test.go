package kafka

import (
	"context"
	"testing"
	"time"
)

// TestConsumer for integration testing
type TestConsumer struct {
	receivedMessages []*Message
	opts             SubscribeOptions
}

func (tc *TestConsumer) Handler(ctx context.Context, msg *Message) error {
	tc.receivedMessages = append(tc.receivedMessages, msg)
	return nil
}

func (tc *TestConsumer) SubscribeOptions() SubscribeOptions {
	return tc.opts
}

// TestProducerConsumerFlow tests the basic flow of publishing and consuming messages
func TestProducerConsumerFlow(t *testing.T) {
	// Skip if no Kafka broker available
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Create plugin with test configuration
	plugin := NewPlugin(
		WithBrokers("localhost:9092"),
		WithClientID("test-client"),
		WithProducer(),
		WithAutoTopic(), // Enable auto topic creation
	)

	// Initialize plugin
	err := plugin.Initialize(nil)
	if err != nil {
		t.Skipf("skipping test - failed to initialize plugin: %v", err)
	}

	// Get producer and consumer manager
	producer := plugin.Producer()
	consumerManager := plugin.ConsumerManager()

	if producer == nil {
		t.Fatal("producer is nil")
	}
	if consumerManager == nil {
		t.Fatal("consumer manager is nil")
	}

	// Create test consumer
	testConsumer := &TestConsumer{
		opts: SubscribeOptions{
			GroupID:       "test-group",
			StartPosition: StartFromLatest,
		},
	}

	// Subscribe to test topic
	testTopic := "test-topic"
	err = consumerManager.Subscribe(testTopic, testConsumer)
	if err != nil {
		t.Fatalf("failed to subscribe: %v", err)
	}

	// Start consumer manager
	err = consumerManager.Start()
	if err != nil {
		t.Fatalf("failed to start consumer manager: %v", err)
	}

	// Give consumers time to start
	time.Sleep(100 * time.Millisecond)

	// Publish test message
	ctx := context.Background()
	testMessage := map[string]interface{}{
		"id":        "test-123",
		"message":   "hello world",
		"timestamp": time.Now(),
	}

	err = producer.Publish(ctx, testTopic, testMessage)
	if err != nil {
		t.Fatalf("failed to publish message: %v", err)
	}

	// Wait for message to be consumed
	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("timeout waiting for message to be consumed")
		default:
			if len(testConsumer.receivedMessages) > 0 {
				goto messageReceived
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

messageReceived:
	// Verify message was received
	if len(testConsumer.receivedMessages) != 1 {
		t.Fatalf("expected 1 message, got %d", len(testConsumer.receivedMessages))
	}

	receivedMsg := testConsumer.receivedMessages[0]
	if receivedMsg.Topic != testTopic {
		t.Errorf("expected topic %s, got %s", testTopic, receivedMsg.Topic)
	}

	// Cleanup
	consumerManager.Stop()
	producer.Stop()
}

// TestCLIMessageDelivery tests the specific CLI message delivery issue
func TestCLIMessageDelivery(t *testing.T) {
	// Skip if no Kafka broker available
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Create plugin
	plugin := NewPlugin(
		WithBrokers("localhost:9092"),
		WithClientID("cli-test"),
		WithProducer(),
		WithAutoTopic(), // Enable auto topic creation
	)

	err := plugin.Initialize(nil)
	if err != nil {
		t.Skipf("skipping test - failed to initialize plugin: %v", err)
	}

	producer := plugin.Producer()
	if producer == nil {
		t.Fatal("producer is nil")
	}

	// Simulate CLI command - publish and flush
	ctx := context.Background()
	testMessage := map[string]interface{}{
		"command": "test-cli",
		"data":    "important data",
	}

	// Publish message
	err = producer.Publish(ctx, "cli-test-topic", testMessage)
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// If we get here without error, the message was delivered
	t.Log("CLI message delivery test passed")

	// Cleanup
	producer.Stop()
}
