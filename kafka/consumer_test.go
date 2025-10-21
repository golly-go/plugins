package kafka

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Mock consumer for testing
type MockConsumer struct {
	handledMessages []*kgo.Record
	opts            SubscribeOptions
	shouldError     bool
}

func (m *MockConsumer) Handler(ctx context.Context, msg *kgo.Record) error {
	m.handledMessages = append(m.handledMessages, msg)
	if m.shouldError {
		return context.Canceled // Use a real error type
	}
	return nil
}

func (m *MockConsumer) SubscribeOptions() SubscribeOptions {
	return m.opts
}

func TestConsumerSubscribeOptions(t *testing.T) {
	tests := []struct {
		name     string
		consumer Consumer
		expected SubscribeOptions
	}{
		{
			name: "event sourcing consumer",
			consumer: &MockConsumer{
				opts: SubscribeOptions{
					GroupID:           "user-service",
					StartFromLatest:   false,
					StartFromEarliest: false,
				},
			},
			expected: SubscribeOptions{
				GroupID:           "user-service",
				StartFromLatest:   false,
				StartFromEarliest: false,
			},
		},
		{
			name: "websocket consumer",
			consumer: &MockConsumer{
				opts: SubscribeOptions{
					GroupID:           "",
					StartFromLatest:   true,
					StartFromEarliest: false,
				},
			},
			expected: SubscribeOptions{
				GroupID:           "",
				StartFromLatest:   true,
				StartFromEarliest: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.consumer.SubscribeOptions()

			if opts.GroupID != tt.expected.GroupID {
				t.Errorf("expected GroupID %s, got %s", tt.expected.GroupID, opts.GroupID)
			}

			if opts.StartFromLatest != tt.expected.StartFromLatest {
				t.Errorf("expected StartFromLatest %v, got %v", tt.expected.StartFromLatest, opts.StartFromLatest)
			}

			if opts.StartFromEarliest != tt.expected.StartFromEarliest {
				t.Errorf("expected StartFromEarliest %v, got %v", tt.expected.StartFromEarliest, opts.StartFromEarliest)
			}
		})
	}
}

func TestGenerateSubscriptionID(t *testing.T) {
	tests := []struct {
		name     string
		topic    string
		opts     SubscribeOptions
		expected string
	}{
		{
			name:  "with group ID",
			topic: "events.user",
			opts: SubscribeOptions{
				GroupID: "user-service",
			},
			expected: "events.user-user-service",
		},
		{
			name:  "without group ID",
			topic: "notifications",
			opts: SubscribeOptions{
				GroupID: "",
			},
			expected: "notifications-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateSubscriptionID(tt.topic, tt.opts)
			if result != tt.expected {
				t.Errorf("expected %s, got %s", tt.expected, result)
			}
		})
	}
}

func TestMockConsumer(t *testing.T) {
	consumer := &MockConsumer{
		opts: SubscribeOptions{
			GroupID: "test-group",
		},
	}

	// Test handler
	ctx := context.Background()
	msg := &kgo.Record{
		Topic: "test-topic",
		Value: []byte("test-message"),
	}

	err := consumer.Handler(ctx, msg)
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}

	if len(consumer.handledMessages) != 1 {
		t.Errorf("expected 1 message, got %d", len(consumer.handledMessages))
	}

	if consumer.handledMessages[0] != msg {
		t.Error("expected handled message to match input message")
	}

	// Test subscribe options
	opts := consumer.SubscribeOptions()
	if opts.GroupID != "test-group" {
		t.Errorf("expected GroupID 'test-group', got '%s'", opts.GroupID)
	}
}
