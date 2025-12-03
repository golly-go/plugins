package kafka

import (
	"context"
	"testing"
)

// Mock consumer for testing
type MockConsumer struct {
	handledMessages []*Message
	opts            SubscribeOptions
	shouldError     bool
}

func (m *MockConsumer) Handler(ctx context.Context, msg *Message) error {
	m.handledMessages = append(m.handledMessages, msg)
	if m.shouldError {
		return context.Canceled // Use a real error type
	}
	return nil
}

func (m *MockConsumer) SubscribeOptions() SubscribeOptions {
	return m.opts
}

func (m *MockConsumer) ProcessEvent(ctx context.Context) error {
	return m.Handler(ctx, nil)
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
					GroupID:       "user-service",
					StartPosition: StartFromDefault,
				},
			},
			expected: SubscribeOptions{
				GroupID:       "user-service",
				StartPosition: StartFromDefault,
			},
		},
		{
			name: "websocket consumer",
			consumer: &MockConsumer{
				opts: SubscribeOptions{
					GroupID:       "",
					StartPosition: StartFromLatest,
				},
			},
			expected: SubscribeOptions{
				GroupID:       "",
				StartPosition: StartFromLatest,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts := tt.consumer.SubscribeOptions()

			if opts.GroupID != tt.expected.GroupID {
				t.Errorf("expected GroupID %s, got %s", tt.expected.GroupID, opts.GroupID)
			}

			if opts.StartPosition != tt.expected.StartPosition {
				t.Errorf("expected StartPosition %v, got %v", tt.expected.StartPosition, opts.StartPosition)
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
	msg := &Message{
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
