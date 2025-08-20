package kafka

import (
	"context"
	"sync"
)

// MockPublisher is a simple in-memory PublisherAPI implementation for tests.
type MockPublisher struct {
	mu   sync.Mutex
	Sent []Published
	Err  error
}

type Published struct {
	Topic   string
	Payload []byte
}

func (m *MockPublisher) Publish(ctx context.Context, topic string, payload []byte) error {
	m.mu.Lock()
	m.Sent = append(m.Sent, Published{Topic: topic, Payload: append([]byte(nil), payload...)})
	m.mu.Unlock()
	return m.Err
}

var _ PublisherAPI = (*MockPublisher)(nil)
