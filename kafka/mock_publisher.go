package kafka

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// MockPublisher is a simple in-memory PublisherAPI implementation for tests.
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Publish(ctx context.Context, topic string, payload any) error {
	return m.Called(ctx, topic, payload).Error(0)
}

var _ PublisherAPI = (*MockPublisher)(nil)

func NewMockPublisher() *MockPublisher {
	return &MockPublisher{}
}
