package kafka

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/golly-go/golly"
)

// Service implements golly.Service for Kafka consumers
type Service struct {
	consumers *ConsumerManager
	app       *golly.Application
	ctx       context.Context
	cancel    context.CancelFunc
	running   atomic.Bool
}

// NewService creates a new Kafka service
func NewService(plugin *Plugin) *Service {
	return &Service{}
}

// Name returns the service name
func (s *Service) Name() string {
	return "kafka-consumers"
}

// Description returns the service description
func (s *Service) Description() string {
	return "Kafka consumer service for processing messages"
}

// Initialize prepares the service
func (s *Service) Initialize(app *golly.Application) error {
	plugin := GetPlugin()

	if plugin == nil {
		return fmt.Errorf("kafka plugin not found")
	}

	s.consumers = plugin.consumerManager

	s.app = app
	return nil
}

// Start begins the Kafka consumer service and blocks until Stop is called.
// The framework runs this in its own goroutine.
func (s *Service) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	if err := s.consumers.Start(); err != nil {
		return err
	}

	s.running.Store(true)
	// Block until Stop is called
	<-s.ctx.Done()
	s.running.Store(false)

	return nil
}

// Stop gracefully stops the Kafka consumer service
func (s *Service) Stop() error {
	// Signal Start() to unblock
	if s.cancel != nil {
		s.cancel()
	}

	// Stop the consumer manager
	if s.consumers != nil {
		return s.consumers.Stop()
	}

	return nil
}

// IsRunning indicates if the service is active
func (s *Service) IsRunning() bool {
	if s.ctx == nil {
		return false
	}

	return s.running.Load()
}
