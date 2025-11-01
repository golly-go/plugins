package kafka

import (
	"context"
	"sync/atomic"

	"github.com/golly-go/golly"
)

// Service implements golly.Service for Kafka consumers
type Service struct {
	plugin  *Plugin
	app     *golly.Application
	ctx     context.Context
	cancel  context.CancelFunc
	running atomic.Bool
}

// NewService creates a new Kafka service
func NewService(plugin *Plugin) *Service {
	return &Service{
		plugin: plugin,
	}
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
	s.app = app
	return nil
}

// Start begins the Kafka consumer service and blocks until Stop is called.
// The framework runs this in its own goroutine.
func (s *Service) Start() error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	consumerManager := s.plugin.ConsumerManager()
	if err := consumerManager.Start(); err != nil {
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
	consumerManager := s.plugin.ConsumerManager()
	return consumerManager.Stop()
}

// IsRunning indicates if the service is active
func (s *Service) IsRunning() bool {
	if s.ctx == nil {
		return false
	}

	return s.running.Load()
}
