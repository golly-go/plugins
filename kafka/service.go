package kafka

import (
	"fmt"

	"github.com/golly-go/golly"
)

// Service implements golly.Service for Kafka consumers
type Service struct {
	plugin *Plugin
	app    *golly.Application
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

// Start begins the Kafka consumer service
func (s *Service) Start() error {
	if s.plugin.consumerManager == nil {
		return fmt.Errorf("consumer manager not initialized")
	}

	return s.plugin.consumerManager.Start()
}

// Stop gracefully stops the Kafka consumer service
func (s *Service) Stop() error {
	if s.plugin.consumerManager == nil {
		return nil
	}

	return s.plugin.consumerManager.Stop()
}

// IsRunning indicates if the service is active
func (s *Service) IsRunning() bool {
	// TODO: Implement proper running state tracking
	return s.plugin.consumerManager != nil
}
