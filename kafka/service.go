package kafka

import (
	"fmt"
	"sync/atomic"

	"github.com/golly-go/golly"
)

// Service composes Kafka consumers and implements golly.Service lifecycle.
type Service struct {
	config    Config
	consumers *Consumers
	running   atomic.Bool
}

func NewService(config Config) *Service {
	return &Service{config: config, consumers: NewConsumers(config)}
}

// Bus returns the underlying event bus (consumers implement Bus)
func (s *Service) Bus() *Consumers { return s.consumers }
func (s *Service) Name() string    { return "kafka-consumers" }

func (s *Service) ApplyConfig(config Config) {
	s.config = config
	s.consumers.ApplyConfig(config)
}

// Initialize builds consumers from app.Config(). Services and plugins are guaranteed to run before app initializers.
func (s *Service) Initialize(app *golly.Application) error {
	if len(s.config.Brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured; set kafka.brokers in config or configure the plugin")
	}

	return nil
}

func (s *Service) Start() error {
	if s.running.Load() {
		return nil
	}

	err := s.consumers.Start()
	if err != nil {
		return err
	}

	s.running.Store(true)

	// Block until Stop() is called (context is cancelled)
	<-s.consumers.ctx.Done()

	return nil
}

func (s *Service) Stop() error {
	fmt.Println("Stopping service")
	if !s.running.Load() {
		fmt.Println("Service is not running")
		return nil
	}

	fmt.Println("Stopping consumers")
	s.consumers.Stop()

	s.running.Store(false)
	return nil
}

func (s *Service) IsRunning() bool { return s.running.Load() }
