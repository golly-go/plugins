package kafka

import (
	"fmt"

	"github.com/golly-go/golly"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	PluginName = "kafka"
)

// Plugin implements the golly.Plugin interface
type Plugin struct {
	config          Config
	producer        *Producer
	consumerManager *ConsumerManager
	client          *kgo.Client

	cfgFunc func(app *golly.Application) Config
}

// NewPlugin creates a new Kafka plugin
func NewPlugin(opts ...Option) *Plugin {
	p := &Plugin{}
	for _, opt := range opts {
		opt(&p.config)
	}
	return p
}

func NewPluginWithConfigFunc(cfgFunc func(app *golly.Application) Config) *Plugin {
	return &Plugin{
		config:  DefaultConfig(),
		cfgFunc: cfgFunc,
	}
}

// Name returns the plugin name
func (p *Plugin) Name() string { return PluginName }

// Initialize sets up the Kafka plugin
func (p *Plugin) Initialize(app *golly.Application) error {
	trace("initializing Kafka plugin")

	if p.cfgFunc != nil {
		p.config = p.cfgFunc(app)
	}

	// Create franz-go client
	client, err := p.createClient()
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}
	p.client = client

	// Create producer if enabled
	if p.config.EnableProducer {
		p.producer = NewProducer(client, p.config)
	}

	// Consumer manager will be created by the service when needed

	return nil
}

// Deinitialize cleans up the plugin
func (p *Plugin) Deinitialize(app *golly.Application) error {
	trace("deinitializing Kafka plugin")

	// Consumer manager lifecycle is managed by the service

	// Stop producer
	if p.producer != nil {
		return p.producer.Stop()
	}

	return nil
}

// ConsumerManager returns the consumer manager, creating it if needed
func (p *Plugin) ConsumerManager() *ConsumerManager {
	if p.consumerManager == nil {
		p.consumerManager = NewConsumerManager(p.client)
	}
	return p.consumerManager
}

// Producer returns the producer instance
func (p *Plugin) Producer() *Producer {
	return p.producer
}

func (p *Plugin) Services() []golly.Service {
	return []golly.Service{
		NewService(p),
	}
}

// createClient creates a franz-go client
func (p *Plugin) createClient() (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(p.config.Brokers...),
		kgo.ClientID(p.config.ClientID),
	}

	// Add authentication if configured
	if p.config.Username != "" && p.config.Password != "" {
		// TODO: Add SASL authentication
		// This will need proper franz-go SASL implementation
	}

	// Add TLS if enabled
	if p.config.TLSEnabled {
		opts = append(opts, kgo.DialTLS())
	}

	return kgo.NewClient(opts...)
}

var _ golly.Plugin = (*Plugin)(nil)
