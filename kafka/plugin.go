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

func (p Plugin) Client() *kgo.Client { return p.client }
func (p Plugin) Config() Config      { return p.config }

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
		trace("creating Kafka producer")
		p.producer = NewProducer(client, p.config)
		p.producer.running.Store(true)
	} else {
		trace("producer disabled (EnableProducer=false)")
	}

	// Consumer manager will be created by the service when needed

	return nil
}

// Deinitialize cleans up the plugin - intentionally does nothing.
// We use AfterDeinitialize instead to ensure all other plugins (especially projections)
// have finished publishing their Kafka messages before we shut down the producer.
func (p *Plugin) Deinitialize(app *golly.Application) error {
	return nil
}

// AfterDeinitialize runs after all other plugins have deinitialized.
// This ensures projections and other plugins have finished publishing their Kafka messages
// before we shut down the producer.
func (p *Plugin) AfterDeinitialize(app *golly.Application) error {
	trace("after deinitialize - stopping Kafka producer")

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
		p.consumerManager = NewConsumerManager(p.client, p.config)
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

	// Configure producer settings if enabled
	if p.config.EnableProducer {
		// Map our RequiredAcks to franz-go's Acks type
		var acks kgo.Acks
		switch p.config.RequiredAcks {
		case AckNone:
			acks = kgo.NoAck()
		case AckLeader:
			acks = kgo.LeaderAck()
		case AckAll:
			acks = kgo.AllISRAcks()
		default:
			acks = kgo.AllISRAcks() // Default to all replicas
		}
		opts = append(opts, kgo.RequiredAcks(acks))

		// Set producer timeout if configured
		if p.config.WriteTimeout > 0 {
			opts = append(opts, kgo.ProduceRequestTimeout(p.config.WriteTimeout))
		}

		// Allow auto topic creation if configured
		// Note: This still requires broker to have auto.create.topics.enable=true
		if p.config.AllowAutoTopic {
			opts = append(opts, kgo.AllowAutoTopicCreation())
		}
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
