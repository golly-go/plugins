package kafka

import (
	"errors"
	"fmt"
	"os"

	"github.com/golly-go/golly"
)

const (
	PluginName = "kafka"
)

// Plugin implements the golly.Plugin interface
type Plugin struct {
	config          Config
	producer        *Producer
	consumerManager *ConsumerManager

	cfgFunc func(app *golly.Application) Config
}

func (p Plugin) Config() Config { return p.config }

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

	// do not boot in migration mode
	if golly.Contains(os.Args, "migration") {
		return nil
	}

	if p.cfgFunc != nil {
		p.config = p.cfgFunc(app)
	}

	// Create producer if enabled
	if !p.config.EnableProducer {
		trace("producer disabled (EnableProducer=false)")
		return nil
	}

	client, err := createClient(p.config)
	if err != nil {
		return fmt.Errorf("failed to create Kafka client: %w", err)
	}

	p.producer = NewProducer(client, p.config)
	p.producer.running.Store(true)

	p.consumerManager = NewConsumerManager(p.config)

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

func (p *Plugin) consumers() *ConsumerManager {
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

var (
	ErrTokenProviderRequired               = errors.New("token provider is required for OAUTHBEARER authentication")
	ErrUsernamePasswordRequired            = errors.New("username and password are required for PLAIN authentication")
	ErrUsernamePasswordRequiredSCRAMSHA256 = errors.New("username and password are required for SCRAM-SHA-256 authentication")
	ErrUsernamePasswordRequiredSCRAMSHA512 = errors.New("username and password are required for SCRAM-SHA-512 authentication")
	ErrInvalidSASLMechanism                = errors.New("invalid sasl mechanism")
)

var _ golly.Plugin = (*Plugin)(nil)
