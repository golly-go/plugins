package kafka

import (
	"errors"
	"fmt"

	"github.com/golly-go/golly"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
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

	// Configure SASL authentication
	if p.config.SASL != "" {
		sasl, err := saslMechanism(p.config)
		if err != nil {
			return nil, fmt.Errorf("failed to configure SASL authentication: %w", err)
		}
		opts = append(opts, sasl)
	}

	// Add TLS if enabled
	if p.config.TLSEnabled {
		opts = append(opts, kgo.DialTLS())
	}

	return kgo.NewClient(opts...)
}

var (
	ErrTokenProviderRequired               = errors.New("token provider is required for OAUTHBEARER authentication")
	ErrUsernamePasswordRequired            = errors.New("username and password are required for PLAIN authentication")
	ErrUsernamePasswordRequiredSCRAMSHA256 = errors.New("username and password are required for SCRAM-SHA-256 authentication")
	ErrUsernamePasswordRequiredSCRAMSHA512 = errors.New("username and password are required for SCRAM-SHA-512 authentication")
	ErrInvalidSASLMechanism                = errors.New("invalid sasl mechanism")
)

func saslMechanism(config Config) (kgo.Opt, error) {
	switch config.SASL {
	case SASLOAUTHBearer:
		if config.TokenProvider == nil {
			return nil, ErrTokenProviderRequired

		}

		// Use our custom OAuthBearerAuth that calls TokenProvider on each auth attempt
		// This ensures we get fresh tokens for AWS/GCP which expire
		return kgo.SASL(OAuthBearerAuth{
			TokenProvider: config.TokenProvider,
		}), nil

	case SASLPlain:
		if config.Username == "" || config.Password == "" {
			return nil, ErrUsernamePasswordRequired
		}
		return kgo.SASL(plain.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsMechanism()), nil

	case SASLScramSHA256:
		if config.Username == "" || config.Password == "" {
			return nil, ErrUsernamePasswordRequiredSCRAMSHA256
		}

		return kgo.SASL(scram.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsSha256Mechanism()), nil

	case SASLScramSHA512:
		if config.Username == "" || config.Password == "" {
			return nil, ErrUsernamePasswordRequiredSCRAMSHA512
		}

		return kgo.SASL(scram.Auth{
			User: config.Username,
			Pass: config.Password,
		}.AsSha512Mechanism()), nil
	}

	return nil, ErrInvalidSASLMechanism
}

var _ golly.Plugin = (*Plugin)(nil)
