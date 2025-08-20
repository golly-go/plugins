package kafka

import (
	"context"
	"time"

	"github.com/golly-go/golly"
)

const pluginName = "kafka"

const (
	publisherCtxKey golly.ContextKey = "kafka-publisher"
)

// Plugin wires a Kafka Publisher into the app and optionally exposes the consumer Service.
// It does not run its own loop; lifecycle is Initialize/Deinitialize.
type Plugin struct {
	publisher *Publisher
	service   *Service

	opts []Option
}

type PluginOption func(*Plugin)

// WithOptions sets default options used for both publisher and consumers
func WithOptions(opts ...Option) PluginOption {
	return func(p *Plugin) { p.opts = append(p.opts, opts...) }
}

func NewPlugin(opts ...PluginOption) *Plugin {
	p := &Plugin{service: NewService()}
	for _, o := range opts {
		o(p)
	}
	p.publisher = NewPublisher(p.opts...)
	return p
}

func (p *Plugin) Name() string { return pluginName }

// Initialize configures and starts the publisher. Consumers are configured in Service.Initialize.
func (p *Plugin) Initialize(app *golly.Application) error {
	// Build publisher options from config if not passed explicitly
	if p.publisher == nil {
		p.publisher = NewPublisher()
	}

	cfg := app.Config()
	// Minimal env driven config; callers may also pass options at construction time
	opts := make([]Option, 0, 8)
	if bs := cfg.GetStringSlice("kafka.brokers"); len(bs) > 0 {
		opts = append(opts, func(c *Config) { c.Brokers = bs })
	}
	if d := cfg.GetDuration("kafka.write_timeout"); d > 0 {
		opts = append(opts, WithWriteTimeout(d))
	} else {
		opts = append(opts, WithWriteTimeout(5*time.Second))
	}
	if u := cfg.GetString("kafka.username"); u != "" {
		pw := cfg.GetString("kafka.password")
		opts = append(opts, WithUserName(u), WithPassword(pw))
	}

	// Recreate publisher with merged options
	if len(opts) > 0 {
		p.publisher = NewPublisher(append(p.opts, opts...)...)
	}
	p.publisher.Start()
	return nil
}

func (p *Plugin) Deinitialize(app *golly.Application) error {
	if p.publisher != nil {
		p.publisher.Stop()
	}
	return nil
}

// Publisher returns the shared Kafka publisher instance.
func (p *Plugin) Publisher() PublisherAPI { return p.publisher }

// Services returns the consumer service so it can be run by the app when desired.
func (p *Plugin) Services() []golly.Service {
	return []golly.Service{NewService()}
}

// Helper accessor to fetch the publisher from context or plugin manager
func GetPublisher(ctx context.Context) PublisherAPI {
	if p, ok := ctx.Value(publisherCtxKey).(PublisherAPI); ok {
		return p
	}

	if pm := golly.CurrentPlugins(); pm != nil {
		if p, ok := pm.Get(pluginName).(*Plugin); ok {
			return p.publisher
		}
	}

	return nil
}

var _ golly.Plugin = (*Plugin)(nil)
