package kafka

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/golly-go/golly"
)

const pluginName = "kafka"

const (
	publisherCtxKey golly.ContextKey = "kafka-publisher"
	consumerCtxKey  golly.ContextKey = "kafka-consumer"
)

// Plugin wires a Kafka Publisher into the app and optionally exposes the consumer Service.
// It does not run its own loop; lifecycle is Initialize/Deinitialize.
type Plugin struct {
	publisher *Publisher
	service   *Service

	opts    []Option
	cfgFunc func(app *golly.Application) Config
}

type PluginOption func(*Plugin)

// WithOptions sets default options used for both publisher and consumers
func WithOptions(opts ...Option) PluginOption {
	return func(p *Plugin) { p.opts = append(p.opts, opts...) }
}

func NewPlugin(opts ...PluginOption) *Plugin {
	p := &Plugin{}
	for _, o := range opts {
		o(p)
	}

	p.service = NewService(p.opts...)
	p.publisher = NewPublisher(p.opts...)

	return p
}

func (p *Plugin) Name() string { return pluginName }

func (p *Plugin) Configure(f func(app *golly.Application) Config) error {
	p.cfgFunc = f
	if p.service != nil {
		p.service.Configure(f)
	}
	return nil
}

// Initialize configures and starts the publisher. Consumers are configured in Service.Initialize.
func (p *Plugin) Initialize(app *golly.Application) error {
	// Build publisher options from config if not passed explicitly
	if p.publisher == nil {
		p.publisher = NewPublisher()
	}

	cfg := app.Config()
	// Minimal env driven config; callers may also pass options at construction time
	opts := make([]Option, 0, 8)

	// Gather brokers from both slice and comma-separated string
	var brokers []string

	if s := cfg.GetString("kafka.brokers"); s != "" {
		parts := strings.Split(s, ",")
		for _, p := range parts {
			if v := strings.TrimSpace(p); v != "" {
				brokers = append(brokers, v)
			}
		}
	}

	if len(brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured; set kafka.brokers in config or configure the plugin")
	}

	opts = append(opts, WithBrokers(brokers))

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

	if p.cfgFunc != nil {
		kCfg := p.cfgFunc(app)
		p.publisher.cfg = kCfg
	}

	return p.publisher.Start()
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
	return []golly.Service{p.service}
}

// High-level instance API to avoid reaching into internal structs
func (p *Plugin) Subscribe(handler Handler, opts ...SubOption) error {
	if p.service == nil || p.service.Bus() == nil {
		return fmt.Errorf("kafka: consumer bus not available")
	}
	return p.service.Bus().Subscribe(handler, opts...)
}

func (p *Plugin) Consumers() *Consumers {
	if p.service != nil {
		return p.service.Bus()
	}
	return nil
}

func (p *Plugin) Unsubscribe(topic string, handler Handler) {
	if p.service != nil && p.service.Bus() != nil {
		p.service.Bus().Unsubscribe(topic, handler)
	}
}

func (p *Plugin) Publish(ctx context.Context, topic string, payload any) error {
	if p.publisher == nil {
		return fmt.Errorf("kafka publisher not found")
	}
	return p.publisher.Publish(ctx, topic, payload)
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

func GetPlugin() *Plugin {
	if pm := golly.CurrentPlugins(); pm != nil {
		if p, ok := pm.Get(pluginName).(*Plugin); ok {
			return p
		}
	}
	return nil
}

func GetConsumer() *Consumers {
	if p := GetPlugin(); p != nil {
		return p.service.Bus()
	}
	return nil
}

// Subscribe registers a handler
func Subscribe[T any](topic T, handler Handler, opts ...SubOption) *Consumers {
	topics := []string{}
	switch any(topic).(type) {
	case string:
		topics = []string{any(topic).(string)}
	case []string:
		topics = any(topic).([]string)
	}

	consumer := GetConsumer()
	if consumer == nil {
		trace("kafka: no consumer struct found")
		return nil
	}

	opts = append(opts, SubscribeWithTopics(topics...))
	if err := consumer.Subscribe(handler, opts...); err != nil {
		golly.Logger().Errorf("kafka: subscribe error: %v", err)
	}

	return consumer
}

func Publish(ctx context.Context, topic string, payload []byte) error {
	publisher := GetPublisher(ctx)
	if publisher == nil {
		return fmt.Errorf("kafka publisher not found")
	}
	return publisher.Publish(ctx, topic, payload)
}

var _ golly.Plugin = (*Plugin)(nil)
