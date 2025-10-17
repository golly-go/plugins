package kafka

import (
	"context"
	"fmt"

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

	opts    []Option
	cfgFunc func(app *golly.Application) Config
}

type PluginOption func(*Plugin)

// WithOptions sets default options used for both publisher and consumers
func WithOptions(opts ...Option) PluginOption {
	return func(p *Plugin) { p.opts = append(p.opts, opts...) }
}

func NewPlugin(fnc ...func(app *golly.Application) Config) *Plugin {
	var f func(app *golly.Application) Config
	if len(fnc) > 0 {
		f = fnc[0]
	}

	return &Plugin{
		service: NewService(Config{}),
		cfgFunc: f,
	}
}

func (p *Plugin) Name() string { return pluginName }

// Initialize configures and starts the publisher. Consumers are configured in Service.Initialize.
func (p *Plugin) Initialize(app *golly.Application) error {
	var config Config
	if p.cfgFunc != nil {
		config = p.cfgFunc(app)
	} else {
		config = ConfigFromApp(app)
	}

	if len(config.Brokers) == 0 {
		return fmt.Errorf("kafka: no brokers configured; set kafka.brokers in config or configure the plugin")
	}

	p.service.ApplyConfig(config)
	p.publisher = NewPublisher(config)

	return p.publisher.Start()
}

func (p *Plugin) Deinitialize(app *golly.Application) error {
	if p.publisher == nil {
		return nil
	}
	p.publisher.Stop()
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
