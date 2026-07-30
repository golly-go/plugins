package kafka

import (
	"context"
	"fmt"

	"github.com/golly-go/golly"
)

// GetPlugin retrieves the Kafka plugin from the golly application
func GetPlugin() *Plugin {
	plugin := golly.GetPlugin[*Plugin](golly.App(), PluginName)
	if plugin == nil {
		return nil
	}

	return plugin
}

// GetProducer retrieves the Kafka producer from the application
func GetProducer() *Producer {
	if plugin := GetPlugin(); plugin != nil {
		return plugin.Producer()
	}
	return nil
}

// GetConsumerManager retrieves the Kafka consumer manager from the application
func GetConsumerManager() *ConsumerManager {
	if plugin := GetPlugin(); plugin != nil {
		return plugin.consumers()
	}

	return nil
}

// Subscribe registers a consumer for a topic using the consumer manager.
// tracker is an optional caller-supplied label (shown in logs) identifying
// who owns this subscription - e.g. a connection ID for a websocket/SSE
// fan-out consumer. It has no effect on uniqueness: every call to Subscribe
// creates its own independent subscription regardless of tracker, topic, or
// group. Callers must Unsubscribe (or call sub.Stop()) when whatever they
// subscribed on behalf of goes away, or the Kafka client and goroutines
// created here will run for the remaining lifetime of the process.
func Subscribe(tracker any, topic string, consumer Consumer) (*Subscription, error) {
	plugin := golly.GetPlugin[*Plugin](golly.App(), PluginName)

	if plugin == nil {
		return nil, fmt.Errorf("[KAFKA] plugin not found")
	}

	consumers := plugin.consumers()
	if consumers == nil {
		return nil, fmt.Errorf("[KAFKA] consumer manager not found")
	}

	return consumers.subscribe(tracker, topic, consumer)
}

// Unsubscribe stops a subscription previously returned by Subscribe.
func Unsubscribe(sub *Subscription) error {
	if sub == nil {
		return nil
	}
	return sub.Stop()
}

// Publish is a convenience function to publish a message using the global producer
func Publish(ctx context.Context, topic string, payload any) error {
	producer := GetProducer()
	if producer == nil {
		golly.DefaultLogger().Warnf("[KAFKA] attempted to publish to %s but producer is not available (EnableProducer=false?)", topic)
		return fmt.Errorf("[KAFKA] producer not available")
	}
	return producer.Publish(ctx, topic, payload)
}

// trace logs a formatted message with the Kafka prefix
func trace(msg string, args ...any) {
	golly.DefaultLogger().Tracef("[KAFKA] %s", fmt.Sprintf(msg, args...))
}
