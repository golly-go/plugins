package kafka

import (
	"fmt"

	"github.com/golly-go/golly"
)

// GetPlugin retrieves the Kafka plugin from the golly application
func GetPlugin() *Plugin {
	if plugin, ok := golly.App().Plugins().Get(PluginName).(*Plugin); ok {
		return plugin
	}
	return nil
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
		return plugin.ConsumerManager()
	}
	return nil
}

// Subscribe registers a consumer for a topic using the consumer manager
func Subscribe(topic string, consumer Consumer) error {
	if consumerManager := GetConsumerManager(); consumerManager != nil {
		return consumerManager.Subscribe(topic, consumer)
	}
	return fmt.Errorf("kafka consumer manager not found")
}

// trace logs a formatted message with the Kafka prefix
func trace(msg string, args ...any) {
	golly.Logger().Tracef("[KAFKA] %s", fmt.Sprintf(msg, args...))
}
