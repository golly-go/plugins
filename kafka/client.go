package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

// createClient creates a franz-go client
func createClient(config Config, kgoOpts ...kgo.Opt) (*kgo.Client, error) {
	opts := append(kgoOpts, []kgo.Opt{
		kgo.SeedBrokers(config.Brokers...),
		kgo.ClientID(config.ClientID),
	}...)

	trace("creating Kafka client with brokers %v and client ID %s", config.Brokers, config.ClientID)

	// Configure producer settings if enabled
	if config.EnableProducer {
		// Map our RequiredAcks to franz-go's Acks type
		var acks kgo.Acks
		switch config.RequiredAcks {
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
		if config.WriteTimeout > 0 {
			opts = append(opts, kgo.ProduceRequestTimeout(config.WriteTimeout))
		}

		// Allow auto topic creation if configured
		// Note: This still requires broker to have auto.create.topics.enable=true
		if config.AllowAutoTopic {
			opts = append(opts, kgo.AllowAutoTopicCreation())
		}
	}

	// Configure SASL authentication
	if config.SASLMechanism != nil {
		opts = append(opts, kgo.SASL(config.SASLMechanism))
	}

	// Add TLS if enabled
	if config.TLSEnabled {
		opts = append(opts, kgo.DialTLS())
	}

	return kgo.NewClient(opts...)
}
