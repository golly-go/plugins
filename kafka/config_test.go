package kafka

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Test default values
	if cfg.ReadMinBytes != 1024 {
		t.Errorf("expected ReadMinBytes 1024, got %d", cfg.ReadMinBytes)
	}

	if cfg.RequiredAcks != AckAll {
		t.Errorf("expected RequiredAcks AckAll, got %v", cfg.RequiredAcks)
	}

	if cfg.Compression != CompressionSnappy {
		t.Errorf("expected Compression Snappy, got %v", cfg.Compression)
	}

	if !cfg.EnableProducer {
		t.Error("expected EnableProducer true")
	}

}

func TestWithBrokers(t *testing.T) {
	cfg := Config{}
	WithBrokers("localhost:9092", "localhost:9093")(&cfg)

	if len(cfg.Brokers) != 2 {
		t.Errorf("expected 2 brokers, got %d", len(cfg.Brokers))
	}

	if cfg.Brokers[0] != "localhost:9092" {
		t.Errorf("expected first broker localhost:9092, got %s", cfg.Brokers[0])
	}
}

func TestWithProducer(t *testing.T) {
	cfg := Config{}
	WithProducer()(&cfg)

	if !cfg.EnableProducer {
		t.Error("expected EnableProducer true")
	}
}

func TestWithRequiredAcks(t *testing.T) {
	cfg := Config{}
	WithRequiredAcks(AckLeader)(&cfg)

	if cfg.RequiredAcks != AckLeader {
		t.Errorf("expected RequiredAcks AckLeader, got %v", cfg.RequiredAcks)
	}
}

func TestWithCompression(t *testing.T) {
	cfg := Config{}
	WithCompression(CompressionGzip)(&cfg)

	if cfg.Compression != CompressionGzip {
		t.Errorf("expected Compression Gzip, got %v", cfg.Compression)
	}
}

func TestWithCredentials(t *testing.T) {
	cfg := Config{}
	WithCredentials("user", "pass")(&cfg)

	if cfg.Username != "user" {
		t.Errorf("expected Username 'user', got '%s'", cfg.Username)
	}

	if cfg.Password != "pass" {
		t.Errorf("expected Password 'pass', got '%s'", cfg.Password)
	}
}
