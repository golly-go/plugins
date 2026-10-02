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

func TestTopicPrefix(t *testing.T) {
	cfg := DefaultConfig()
	if got := cfg.topicName("orders"); got != "orders" {
		t.Errorf("expected unprefixed topic, got %s", got)
	}
	if got := cfg.trimTopicPrefix("development-orders"); got != "development-orders" {
		t.Errorf("expected topic untouched without prefix, got %s", got)
	}

	WithTopicPrefix("development")(&cfg)
	if got := cfg.topicName("orders"); got != "development-orders" {
		t.Errorf("expected development-orders, got %s", got)
	}

	trims := map[string]string{
		"development-orders": "orders",
		"orders":             "orders",            // not prefixed
		"developmentorders":  "developmentorders", // missing separator
		"development-":       "",                  // empty remainder
		"development":        "development",       // prefix only
		"staging-orders":     "staging-orders",    // other environment
		"dev-orders":         "dev-orders",        // shorter prefix
	}
	for in, want := range trims {
		if got := cfg.trimTopicPrefix(in); got != want {
			t.Errorf("trimTopicPrefix(%q): expected %q, got %q", in, want, got)
		}
	}
}

func TestTrimTopicPrefixNoAllocs(t *testing.T) {
	cfg := DefaultConfig()
	WithTopicPrefix("development")(&cfg)

	topic := "development-orders"
	allocs := testing.AllocsPerRun(100, func() {
		_ = cfg.trimTopicPrefix(topic)
	})
	if allocs != 0 {
		t.Errorf("expected 0 allocs, got %v", allocs)
	}
}

func TestProducerTopicName(t *testing.T) {
	cfg := DefaultConfig()
	p := NewProducer(nil, cfg)
	if got := p.topicName("orders"); got != "orders" {
		t.Errorf("expected unprefixed topic, got %s", got)
	}

	WithTopicPrefix("development")(&cfg)
	p = NewProducer(nil, cfg)
	if got := p.topicName("orders"); got != "development-orders" {
		t.Errorf("expected development-orders, got %s", got)
	}
	if got := p.topicName("users"); got != "development-users" {
		t.Errorf("expected development-users, got %s", got)
	}

	// Cached lookups must not allocate.
	allocs := testing.AllocsPerRun(100, func() {
		_ = p.topicName("orders")
	})
	if allocs != 0 {
		t.Errorf("expected 0 allocs on cached topic, got %v", allocs)
	}
}

func TestGroupPrefix(t *testing.T) {
	cfg := DefaultConfig()
	if got := cfg.groupName("billing"); got != "billing" {
		t.Errorf("expected unprefixed group, got %s", got)
	}

	WithGroupPrefix("development")(&cfg)
	if got := cfg.groupName("billing"); got != "development-billing" {
		t.Errorf("expected development-billing, got %s", got)
	}
	// Groupless subscriptions must stay groupless.
	if got := cfg.groupName(""); got != "" {
		t.Errorf("expected empty group to stay empty, got %q", got)
	}
}
