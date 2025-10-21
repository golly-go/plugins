package kafka

import (
	"time"
)

// RequiredAcks specifies the acknowledgment level for producers
type RequiredAcks int

const (
	// AckNone - no acknowledgment required (0)
	AckNone RequiredAcks = 0
	// AckLeader - leader acknowledgment only (1)
	AckLeader RequiredAcks = 1
	// AckAll - all replicas acknowledgment (-1)
	AckAll RequiredAcks = -1
)

// Compression specifies the compression algorithm
type Compression string

const (
	CompressionNone   Compression = "none"
	CompressionGzip   Compression = "gzip"
	CompressionSnappy Compression = "snappy"
	CompressionLZ4    Compression = "lz4"
	CompressionZstd   Compression = "zstd"
)

// SASLMechanism specifies the SASL authentication mechanism
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "PLAIN"
	SASLScramSHA256 SASLMechanism = "SCRAM-SHA-256"
	SASLScramSHA512 SASLMechanism = "SCRAM-SHA-512"
)

// Config holds the Kafka configuration
type Config struct {
	Brokers        []string
	ClientID       string
	GroupID        string
	ReadMinBytes   int
	ReadMaxBytes   int
	ReadMaxWait    time.Duration
	WriteTimeout   time.Duration
	AllowAutoTopic bool

	// Authentication
	Username string
	Password string
	SASL     SASLMechanism

	// TLS
	TLSEnabled bool

	// Producer settings
	RequiredAcks RequiredAcks
	Compression  Compression

	// Consumer settings
	StartFromLatest bool
	CommitInterval  time.Duration

	// Retry settings
	MaxRetries   int
	RetryBackoff time.Duration

	// Service settings
	EnableProducer  bool
	EnableConsumers bool

	// Key generation function
	KeyFunc func(topic string, payload any) []byte
}

// DefaultConfig returns a sensible default configuration
func DefaultConfig() Config {
	return Config{
		ReadMinBytes:    1024,
		ReadMaxBytes:    1048576,
		ReadMaxWait:     250 * time.Millisecond,
		WriteTimeout:    5 * time.Second,
		AllowAutoTopic:  true,
		RequiredAcks:    AckAll, // Require all replicas
		Compression:     CompressionSnappy,
		CommitInterval:  0, // Manual commit
		MaxRetries:      3,
		RetryBackoff:    100 * time.Millisecond,
		EnableProducer:  true,
		EnableConsumers: false,
	}
}

// Option configures the Kafka plugin
type Option func(*Config)

// WithBrokers sets the Kafka brokers
func WithBrokers(brokers ...string) Option {
	return func(c *Config) {
		c.Brokers = brokers
	}
}

// WithClientID sets the client ID
func WithClientID(clientID string) Option {
	return func(c *Config) {
		c.ClientID = clientID
	}
}

// WithProducer enables the producer
func WithProducer() Option {
	return func(c *Config) {
		c.EnableProducer = true
	}
}

// WithConsumers enables consumers
func WithConsumers() Option {
	return func(c *Config) {
		c.EnableConsumers = true
	}
}

// WithTLS enables TLS
func WithTLS() Option {
	return func(c *Config) {
		c.TLSEnabled = true
	}
}

// WithCredentials sets username and password
func WithCredentials(username, password string) Option {
	return func(c *Config) {
		c.Username = username
		c.Password = password
	}
}

// WithGroupID sets the default group ID
func WithGroupID(groupID string) Option {
	return func(c *Config) {
		c.GroupID = groupID
	}
}

// WithAutoTopic enables auto topic creation
func WithAutoTopic() Option {
	return func(c *Config) {
		c.AllowAutoTopic = true
	}
}

// WithStartFromLatest sets consumers to start from latest
func WithStartFromLatest() Option {
	return func(c *Config) {
		c.StartFromLatest = true
	}
}

// WithKeyFunc sets the key generation function
func WithKeyFunc(keyFunc func(topic string, payload any) []byte) Option {
	return func(c *Config) {
		c.KeyFunc = keyFunc
	}
}

// WithRequiredAcks sets the acknowledgment level
func WithRequiredAcks(acks RequiredAcks) Option {
	return func(c *Config) {
		c.RequiredAcks = acks
	}
}

// WithCompression sets the compression algorithm
func WithCompression(compression Compression) Option {
	return func(c *Config) {
		c.Compression = compression
	}
}

// WithSASL sets the SASL mechanism
func WithSASL(mechanism SASLMechanism) Option {
	return func(c *Config) {
		c.SASL = mechanism
	}
}
