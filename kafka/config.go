package kafka

import (
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
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

// StartPosition specifies where to start consuming from
type StartPosition int

const (
	StartFromDefault StartPosition = iota // Let Kafka decide based on group state
	StartFromLatest
	StartFromEarliest
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

	// TopicPrefix namespaces every topic this plugin produces to or consumes
	// from as "<prefix>-<topic>", so environments sharing a cluster don't
	// consume each other's messages. Empty means no prefix.
	TopicPrefix string

	// Authentication
	Username string
	Password string

	// TLS
	TLSEnabled bool

	// Producer settings
	RequiredAcks RequiredAcks
	Compression  Compression

	// Consumer settings
	CommitInterval time.Duration

	// Retry settings
	MaxRetries   int
	RetryBackoff time.Duration

	// Service settings
	EnableProducer bool

	// Key generation function
	KeyFunc func(topic string, payload any) []byte

	// TokenProvider returns an OAuth access token for SASL authentication.
	// Used by SASLOAUTHPlain to pass the token as the PLAIN password.
	TokenProvider func() (string, error)

	SASLMechanism sasl.Mechanism
}

// DefaultConfig returns a sensible default configuration
func DefaultConfig() Config {
	return Config{
		ReadMinBytes:   1024,
		ReadMaxBytes:   1048576,
		ReadMaxWait:    250 * time.Millisecond,
		WriteTimeout:   5 * time.Second,
		AllowAutoTopic: true,
		RequiredAcks:   AckAll, // Require all replicas
		Compression:    CompressionSnappy,
		CommitInterval: 0, // Manual commit
		MaxRetries:     3,
		RetryBackoff:   100 * time.Millisecond,
		EnableProducer: true,
	}
}

// topicName returns the topic as it exists on the broker (with prefix applied).
func (c Config) topicName(topic string) string {
	if c.TopicPrefix == "" {
		return topic
	}
	return c.TopicPrefix + "-" + topic
}

// trimTopicPrefix reverses topicName, returning the logical topic name.
func (c Config) trimTopicPrefix(topic string) string {
	if c.TopicPrefix == "" {
		return topic
	}
	// Slice rather than strings.TrimPrefix(topic, prefix+"-") so this runs
	// per consumed record without allocating.
	p := c.TopicPrefix
	if len(topic) <= len(p) || topic[len(p)] != '-' || topic[:len(p)] != p {
		return topic
	}
	return topic[len(p)+1:]
}

// Option configures the Kafka plugin
type Option func(*Config)

// WithWriteTimeout sets the write timeout
func WithWriteTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		c.WriteTimeout = timeout
	}
}

// WithTopicPrefix prefixes all topics as "<prefix>-<topic>" (e.g. "development-orders")
func WithTopicPrefix(prefix string) Option {
	return func(c *Config) {
		c.TopicPrefix = prefix
	}
}

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

// WithCommitInterval sets the commit interval
func WithCommitInterval(interval time.Duration) Option {
	return func(c *Config) {
		c.CommitInterval = interval
	}
}

// WithMaxRetries sets the maximum number of retries
func WithMaxRetries(maxRetries int) Option {
	return func(c *Config) {
		c.MaxRetries = maxRetries
	}
}

// WithRetryBackoff sets the retry backoff
func WithRetryBackoff(backoff time.Duration) Option {
	return func(c *Config) {
		c.RetryBackoff = backoff
	}
}

// WithUsername sets the username
func WithUsername(username string) Option {
	return func(c *Config) {
		c.Username = username
	}
}

// WithPassword sets the password
func WithPassword(password string) Option {
	return func(c *Config) {
		c.Password = password
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

// WithSASLMechanism sets a custom SASL authentication mechanism.
func WithSASLMechanism(mechanism sasl.Mechanism) Option {
	return func(c *Config) {
		c.SASLMechanism = mechanism
	}
}

// WithGCPOAuth configures OAUTHBEARER authentication for GCP Managed Kafka.
// This uses Google's default credentials (workload identity, service account, etc.).
func WithGCPOAuth() Option {
	return func(c *Config) {
		c.SASLMechanism = NewGCPOAuthMechanism()
		c.TLSEnabled = true // GCP Managed Kafka requires TLS
	}
}
