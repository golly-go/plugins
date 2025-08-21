package kafka

import (
	"time"
)

type KeyFunc func(topic string, payload any) []byte

type Config struct {
	Brokers        []string
	GroupID        string
	ClientID       string
	ReadMinBytes   int           // default: 1e4
	ReadMaxBytes   int           // default: 1e6
	ReadMaxWait    time.Duration // default: 250ms
	CommitInterval time.Duration // default: 0 (manual)
	WriteTimeout   time.Duration // default: 5s
	AllowAutoTopic bool          // if your cluster auto-creates topics

	StartFromLatest      bool // if true, use kafka.LastOffset for new groups
	CooperativeBalancing bool // reserved for future

	KeyFunc KeyFunc // optional

	// Optional constructor hooks
	ReaderFunc ReaderFunc
	WriterFunc WriterFunc

	UserName string
	Password string

	// Publish retry policy
	PublishMaxRetries int
	PublishBackoff    time.Duration
}

type Option func(*Config)

type ReaderFunc func(topics []string, groupID string) readerIface

type WriterFunc func() writerIface

// Subscription options

type SubConfig struct {
	Topics       []string
	GroupID      string
	DeriveGroup  bool
	DerivePrefix string
}

type SubOption func(*SubConfig)

func SubscribeWithTopic(topic string) SubOption {
	return func(sc *SubConfig) { sc.Topics = []string{topic} }
}

func SubscribeWithTopics(topics ...string) SubOption {
	return func(sc *SubConfig) { sc.Topics = append([]string(nil), topics...) }
}

func SubscribeWithGroupID(groupID string) SubOption {
	return func(sc *SubConfig) { sc.GroupID = groupID }
}

// SubscribeWithDerivedGroupID requests deriving a stable group ID from handler and topics
// Optional prefix lets teams namescape their groups (e.g., "billing").
func SubscribeWithDerivedGroupID(prefix string) SubOption {
	return func(sc *SubConfig) {
		sc.DeriveGroup = true
		sc.DerivePrefix = prefix
	}
}

func WithBrokers(brokers []string) Option {
	return func(cfg *Config) { cfg.Brokers = brokers }
}

func WithGroupID(groupID string) Option {
	return func(cfg *Config) { cfg.GroupID = groupID }
}

func WithClientID(clientID string) Option {
	return func(cfg *Config) { cfg.ClientID = clientID }
}

func WithReaderFunc(factory ReaderFunc) Option {
	return func(cfg *Config) { cfg.ReaderFunc = factory }
}

func WithWriterFunc(factory WriterFunc) Option {
	return func(cfg *Config) { cfg.WriterFunc = factory }
}

func WithKeyFunc(keyFunc KeyFunc) Option {
	return func(cfg *Config) {
		cfg.KeyFunc = keyFunc
	}
}

func WithReadMinBytes(readMinBytes int) Option {
	return func(cfg *Config) {
		cfg.ReadMinBytes = readMinBytes
	}
}

func WithReadMaxBytes(readMaxBytes int) Option {
	return func(cfg *Config) {
		cfg.ReadMaxBytes = readMaxBytes
	}
}

func WithReadMaxWait(readMaxWait time.Duration) Option {
	return func(cfg *Config) {
		cfg.ReadMaxWait = readMaxWait
	}
}

func WithCommitInterval(commitInterval time.Duration) Option {
	return func(cfg *Config) {
		cfg.CommitInterval = commitInterval
	}
}

func WithWriteTimeout(writeTimeout time.Duration) Option {
	return func(cfg *Config) {
		cfg.WriteTimeout = writeTimeout
	}
}

func WithUserName(userName string) Option {
	return func(cfg *Config) {
		cfg.UserName = userName
	}
}

func WithPassword(password string) Option {
	return func(cfg *Config) {
		cfg.Password = password
	}
}

func WithStartFromLatest() Option {
	return func(cfg *Config) {
		cfg.StartFromLatest = true
	}
}

func WithCooperativeBalancing() Option {
	return func(cfg *Config) {
		cfg.CooperativeBalancing = true
	}
}

func WithoutAutoTopic() Option {
	return func(cfg *Config) {
		cfg.AllowAutoTopic = false
	}
}

// WithPublishRetries sets the maximum number of retries for transient publish errors
func WithPublishRetries(maxRetries int) Option {
	return func(cfg *Config) { cfg.PublishMaxRetries = maxRetries }
}

// WithPublishBackoff sets the initial backoff used for publish retries
func WithPublishBackoff(backoff time.Duration) Option {
	return func(cfg *Config) { cfg.PublishBackoff = backoff }
}
