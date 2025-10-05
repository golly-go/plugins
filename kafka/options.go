package kafka

import (
	"strings"
	"time"

	"github.com/golly-go/golly"
)

type SASLMechanism string

const (
	PLAIN       SASLMechanism = "PLAIN"
	SCRAM       SASLMechanism = "SCRAM"
	OAUTHBEARER SASLMechanism = "OAUTHBEARER"
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

	TLSEnabled bool

	StartFromLatest      bool // if true, use kafka.LastOffset for new groups
	CooperativeBalancing bool // reserved for future

	KeyFunc KeyFunc // optional

	SASLMechanism SASLMechanism

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

func WithTLSEnabled() Option {
	return func(cfg *Config) { cfg.TLSEnabled = true }
}

func WithAllowAutoTopic() Option {
	return func(cfg *Config) { cfg.AllowAutoTopic = true }
}

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

func WithCredentials(userName string, password string) Option {
	return func(cfg *Config) {
		cfg.UserName = userName
		cfg.Password = password
	}
}

func WithSASLMechanism(mechanism SASLMechanism) Option {
	return func(cfg *Config) {
		cfg.SASLMechanism = mechanism
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

func ConfigFromApp(app *golly.Application) Config {

	var opts []Option

	cfg := app.Config()
	if s := cfg.GetString("kafka.brokers"); s != "" {
		parts := strings.Split(s, ",")

		brokers := make([]string, 0, len(parts))
		for _, p := range parts {
			if v := strings.TrimSpace(p); v != "" {
				brokers = append(brokers, v)
			}
		}

		opts = append(opts, WithBrokers(brokers))
	}

	if g := cfg.GetString("kafka.group_id"); g != "" {
		opts = append(opts, func(c *Config) { c.GroupID = g })
	}
	if id := cfg.GetString("kafka.client_id"); id != "" {
		opts = append(opts, func(c *Config) { c.ClientID = id })
	}

	// Behavior flags
	if cfg.GetBool("kafka.cooperative") {
		opts = append(opts, WithCooperativeBalancing())
	}
	if cfg.GetBool("kafka.start_latest") {
		opts = append(opts, WithStartFromLatest())
	}
	// Tuning
	if n := cfg.GetInt("kafka.read_min_bytes"); n > 0 {
		opts = append(opts, WithReadMinBytes(n))
	}
	if n := cfg.GetInt("kafka.read_max_bytes"); n > 0 {
		opts = append(opts, WithReadMaxBytes(n))
	}
	if d := cfg.GetDuration("kafka.read_max_wait"); d > 0 {
		opts = append(opts, WithReadMaxWait(d))
	}
	if d := cfg.GetDuration("kafka.commit_interval"); d > 0 {
		opts = append(opts, WithCommitInterval(d))
	}
	if d := cfg.GetDuration("kafka.write_timeout"); d > 0 {
		opts = append(opts, WithWriteTimeout(d))
	} else {
		opts = append(opts, WithWriteTimeout(5*time.Second))
	}

	if d := cfg.GetDuration("kafka.write_timeout"); d > 0 {
		opts = append(opts, WithWriteTimeout(d))
	} else {
		opts = append(opts, WithWriteTimeout(5*time.Second))
	}

	// Auth
	if u := cfg.GetString("kafka.username"); u != "" {
		p := cfg.GetString("kafka.password")
		opts = append(opts, WithCredentials(u, p))
	}

	config := Config{}
	for _, o := range opts {
		o(&config)
	}

	return config
}
