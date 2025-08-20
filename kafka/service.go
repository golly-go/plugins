package kafka

import (
	"time"

	"github.com/golly-go/golly"
)

// Service composes Kafka consumers and implements golly.Service lifecycle.
type Service struct {
	consumers *Consumers
	running   bool
	cfgFunc   func(app *golly.Application) Config
}

func NewService(opts ...Option) *Service {
	return &Service{consumers: NewConsumers(opts...)}
}

// Bus returns the underlying event bus (consumers implement Bus)
func (s *Service) Bus() *Consumers { return s.consumers }
func (s *Service) Name() string    { return "kafka-consumers" }

// Initialize builds consumers from app.Config(). Services and plugins are guaranteed to run before app initializers.
func (s *Service) Initialize(app *golly.Application) error {
	cfg := app.Config()

	opts := make([]Option, 0, 16)
	// Required cluster settings
	if bs := cfg.GetStringSlice("kafka.brokers"); len(bs) > 0 {
		opts = append(opts, func(c *Config) { c.Brokers = bs })
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

	// Auth
	if u := cfg.GetString("kafka.username"); u != "" {
		p := cfg.GetString("kafka.password")
		opts = append(opts, WithUserName(u), WithPassword(p))
	}

	// Merge into existing consumers instead of recreating so subscriptions persist
	if s.consumers == nil {
		s.consumers = NewConsumers()
	}
	if len(opts) > 0 {
		s.consumers.ApplyOptions(opts...)
	}

	// Allow dynamic config override (e.g., generate signer tokens)
	if s.cfgFunc != nil {
		s.consumers.cfg = s.cfgFunc(app)
	}
	return nil
}

func (s *Service) Start() error {
	if s.running {
		return nil
	}
	s.consumers.Start()
	s.running = true
	return nil
}

func (s *Service) Stop() error {
	if !s.running {
		return nil
	}
	s.consumers.Stop()
	s.running = false
	return nil
}

func (s *Service) IsRunning() bool { return s.running }

// Configure sets a function to produce a Config at Initialize time.
func (s *Service) Configure(f func(app *golly.Application) Config) { s.cfgFunc = f }
