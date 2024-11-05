package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/workers"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
)

type ConsumerConfig interface {
	Brokers(gctx golly.Context) []string
	UserName(gctx golly.Context) string
	Password(gctx golly.Context) string
	UseErrorLogger() bool
	Group(ctx golly.Context) string
	MaxPool() int
	MinPool() int
	BufferSize() int
	Partition() int
	MinRead() int
	MaxRead() int
	StartOffset() int64
	BalanceStrategy() kafka.Balancer
	IdleTimeout() time.Duration
	ConnectTimeout() time.Duration
	Retries() int
}

type Consumer interface {
	ConsumerConfig

	Name() string

	Topics() []string

	Handler(golly.Context, Message) error
	Config(golly.Context) Config
	Init(golly.Context, Consumer) error

	Run(golly.Context, Consumer)
	Stop(golly.Context)
	Wait(golly.Context)
	Group(golly.Context) string

	Logger() *logrus.Entry
	SetLogger(logger *logrus.Entry)

	Reader(golly.Context, Consumer) *kafka.Reader

	Running() bool

	Pool() *workers.Pool
}

type ConsumerBase struct {
	running bool
	wp      *workers.Pool
	logger  *logrus.Entry

	done chan struct{}

	cancel context.CancelFunc
}

// These should be configured via ENV
func (cb *ConsumerBase) Brokers(gctx golly.Context) []string {
	return gctx.Config().GetStringSlice("kafka.address")
}
func (cb *ConsumerBase) UserName(gctx golly.Context) string {
	return gctx.Config().GetString("kafka.username")
}
func (cb *ConsumerBase) Password(gctx golly.Context) string {
	return gctx.Config().GetString("kafka.password")
}

func (cb *ConsumerBase) UseErrorLogger() bool           { return true }
func (cb *ConsumerBase) SetLogger(logger *logrus.Entry) { cb.logger = logger }
func (cb *ConsumerBase) Logger() *logrus.Entry          { return cb.logger }
func (cb *ConsumerBase) Running() bool                  { return cb.running }
func (cb *ConsumerBase) Pool() *workers.Pool            { return cb.wp }

func (cb *ConsumerBase) Group(ctx golly.Context) string { return "default-group" }
func (cb *ConsumerBase) MaxPool() int                   { return 10 }
func (cb *ConsumerBase) MinPool() int                   { return 1 }
func (cb *ConsumerBase) BufferSize() int                { return 50 }

func (cb *ConsumerBase) Partition() int { return 0 }

func (cb *ConsumerBase) MinRead() int                    { return 10_000 }
func (cb *ConsumerBase) MaxRead() int                    { return 10_000_000 }
func (cb *ConsumerBase) StartOffset() int64              { return kafka.FirstOffset }
func (cb *ConsumerBase) BalanceStrategy() kafka.Balancer { return kafka.Murmur2Balancer{} }

func (cb *ConsumerBase) IdleTimeout() time.Duration    { return 45 * time.Second }
func (cb *ConsumerBase) ConnectTimeout() time.Duration { return 10 * time.Second }

func (cb *ConsumerBase) Retries() int { return 3 }

func (cb *ConsumerBase) Init(ctx golly.Context, consumer Consumer) error {
	return nil
}

func (cb *ConsumerBase) Run(ctx golly.Context, consumer Consumer) {
	defer func(t time.Time) {
		if r := recover(); r != nil {
			ctx.Logger().Errorln("panic in consumer run receive: ", r)
		}
		ctx.Logger().Debugf("stoping consumer %s after %v", consumer.Name(), time.Since(t))

	}(time.Now())

	cb.running = true

	config := consumer.Config(ctx)

	cb.wp = workers.NewPool(consumer.Name(),
		int32(config.MinPool),
		int32(config.MaxPool),
		wrap(consumer.Handler),
	)

	logger := newKafkaLogger(ctx.Logger(), "consumers").
		WithFields(logrus.Fields{
			"spawner":        cb.wp.Name(),
			"consumer.name":  consumer.Name(),
			"consumer.hosts": config.Brokers[0],
		})

	logger.Debugf("starting consumer %s topics: %#v", consumer.Name(), consumer.Topics())

	consumer.SetLogger(logger)

	cb.done = make(chan struct{})

	go cb.wp.Run(ctx)

	reader := consumer.Reader(ctx, consumer)
	defer reader.Close()

	reattempts := 0

	for cb.running && reattempts <= 3 {
		goCtx, cancel := context.WithCancel(ctx.Context())
		cb.cancel = cancel

		logger.Debug("Fetching kafka messages")

		m, err := reader.ReadMessage(goCtx)
		// Do we break here if the error occurs or should we retry 3 times?
		if err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}

			if e, ok := err.(kafka.Error); ok {
				reattempts++

				logger.Errorf("%s: when when fetching messages: %v (temporary: %#v) (timeout: %#v)", e.Title(), e.Description(), e.Temporary(), e.Timeout())

				if e.Temporary() {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				continue
			}

			logger.Errorf("error when when fetching messages: %v", err.Error())
			break
		}

		reattempts = 0

		consumer.Logger().Debugf("received message: %s (%s)", m.Topic, string(m.Value))

		consumer.Pool().EnQueue(ctx, Message{
			Topic:   m.Topic,
			Data:    m.Value,
			Key:     string(m.Key),
			Time:    m.Time,
			Headers: m.Headers,
		})

		consumer.Logger().Debugf("loop %s (%s)", m.Topic, string(m.Value))

	}

	cb.running = false

	close(cb.done)

}

func (cb *ConsumerBase) Reader(ctx golly.Context, consumer Consumer) *kafka.Reader {
	brokers := consumer.Brokers(ctx)

	if len(brokers) == 0 {
		ctx.Logger().Error("Brokers not defined for consumer or in env")
		panic("brokers set in consumer or kafka.address array")
	}

	var dialer *kafka.Dialer = &kafka.Dialer{
		Timeout:   consumer.ConnectTimeout(),
		DualStack: true,
		KeepAlive: 1 * time.Second,
	}

	user := consumer.UserName(ctx)
	password := consumer.Password(ctx)

	if user != "" {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		dialer.SASLMechanism = plain.Mechanism{
			Username: user,
			Password: password,
		}
	}

	return kafka.NewReader(
		kafka.ReaderConfig{
			GroupTopics:           consumer.Topics(),
			Brokers:               consumer.Brokers(ctx),
			Partition:             consumer.Partition(),
			MinBytes:              consumer.MinRead(),
			MaxBytes:              consumer.MaxRead(),
			MaxWait:               1 * time.Second,
			GroupID:               consumer.Group(ctx),
			Dialer:                dialer,
			WatchPartitionChanges: true,
			JoinGroupBackoff:      10 * time.Second,
			ReadBackoffMin:        250 * time.Millisecond,
			ReadBackoffMax:        10 * time.Second,
			StartOffset:           consumer.StartOffset(),
			GroupBalancers:        []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}},
			// ErrorLogger:           logger,
		},
	)
}

func (cb *ConsumerBase) Stop(ctx golly.Context) {
	defer func(t time.Time) {
		cb.logger.Infof("stopped %s consumer in %#v", cb.wp.Name(), time.Since(t).String())
		if r := recover(); r != nil {
			cb.logger.Errorln("panic in stop receive: ", r)
		}
	}(time.Now())

	cb.running = false

	cb.logger.Debugln("stopping workerpool")

	cb.wp.Stop()

	cb.logger.Debugln("waiting for done signal")

	cb.cancel()

	<-cb.done
}

func (cb *ConsumerBase) Wait(ctx golly.Context) {
	cb.wp.Wait()
	cb.logger.Debugf("consumer %s stopped", cb.wp.Name())
}

func wrap(h func(golly.Context, Message) error) workers.WorkerFunc {
	return func(ctx golly.Context, data interface{}) error {
		if m, ok := data.(Message); ok {
			return h(ctx, m)
		}
		return fmt.Errorf("cannot convert to kafka message: %#v", data)
	}
}

/*

Consumers
    event.mytopic
	   - consumerA
	   - consumerB
	   - consumerC


DefineConsumers(
	&something.MyConsumer{},
	&something.MyConsumer2{},
)


DefineConsumers(map[string][]Consumer{
	"events.users": {
		&something.MyConsumer,
	}
})

*/
