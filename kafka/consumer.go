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

type Consumer interface {
	Name() string

	Topics() []string

	Handler(golly.Context, Message) error
	Config(golly.Context) Config
	Init(golly.Context, Consumer) error

	Run(golly.Context, Consumer)
	Stop(golly.Context)
	Wait(golly.Context)

	Logger() *logrus.Entry
	SetLogger(logger *logrus.Entry)

	Reader(golly.Context, Consumer) *kafka.Reader

	Running() bool

	Pool() workers.Pool
}

type ConsumerBase struct {
	running bool
	wp      workers.Pool
	logger  *logrus.Entry

	done chan struct{}
}

func (cb *ConsumerBase) SetLogger(logger *logrus.Entry) { cb.logger = logger }
func (cb *ConsumerBase) Logger() *logrus.Entry          { return cb.logger }
func (cb *ConsumerBase) Running() bool                  { return cb.running }
func (cb *ConsumerBase) Pool() workers.Pool             { return cb.wp }

// Sensible defaults
func (cb *ConsumerBase) Config(ctx golly.Context) Config { return NewConfig(ctx.Config()) }

func (cb *ConsumerBase) Init(ctx golly.Context, consumer Consumer) error {
	name := consumer.Name()
	cb.running = true

	config := consumer.Config(ctx)

	logger := newKafkaLogger(ctx.Logger(), "consumers").WithField("name", name)

	consumer.SetLogger(logger)

	cb.wp = workers.NewGernicPool(consumer.Name(),
		int32(config.MinPool),
		int32(config.MaxPool),
		wrap(consumer.Handler),
	)

	return nil
}

func (cb *ConsumerBase) Run(ctx golly.Context, consumer Consumer) {
	logger := consumer.Logger()

	logger.Debugf("consumer %s topics: %#v", consumer.Name(), consumer.Topics())

	cb.done = make(chan struct{})

	go cb.wp.Spawn(ctx)

	reader := consumer.Reader(ctx, consumer)

	reattempts := 0

	for cb.running && reattempts <= 3 {
		goCtx, _ := context.WithCancel(ctx.Context())

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

		consumer.Pool().EnQueueAsync(ctx, Message{
			Topic:   m.Topic,
			Data:    m.Value,
			Key:     string(m.Key),
			Time:    m.Time,
			Headers: m.Headers,
		})
	}

	cb.running = false
	reader.Close()
	close(cb.done)
}

func (cb *ConsumerBase) Reader(ctx golly.Context, consumer Consumer) *kafka.Reader {
	config := consumer.Config(ctx)

	var dialer *kafka.Dialer = &kafka.Dialer{
		Timeout:   config.Timeouts.Connection,
		DualStack: true,
		KeepAlive: 1 * time.Second,
	}

	if config.Username != "" {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
		dialer.SASLMechanism = plain.Mechanism{
			Username: config.Username,
			Password: config.Password,
		}
	}

	return kafka.NewReader(
		kafka.ReaderConfig{
			GroupTopics:    consumer.Topics(),
			Brokers:        config.Brokers,
			Partition:      config.Partition,
			MinBytes:       config.MinRead,
			MaxBytes:       config.MaxRead,
			MaxWait:        5 * time.Second,
			GroupID:        config.GroupID,
			Dialer:         dialer,
			GroupBalancers: []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}},
			ErrorLogger:    errorLogger{consumer.Logger()},
		},
	)
}

func (cb *ConsumerBase) Stop(ctx golly.Context) {
	defer func(t time.Time) {
		cb.logger.Infof("stopped %s consumer in %#v\n", cb.wp.Name(), time.Since(t).String())
	}(time.Now())

	cb.running = false
	cb.wp.Stop()

	<-cb.done
}

func (cb *ConsumerBase) Wait(ctx golly.Context) {
	cb.wp.Wait()
	ctx.Logger().Debugf("consumer %s stopped", cb.wp.Name())
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
