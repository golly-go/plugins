package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
	"github.com/golly-go/plugins/workers"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
)

type Consumer interface {
	Topics() []string
	Handler(golly.Context, Message) error
	Config(golly.Context) Config

	Run(golly.Context, Consumer)
	Stop(golly.Context)
	Wait(golly.Context)
	Logger() *logrus.Entry
	SetLogger(logger *logrus.Entry)
}

type ConsumerBase struct {
	running bool
	wp      *workers.WorkerPool
	logger  *logrus.Entry

	done chan struct{}
}

func (cb *ConsumerBase) SetLogger(logger *logrus.Entry) { cb.logger = logger }
func (cb *ConsumerBase) Logger() *logrus.Entry          { return cb.logger }

// Sensible defaults
func (cb *ConsumerBase) Config(ctx golly.Context) Config { return NewConfig(ctx.Config()) }

func (cb *ConsumerBase) Run(ctx golly.Context, consumer Consumer) {
	cb.running = true
	name := utils.GetTypeWithPackage(consumer)
	config := consumer.Config(ctx)

	cb.wp = workers.NewPool(name, config.MinPool, config.MaxPool, config.JobsBuffer, wrap(consumer.Handler))

	logger := newKafkaLogger(ctx.Logger(), "consumers").
		WithField("name", cb.wp.Name)

	consumer.SetLogger(logger)

	logger.Debugf("consumer %s topics: %#v", name, consumer.Topics())

	cb.done = make(chan struct{})

	go cb.wp.Spawn(ctx)

	reader := newReader(ctx, consumer)

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

		cb.wp.C <- Message{
			Topic:   m.Topic,
			Data:    m.Value,
			Key:     string(m.Key),
			Time:    m.Time,
			Headers: m.Headers,
		}

	}

	reader.Close()
	close(cb.done)
}

func newReader(ctx golly.Context, consumer Consumer) *kafka.Reader {
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
			// ErrorLogger:    errorLogger{consumer.Logger()},
		},
	)
}

func (cb *ConsumerBase) Stop(ctx golly.Context) {
	defer func(t time.Time) {
		cb.logger.Infof("stopped %s consumer in %#v\n", cb.wp.Name, time.Since(t).String())
	}(time.Now())

	cb.running = false
	cb.wp.Stop()

	<-cb.done
}

func (cb *ConsumerBase) Wait(ctx golly.Context) {
	cb.wp.Wait()
	ctx.Logger().Debugf("consumer %s stopped", cb.wp.Name)
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
