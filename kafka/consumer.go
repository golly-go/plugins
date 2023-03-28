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

	cancel context.CancelFunc
}

func (cb *ConsumerBase) SetLogger(logger *logrus.Entry) { cb.logger = logger }
func (cb *ConsumerBase) Logger() *logrus.Entry          { return cb.logger }
func (cb *ConsumerBase) Running() bool                  { return cb.running }
func (cb *ConsumerBase) Pool() workers.Pool             { return cb.wp }

// Sensible defaults
func (cb *ConsumerBase) Config(ctx golly.Context) Config { return NewConfig(ctx.Config()) }

func (cb *ConsumerBase) Init(ctx golly.Context, consumer Consumer) error {
	return nil
}

func (cb *ConsumerBase) Run(ctx golly.Context, consumer Consumer) {
	defer func() {
		if r := recover(); r != nil {
			ctx.Logger().Errorln("panic in consumer run receive: ", r)
		}
	}()

	cb.running = true

	config := consumer.Config(ctx)

	cb.wp = workers.NewGenericPool(consumer.Name(),
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
			GroupTopics:           consumer.Topics(),
			Brokers:               config.Brokers,
			Partition:             config.Partition,
			MinBytes:              config.MinRead,
			MaxBytes:              config.MaxRead,
			MaxWait:               1 * time.Second,
			GroupID:               config.GroupID,
			Dialer:                dialer,
			WatchPartitionChanges: true,
			JoinGroupBackoff:      10 * time.Second,
			ReadBackoffMin:        250 * time.Millisecond,
			ReadBackoffMax:        10 * time.Second,

			GroupBalancers: []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}},
			ErrorLogger:    errorLogger{ctx.Logger()},
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
