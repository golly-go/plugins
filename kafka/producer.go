package kafka

import (
	"context"
	"crypto/tls"
	errs "errors"
	"sync"
	"time"

	"github.com/golly-go/golly"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
)

type KafkaPublisher struct {
	config  Config
	ctx     golly.Context
	writers []*kafka.Writer
	logger  *logrus.Entry

	lock sync.RWMutex

	running bool
}

func (k *KafkaPublisher) borrow() *kafka.Writer {
	k.lock.Lock()
	defer k.lock.Unlock()

	if !k.running {
		return nil
	}

	if len(k.writers) == 0 {
		for {
			if writer := k.createProducer(k.ctx); writer != nil {
				return writer
			}
		}
	}

	index := len(k.writers) - 1
	writer := k.writers[index]

	k.writers = k.writers[:index]
	return writer
}

func (k *KafkaPublisher) release(writer *kafka.Writer) {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.writers = append(k.writers, writer)
}

func (k KafkaPublisher) close() {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.running = false
	for _, writer := range k.writers {
		writer.Close()
	}

	k.writers = k.writers[:0]
}

func (k KafkaPublisher) Publish(messages ...Message) {
	if !k.running {
		return
	}

	go func() {
		writer := k.borrow()
		defer k.release(writer)

		m := messages
		for retries := 0; retries < 3; retries++ {
			var err error = nil

			for pos, message := range m {
				err = writer.WriteMessages(k.ctx.Context(), kafka.Message{
					Topic: message.Topic,
					Key:   []byte(message.Key),
					Value: message.Marshal(),
				})

				if err == nil {
					k.logger.Debugf("published message to %s (%s)", message.Topic, message.Key)
					break
				}
				m = messages[:pos]
			}

			if err == nil {
				break
			}

			if errs.Is(err, kafka.LeaderNotAvailable) || errs.Is(err, context.DeadlineExceeded) {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			time.Sleep(time.Millisecond * 500)
			k.logger.Errorf("unable to write kafka message: %v (retries %d)", err, retries)
		}
	}()
}

func (k *KafkaPublisher) createProducer(ctx golly.Context) *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(k.config.Brokers...),
		Balancer:               &kafka.Murmur2Balancer{},
		Logger:                 ctx.Logger(),
		AllowAutoTopicCreation: true,
		ErrorLogger:            errorLogger{newKafkaLogger(k.logger, "producer")},
	}

	if k.config.Username != "" && k.config.Password != "" {
		w.Transport = &kafka.Transport{
			DialTimeout: 20 * time.Second,
			TLS:         &tls.Config{MinVersion: tls.VersionTLS12},
			SASL: plain.Mechanism{
				Username: k.config.Username,
				Password: k.config.Password,
			},
		}
	}

	return w
}

func InitializerPublisher(app golly.Application) error {
	InitDefaultConfig(app.Config)

	publisher = NewPublisher(app)

	golly.Events().Add(golly.EventAppShutdown, func(golly.Context, golly.Event) error {
		publisher.close()
		return nil
	})

	return nil
}

func Publisher() *KafkaPublisher { return publisher }

func Publish(messages ...Message) {
	publisher.Publish(messages...)
}

func NewPublisher(app golly.Application) *KafkaPublisher {
	return &KafkaPublisher{
		ctx:     golly.NewContext(app.GoContext()),
		writers: []*kafka.Writer{},
		running: true,
		logger:  newKafkaLogger(app.Logger, "publisher"),
		config:  NewConfig(app.Config),
	}
}
