package kafka

import (
	"context"
	"crypto/tls"
	errs "errors"
	"runtime/debug"
	"sync"
	"time"

	"github.com/golly-go/golly"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/sirupsen/logrus"
)

type Producer interface {
	Publish(gctx golly.Context, messages ...Message)
}

type KafkaPublisher struct {
	brokers []string

	ctx     golly.Context
	writers []*kafka.Writer
	logger  *logrus.Entry

	lock sync.RWMutex
	wg   sync.WaitGroup

	user     string
	password string

	running bool
}

func (k *KafkaPublisher) borrow() *kafka.Writer {
	k.lock.Lock()
	defer k.lock.Unlock()

	if !k.running {
		return nil
	}

	// Check if there are writers available
	if len(k.writers) == 0 {
		// Set a limit for retries
		for retries := 3; retries > 0; retries-- {
			writer := k.createProducer()
			if writer != nil {
				k.wg.Add(1)
				return writer
			}

			time.Sleep(100 * time.Nanosecond) // Wait before retrying
		}
		return nil
	}

	// If there are writers available, borrow one
	index := len(k.writers) - 1
	writer := k.writers[index]
	k.writers = k.writers[:index]

	k.wg.Add(1)

	return writer
}

func (k *KafkaPublisher) release(writer *kafka.Writer) {
	k.lock.Lock()
	defer k.lock.Unlock()

	k.writers = append(k.writers, writer)
	k.wg.Done()
}

func (k *KafkaPublisher) close() {
	k.lock.Lock()
	k.running = false
	k.lock.Unlock()

	// Wait for all active borrow operations to complete
	k.wg.Wait()

	k.lock.Lock()
	defer k.lock.Unlock()

	for _, writer := range k.writers {
		writer.Close()
	}

	k.writers = k.writers[:0]
}

func (k *KafkaPublisher) Publish(gctx golly.Context, messages ...Message) {
	writer := k.borrow()

	if writer == nil {
		gctx.Logger().Warnf("cannot get writer to send message")
		return
	}

	go func(ctx golly.Context, k *KafkaPublisher, writer *kafka.Writer) {
		defer k.release(writer)

		defer func() {
			if r := recover(); r != nil {
				k.logger.Errorf("Recovered from panic: %v", r)
				debug.PrintStack()
			}
		}()

		m := messages
		for retries := 0; retries < 3; retries++ {
			var err error = nil

			for pos, message := range m {
				err = writer.WriteMessages(gctx.Context(), kafka.Message{
					Topic: message.Topic,
					Key:   []byte(message.Key),
					Value: message.Marshal(),
				})

				if err != nil {
					k.logger.Warnf("Failed to publish message to %s (%s): %v", message.Topic, message.Key, err)
					m = m[pos:]
					break
				}
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
	}(gctx, k, writer)
}

func (k *KafkaPublisher) createProducer() *kafka.Writer {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(k.brokers...),
		Balancer:               &kafka.Murmur2Balancer{},
		AllowAutoTopicCreation: true,
		Async:                  false,
		ErrorLogger:            errorLogger{newKafkaLogger(k.logger, "producer")},
	}

	if k.user != "" && k.password != "" {
		w.Transport = &kafka.Transport{
			DialTimeout: 20 * time.Second,
			IdleTimeout: 45 * time.Second,
			TLS:         &tls.Config{MinVersion: tls.VersionTLS12},
			SASL: plain.Mechanism{
				Username: k.user,
				Password: k.password,
			},
		}
	}

	return w
}

func InitializerPublisher(app golly.Application) error {
	publisher = NewPublisher(app)

	golly.Events().Add(golly.EventAppShutdown, func(golly.Context, golly.Event) error {
		publisher.close()
		return nil
	})

	return nil
}

type NoOpProducer struct{}

func (NoOpProducer) Publish(golly.Context, ...Message) {}

// For now
func Publisher() Producer {
	if golly.Env().IsTest() {
		return NoOpProducer{}
	}
	return publisher
}

func NewPublisher(app golly.Application) *KafkaPublisher {
	return &KafkaPublisher{
		ctx: golly.NewContext(app.GoContext()),

		writers: []*kafka.Writer{},

		running: true,
		logger:  newKafkaLogger(golly.NewLogger(), "publisher"),

		brokers:  app.Config.GetStringSlice("kafka.address"),
		user:     app.Config.GetString("kafka.username"),
		password: app.Config.GetString("kafka.password"),
	}
}
