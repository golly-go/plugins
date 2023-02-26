package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
	"github.com/sirupsen/logrus"
	"github.com/slimloans/golly"
)

var (
	publisher *KafkaPublisher
)

type KafkaPublisher struct {
	ctx    context.Context
	w      *kafka.Writer
	write  chan Message
	logger *logrus.Entry
}

type Message struct {
	Topic   string
	Key     string
	Data    interface{}
	Time    time.Time
	Headers []protocol.Header
}

func Publish(topic, id string, data interface{}) {
	publisher.write <- Message{Topic: topic, Key: id, Data: data}
}

func Publisher() *KafkaPublisher { return publisher }

func (k KafkaPublisher) Publisher() {
	k.logger.Debug("starting publisher thread")

	for message := range k.write {
		k.logger.Debugf("[Message]: %#v", message)

		b, _ := json.Marshal(message.Data)
		key := []byte(message.Key)

		for retries := 0; retries < 3; retries++ {

			err := k.w.WriteMessages(k.ctx, kafka.Message{
				Topic: message.Topic,
				Key:   key,
				Value: b,
			})

			if err == nil {
				k.logger.Debugf("published message to %s (%s)", message.Topic, key)
				break
			}

			if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
				time.Sleep(time.Millisecond * 100)
				continue
			}

			k.logger.Errorf("unable to write kafka message: %v (retries %d)", err, retries)
		}
	}
}

func (k KafkaPublisher) close() { close(k.write) }

func NewPublisher(app golly.Application) *KafkaPublisher {
	var l = golly.NewLogger()
	l.Logger.SetLevel(logrus.WarnLevel)

	k := &KafkaPublisher{
		ctx:    app.GoContext(),
		write:  make(chan Message, 100),
		logger: l,
		w: &kafka.Writer{
			Addr:                   kafka.TCP(app.Config.GetString("KAFKA_ADDRESS")),
			Balancer:               &kafka.LeastBytes{},
			Logger:                 l,
			AllowAutoTopicCreation: true,
		},
	}

	return k
}

func InitializerPublisher(app golly.Application) error {
	publisher = NewPublisher(app)

	golly.Events().Add(golly.EventAppShutdown, func(golly.Context, golly.Event) error {
		publisher.close()
		return nil
	})

	go publisher.Publisher()

	return nil
}
