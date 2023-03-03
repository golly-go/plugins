package kafka

import (
	"fmt"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
	"github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	MinPool    int
	MaxPool    int
	JobsBuffer int

	Brokers   []string
	Partition int

	MinRead int
	MaxRead int

	GroupID string
}

type Consumer interface {
	Topics() []string
	Handler(golly.Context, Message) error
	Config(golly.Context) ConsumerConfig

	Run(golly.Context, Consumer)
	Stop(golly.Context)
	Wait(golly.Context)
}

type ConsumerBase struct {
	running bool

	wp *WorkerPool
}

func wrap(h func(golly.Context, Message) error) WorkerFunc {
	return func(ctx golly.Context, data interface{}) error {
		if m, ok := data.(Message); ok {
			return h(ctx, m)
		}
		return fmt.Errorf("cannot convert to kafka message: %#v", data)
	}
}

func (cb *ConsumerBase) Stop(ctx golly.Context) {
	cb.running = false

	cb.wp.Stop()
}

func (cb *ConsumerBase) Wait(ctx golly.Context) {
	cb.wp.Wait()

	ctx.Logger().Debugf("consumer %s stopped", cb.wp.Name)
}

func (cb *ConsumerBase) Run(ctx golly.Context, consumer Consumer) {
	cb.running = true
	name := utils.GetTypeWithPackage(consumer)
	config := consumer.Config(ctx)

	ctx.Logger().Debugf("consumer %s config %#v", name, consumer.Config(ctx))

	cb.wp = NewPool(name, config.MinPool, config.MaxPool, config.JobsBuffer, wrap(consumer.Handler))

	go cb.wp.Spawn(ctx)

	reader := kafka.NewReader(kafka.ReaderConfig{
		GroupTopics: consumer.Topics(),
		Brokers:     config.Brokers,
		Partition:   config.Partition,
		MinBytes:    config.MinRead,
		MaxBytes:    config.MaxRead,
		MaxWait:     10 * time.Millisecond,
		GroupID:     config.GroupID,
	})

	goCtx := ctx.Context()

	for cb.running {
		m, err := reader.FetchMessage(goCtx)

		// Do we break here if the error occurs or should we retry 3 times?
		if err != nil {
			break
		}

		cb.wp.C <- Message{
			Topic:   m.Topic,
			Data:    m.Value,
			Key:     string(m.Key),
			Time:    m.Time,
			Headers: m.Headers,
		}

		if err := reader.CommitMessages(goCtx, m); err != nil {
			ctx.Logger().Errorf("error when commiting messages: %v", err)
			break
		}
	}
}

// Sensible defaults
func (cb *ConsumerBase) Config(ctx golly.Context) ConsumerConfig {
	config := ctx.Config()

	return ConsumerConfig{
		// Pool Config
		MinPool:    config.GetInt("kafka.consumer.workers.min"),
		MaxPool:    config.GetInt("kafka.consumer.workers.max"),
		JobsBuffer: config.GetInt("kafka.consumer.workers.buffer"),

		// Kafka Config
		Brokers:   config.GetStringSlice("kafka.consumer.brokers"),
		Partition: config.GetInt("kafka.consumer.partition"),
		MinRead:   10e3, // 10e3, // 10KB
		MaxRead:   10e6, // 10MB

		GroupID: config.GetString("kafka.consumer.group_id"),
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
