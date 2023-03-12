package kafka

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
	"github.com/spf13/viper"
)

var consumers []Consumer

type ConsumerService struct {
	golly.ServiceBase

	running bool
	quit    chan struct{}
}

func NewConsumerService() *ConsumerService {
	return &ConsumerService{
		quit:    make(chan struct{}),
		running: false,
	}
}

func (*ConsumerService) Name() string     { return "consumers" }
func (cs *ConsumerService) Running() bool { return cs.running }
func (cs *ConsumerService) Quit() {
	cs.running = false
	close(cs.quit)
}

func (cs *ConsumerService) Initialize(app golly.Application) error {
	InitConsumerDefaultConfig(app.Config)
	return nil
}

func (cs *ConsumerService) Run(ctx golly.Context) error {
	for _, consumer := range consumers {
		ctx.Logger().Infof("Starting consumer: %s [%s]", utils.GetTypeWithPackage(consumer), consumer.Topics())

		go consumer.Run(ctx, consumer)
	}

	ctx.Logger().Debugf("started consumers - waiting for quit")
	<-cs.quit
	cs.running = false

	ctx.Logger().Debugf("stopping consumers")

	for _, consumer := range consumers {
		consumer.Stop(ctx)
	}

	ctx.Logger().Debugf("waiting for consumers")

	for _, consumer := range consumers {
		consumer.Wait(ctx)
	}

	return nil
}

func ConsumerPreBoot() error {
	golly.RegisterServices(NewConsumerService())
	return nil
}

func InitConsumerDefaultConfig(config *viper.Viper) {
	config.SetDefault("kafka.consumer", map[string]interface{}{
		"workers": map[string]interface{}{
			"min": 1,
			"max": 25,
			// "buffer": 2_500,
			"buffer": 50,
		},
		"bytes": map[string]int{
			"min": 10_000, // 10e3,
			"max": 10_000_000,
		},
		"partition": 0,
		"wait":      "50ms",
		"brokers":   []string{"localhost:9092"},
		"group_id":  "default-group",
	})
}

// DefineConsumers - defines the consumers in the system,
// we should not be calling this twice, but guard against it
func RegisterConsumers(definitions ...Consumer) golly.GollyAppFunc {
	return func(golly.Application) error {
		consumers = definitions
		return nil
	}
}
