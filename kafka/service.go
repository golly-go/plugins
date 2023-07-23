package kafka

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
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
	InitDefaultConfig(app.Config)

	cs.RunSideCar(app, "status-endpoint-service")

	return nil
}

func (cs *ConsumerService) Run(ctx golly.Context) error {
	for _, consumer := range consumers {
		go func(ctx golly.Context, consumer Consumer) {
			ctx.Logger().Infof("Starting consumer: %s [%s]", utils.GetTypeWithPackage(consumer), consumer.Topics())

			consumer.Init(ctx, consumer)
			consumer.Run(ctx, consumer)

		}(ctx, consumer)
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

// DefineConsumers - defines the consumers in the system,
// we should not be calling this twice, but guard against it
func RegisterConsumers(definitions ...Consumer) golly.GollyAppFunc {
	return func(golly.Application) error {
		consumers = append(consumers, definitions...)
		return nil
	}
}
