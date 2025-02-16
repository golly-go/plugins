package main

import (
	"log"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
)

func main() {
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	// Subscribe to specific event types using the default stream
	engine.Subscribe("OrderCreated", func(ctx *golly.Context, evt eventsource.Event) {
		log.Printf("Handling order created event: %s", evt.Type)
	})

	engine.Subscribe("OrderUpdated", func(ctx *golly.Context, evt eventsource.Event) {
		log.Printf("Handling order updated event: %s", evt.Type)
	})

	// Subscribe to aggregate type
	engine.Subscribe("Customer", func(ctx *golly.Context, evt eventsource.Event) {
		log.Printf("Handling customer event: %s", evt.Type)
	})
}
