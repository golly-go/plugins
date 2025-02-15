package main

import (
	"context"
	"log"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
)

// Domain Events
type OrderCreated struct {
	ID         string
	CustomerID string
	Amount     float64
}

func main() {
	// Create engine with an event store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	// Register event handler
	engine.Subscribe("orders", "OrderCreated", func(ctx *golly.Context, evt eventsource.Event) {
		order := evt.Data.(OrderCreated)
		log.Printf("Order created: %s for customer %s", order.ID, order.CustomerID)
	})

	// Send events through the engine
	ctx := golly.NewContext(context.Background())

	engine.Send(ctx, "orders", eventsource.Event{
		Type: "OrderCreated",
		Data: OrderCreated{
			ID:         "order_123",
			CustomerID: "cust_456",
			Amount:     99.99,
		},
	})
}
