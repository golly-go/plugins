package main

import (
	"context"
	"log"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
)

// OrderCreated event
type OrderCreated struct {
	ID     string
	Amount float64
}

// Order aggregate
type Order struct {
	eventsource.AggregateBase
	ID     string
	Amount float64
}

func (o *Order) GetID() string {
	return o.ID
}

func (o *Order) ApplyOrderCreated(event OrderCreated) {
	o.ID = event.ID
	o.Amount = event.Amount
}

// OrderSummary projection
type OrderSummary struct {
	eventsource.ProjectionBase
	TotalOrders int
	TotalAmount float64
}

func (os *OrderSummary) HandleEvent(ctx *golly.Context, evt eventsource.Event) error {
	switch e := evt.Data.(type) {
	case OrderCreated:
		os.TotalOrders++
		os.TotalAmount += e.Amount
	}
	return nil
}

// CreateOrder command
type CreateOrder struct {
	ID     string
	Amount float64
}

func (c *CreateOrder) Perform(ctx *golly.Context, agg eventsource.Aggregate) error {
	order := agg.(*Order)
	order.Record(eventsource.Event{
		Type: "OrderCreated",
		Data: OrderCreated{
			ID:     c.ID,
			Amount: c.Amount,
		},
	})
	return nil
}

func main() {
	// Create engine with in-memory store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	// Register the Order aggregate
	engine.RegisterAggregate(&Order{}, []any{OrderCreated{}})

	// Create and register the OrderSummary projection
	summary := &OrderSummary{}
	engine.RegisterProjection(summary, eventsource.WithStreamName("orders"))

	// Create a new order
	order := &Order{ID: "order_1"}

	// Execute the CreateOrder command
	ctx := golly.NewContext(context.Background())
	cmd := &CreateOrder{ID: "order_1", Amount: 100.0}
	if err := engine.Execute(ctx, order, cmd); err != nil {
		log.Fatalf("Failed to execute command: %v", err)
	}

	// Print the projection state
	log.Printf("Total Orders: %d", summary.TotalOrders)
	log.Printf("Total Amount: %.2f", summary.TotalAmount)
}
