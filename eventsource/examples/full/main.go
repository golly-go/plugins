package main

import (
	"context"
	"fmt"
	"os"
	"time"

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

func (o *Order) OrderCreatedHandler(evt eventsource.Event) {
	event := evt.Data.(OrderCreated)

	fmt.Printf("OrderCreatedHandler: %#v\n", event)

	o.ID = event.ID
	o.Amount = event.Amount
}

// or  conversly
// func (o *Order) Apply(evt eventsource.Event) {
// 	switch event := evt.Data.(type) {
// 	case OrderCreated:
// 		o.ID = event.ID
// 		o.Amount = event.Amount
// 	}
// }

// OrderSummary projection
type OrderSummary struct {
	eventsource.ProjectionBase

	TotalOrders int
	TotalAmount float64
}

func (*OrderSummary) AggregateTypes() []any {
	return []any{&Order{}}
}

func (os *OrderSummary) HandleEvent(evt eventsource.Event) error {
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

func (c CreateOrder) Perform(ctx *golly.Context, agg eventsource.Aggregate) error {
	agg.Record(OrderCreated{
		ID:     c.ID,
		Amount: c.Amount,
	})

	return nil
}

func main() {
	// Create engine with in-memory store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	engine.Start()
	defer engine.Stop()

	// Register the Order aggregate
	engine.RegisterAggregate(&Order{}, []any{OrderCreated{}})

	// Create and register the OrderSummary projection
	summary := &OrderSummary{}
	engine.RegisterProjection(summary)

	// Create a new order
	order := &Order{ID: "order_1"}

	// Execute the CreateOrder command
	ctx := golly.NewContext(context.Background())

	cmd := CreateOrder{ID: "order_1", Amount: 100.0}

	if err := engine.Execute(ctx, order, cmd); err != nil {
		fmt.Printf("Failed to execute command: %v", err)
		os.Exit(1)
	}

	time.Sleep(time.Millisecond * 100)

	fmt.Println("Aggregate")
	fmt.Printf("OrderID: %#v\n", order.ID)
	fmt.Printf("Amount: %#v\n", order.Amount)

	fmt.Println("Projection")
	// Print the projection state
	fmt.Printf("Total Orders: %d\n", summary.TotalOrders)
	fmt.Printf("Total Amount: %.2f\n", summary.TotalAmount)
}
