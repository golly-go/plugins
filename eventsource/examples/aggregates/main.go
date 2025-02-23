package main

import (
	"context"
	"log"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
)

// Domain Events
type CustomerOrderAdded struct {
	CustomerID string
	OrderID    string
}

// Aggregate
type Customer struct {
	eventsource.AggregateBase // Embed the base aggregate
	ID                        string
	Orders                    []string
	Balance                   float64
}

func (c *Customer) GetID() string {
	return c.ID
}

// HandleEvent implements the Projection interface
func (c *Customer) HandleEvent(ctx *golly.Context, evt eventsource.Event) error {
	switch evt.Data.(type) {
	case CustomerOrderAdded:
		data := evt.Data.(CustomerOrderAdded)
		c.Orders = append(c.Orders, data.OrderID)
	}
	return nil
}

func main() {
	// Create engine with store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	// Register aggregate with domain event types
	engine.RegisterAggregate(&Customer{}, []any{
		CustomerOrderAdded{}, // Register the domain event struct
	})

	// Create a customer instance
	customer := &Customer{ID: "cust_1"}

	// Execute command through engine
	ctx := golly.NewContext(context.Background())
	cmd := &AddOrderCommand{
		CustomerID: "cust_1",
		OrderID:    "order_123",
	}

	if err := engine.Execute(ctx, customer, cmd); err != nil {
		log.Printf("Error executing command: %v", err)
	}

	log.Printf("Customer orders: %v", customer.Orders)
}

// Add command handling
type AddOrderCommand struct {
	CustomerID string
	OrderID    string
}

func (c *AddOrderCommand) Perform(ctx context.Context, agg eventsource.Aggregate) error {
	customer := agg.(*Customer)

	// Create event using domain event struct
	customer.Record(eventsource.Event{
		Type:          "CustomerOrderAdded",
		AggregateType: "Customer",
		AggregateID:   customer.ID,
		Data: CustomerOrderAdded{
			CustomerID: c.CustomerID,
			OrderID:    c.OrderID,
		},
	})
	return nil
}
