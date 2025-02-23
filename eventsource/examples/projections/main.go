package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/google/uuid"
)

// OrderCreated event
type OrderCreated struct {
	ID         string
	CustomerID string
	Amount     float64
}

// OrderSummary projection
type OrderSummary struct {
	eventsource.ProjectionBase

	CustomerCounts map[string]int
	TotalOrders    int
	TotalAmount    float64
}

func (os *OrderSummary) HandleEvent(ctx context.Context, evt eventsource.Event) error {
	switch e := evt.Data.(type) {
	case OrderCreated:
		os.TotalOrders++
		os.TotalAmount += e.Amount
		os.CustomerCounts[e.CustomerID]++
	}
	return nil
}

func (*OrderSummary) EventTypes() []any {
	return []any{eventsource.AllEvents}
}

func main() {
	// Create engine with in-memory store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	engine.Start()
	defer engine.Stop()

	// Create and register the OrderSummary projection
	summary := &OrderSummary{
		CustomerCounts: make(map[string]int),
	}
	engine.RegisterProjection(summary)

	// Send some events
	events := make([]eventsource.Event, 10)

	for i := 1; i <= 30; i++ {
		aggID, _ := uuid.NewV7()

		events = append(events, eventsource.Event{
			ID:            uuid.New(),
			Type:          "OrderCreated",
			AggregateID:   aggID.String(),
			AggregateType: "Order",
			Version:       1,
			Data: OrderCreated{
				ID:         uuid.New().String(),
				CustomerID: fmt.Sprintf("cust_%d", i%3),
				Amount:     float64(i) * 10.0,
			},
		})
	}

	engine.Send(golly.NewContext(nil), events...)

	// Wait for the projection to process the events
	// we could just call stop and let the engine handle it
	// but this is just an example
	time.Sleep(200 * time.Millisecond)

	// Print the projection state
	log.Printf("Total Orders: %d", summary.TotalOrders)
	log.Printf("Total Amount: %.2f", summary.TotalAmount)
	log.Printf("Customer Counts: %v", summary.CustomerCounts)
}
