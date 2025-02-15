package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/google/uuid"
)

// Order aggregate
type Order struct {
	ID      string
	Status  string
	Amount  float64
	Version int64
}

// OrderSummary projection
type OrderSummary struct {
	eventsource.ProjectionBase

	TotalOrders int
	TotalAmount float64
	StatusCount map[string]int

	mu sync.RWMutex
}

func (o *OrderSummary) HandleEvent(ctx *golly.Context, evt eventsource.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	switch evt.Type {
	case "OrderCreated":
		order := evt.Data.(Order)
		o.TotalOrders++
		o.TotalAmount += order.Amount
		o.StatusCount["created"]++
	case "OrderStatusChanged":
		order := evt.Data.(Order)
		o.StatusCount[order.Status]++
	}
	return nil
}

func main() {
	// Create engine with in-memory store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	// Create and configure projection
	summary := &OrderSummary{
		StatusCount: make(map[string]int),
	}

	// Register projection with proper stream configuration
	err := engine.RegisterProjection(summary,
		eventsource.WithStream("orders", true, 4), // name, autoCreate, partitions
		eventsource.WithStreamBufferSize(1000))
	if err != nil {
		log.Fatalf("Failed to register projection: %v", err)
	}

	// Start the engine
	engine.Start()
	defer engine.Stop()

	// Send some events
	events := []eventsource.Event{
		{
			ID:          uuid.New(),
			Type:        "OrderCreated",
			AggregateID: "order_1",
			Version:     1,
			Data: Order{
				ID:      "order_1",
				Status:  "created",
				Amount:  100.00,
				Version: 1,
			},
		},
		{
			ID:          uuid.New(),
			Type:        "OrderStatusChanged",
			AggregateID: "order_1",
			Version:     2,
			Data: Order{
				ID:      "order_1",
				Status:  "processing",
				Amount:  100.00,
				Version: 2,
			},
		},
	}

	for _, evt := range events {
		if err := engine.Send(golly.NewContext(context.Background()), "orders", evt); err != nil {
			log.Printf("Failed to send event: %v", err)
			continue
		}
	}

	// Allow time for processing
	time.Sleep(100 * time.Millisecond)

	// Print summary
	summary.mu.RLock()
	log.Printf("Total Orders: %d", summary.TotalOrders)
	log.Printf("Total Amount: %.2f", summary.TotalAmount)
	log.Printf("Status Counts: %v", summary.StatusCount)
	summary.mu.RUnlock()
}
