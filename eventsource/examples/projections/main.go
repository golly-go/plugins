package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/google/uuid"
)

// OrderCreated represents a new order event
type OrderCreated struct {
	ID         string
	CustomerID string
	Amount     float64
}

// OrderSummary is a projection that maintains order statistics
type OrderSummary struct {
	eventsource.ProjectionBase
	mu sync.RWMutex

	TotalOrders    int
	TotalAmount    float64
	CustomerCounts map[string]int
}

func (*OrderSummary) EventTypes() []string {
	return []string{"OrderCreated"}
}

func (o *OrderSummary) HandleEvent(ctx *golly.Context, evt eventsource.Event) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	switch event := evt.Data.(type) {
	case OrderCreated:
		o.TotalOrders++
		o.TotalAmount += event.Amount
		o.CustomerCounts[event.CustomerID]++
	}
	return nil
}

func (o *OrderSummary) Reset() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.TotalOrders = 0
	o.TotalAmount = 0
	o.CustomerCounts = make(map[string]int)
	return nil
}

func main() {
	// Create engine with store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	engine.Start()
	defer engine.Stop()

	// Create and register projection
	summary := &OrderSummary{
		CustomerCounts: make(map[string]int),
	}

	// Register projection with stream option
	err := engine.RegisterProjection(summary, eventsource.WithStream("orders", true, 1))
	if err != nil {
		log.Fatalf("Failed to register projection: %v", err)
	}

	for i := 1; i < 123; i++ {
		// Send events through engine
		err = engine.Send(golly.NewContext(context.Background()), "orders", eventsource.Event{
			AggregateID:   uuid.New().String(),
			GlobalVersion: int64(i),
			Type:          "OrderCreated",
			Data:          OrderCreated{ID: fmt.Sprintf("order_%d", i), CustomerID: fmt.Sprintf("cust_%d", i%20), Amount: float64(i) * 1.13},
		})

		if err != nil {
			log.Fatalf("Failed to send event: %v", err)
			os.Exit(1)
		}
	}

	// Add a small delay to allow processing
	time.Sleep(100 * time.Millisecond)

	// Print current state (in production you'd want proper synchronization here)
	summary.mu.RLock()
	log.Printf("Total Orders: %d", summary.TotalOrders)
	log.Printf("Total Amount: %.2f", summary.TotalAmount)
	log.Printf("Customer Counts: %v", summary.CustomerCounts)
	summary.mu.RUnlock()
}
