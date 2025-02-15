# Event Sourcing with Golang

This project provides a robust event-sourcing framework for Golang, designed for building scalable, event-driven applications. It enables the use of aggregates, streams, and event handlers to ensure consistent and traceable state changes.

## Overview

Event sourcing is a pattern that records changes in application state as immutable events. This approach provides several advantages:

- **Auditability**: Every state change is stored as an immutable event.
- **Reconstruction**: System state can be rebuilt by replaying historical events.
- **Flexibility**: The same event stream can power multiple projections or views.
- **Concurrency**: Supports concurrent event processing.
- **Scalability**: Uses partitioned streams for high-throughput event handling.

## Features

- **Aggregate Management**: Type-safe aggregates with automatic event application.
- **Stream Processing**: Configurable event streams with partitioning support.
- **Projections**: Supports real-time and rebuildable read models.
- **Event Store**: Pluggable storage backends (including PostgreSQL and in-memory storage).
- **Command Handling**: Structured command processing with validation.
- **Snapshots**: Automatic aggregate state snapshots for improved performance.

## Installation

Install the package using:

```bash
go get github.com/golly-go/plugins/eventsource
```

## Quick Start

### 1. Define Your Domain Events

Define the domain events representing state changes:

```go
type OrderCreated struct {
    ID         string
    CustomerID string
    Amount     float64
}

type OrderStatusChanged struct {
    ID     string
    Status string
}
```

### 2. Create an Aggregate

Define an aggregate that maintains state and applies events:

```go
type Order struct {
    eventsource.AggregateBase
    ID      string
    Status  string
    Amount  float64
}

func (o *Order) GetID() string {
    return o.ID
}

// Automatically applies event handlers
func (o *Order) ApplyOrderCreated(event OrderCreated) {
    o.ID = event.ID
    o.Amount = event.Amount
}

func (o *Order) ApplyOrderStatusChanged(event OrderStatusChanged) {
    o.Status = event.Status
}
```

### 3. Set Up the Engine

Initialize the event-sourcing engine and register aggregates:

```go
// Initialize with a preferred store
engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

// Register aggregates and their events
engine.RegisterAggregate(&Order{}, []any{
    OrderCreated{},
    OrderStatusChanged{},
})

// Start processing
engine.Start()
defer engine.Stop()
```

### 4. Create a Projection

Projections allow building read models from events:

```go
type OrderSummary struct {
    eventsource.ProjectionBase
    mu          sync.RWMutex
    TotalOrders int
    TotalAmount float64
}

func (o *OrderSummary) HandleEvent(ctx *golly.Context, evt eventsource.Event) error {
    o.mu.Lock()
    defer o.mu.Unlock()

    switch e := evt.Data.(type) {
    case OrderCreated:
        o.TotalOrders++
        o.TotalAmount += e.Amount
    }
    return nil
}

// Register projection
summary := &OrderSummary{}
engine.RegisterProjection(summary,
    eventsource.WithStream("orders", true, 4))
```

### 5. Execute Commands

Commands trigger state changes within aggregates:

```go
type CreateOrder struct {
    ID     string
    Amount float64
}

func (c CreateOrder) Perform(ctx *golly.Context, agg eventsource.Aggregate) error {
    order := agg.(*Order)
    order.Record(OrderCreated{
        ID:     c.ID,
        Amount: c.Amount,
    })
    return nil
}

// Execute the command
order := &Order{}
err := engine.Execute(ctx, order, CreateOrder{
    ID:     "order_1",
    Amount: 99.99,
})
```

## Advanced Features

### Event Store Configuration

The library supports multiple event store implementations:

```go
// PostgreSQL Store
store := &gormstore.Store{
    DB: db, // Your GORM DB instance
}

// In-Memory Store (ideal for testing)
store := &eventsource.InMemoryStore{}

engine := eventsource.NewEngine(store)
```

### Stream Partitioning

Configure streams with custom partitioning for improved scalability:

```go
engine.RegisterProjection(projection,
    eventsource.WithStream("orders", true, 8),  // 8 partitions
    eventsource.WithStreamBufferSize(1000))
```

### Snapshots

Enable automatic snapshots to speed up aggregate loading:

```go
// Set snapshot frequency
engine.SetSnapshotFrequency(100) // Every 100 events

// Load aggregate using latest snapshot
agg, err := engine.Load(ctx, "Order", "order_123")
```

## Testing

The library includes an in-memory store ideal for testing:

```go
func TestOrderCreation(t *testing.T) {
    engine := eventsource.NewEngine(&eventsource.InMemoryStore{})
    order := &Order{}
    
    err := engine.Execute(ctx, order, CreateOrder{
        ID:     "test_1",
        Amount: 100,
    })
    
    assert.NoError(t, err)
    assert.Equal(t, 100.0, order.Amount)
}
```

## Contributing

Contributions are welcome! Please submit a Pull Request.

## License

This project is licensed under the [MIT License](./LICENSE). See the `LICENSE` file for details.

