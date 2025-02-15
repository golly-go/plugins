# Event Sourcing with Golang

This project implements a robust event-sourcing pattern in Golang, providing a comprehensive framework for building event-driven applications. It enables the use of aggregates, streams, and event handlers to manage state changes in a consistent and traceable manner.

## Overview

Event sourcing is a pattern where changes to an application's state are stored as a sequence of immutable events. This approach offers several benefits:

- **Auditability**: Every state change is recorded as an immutable event.
- **Reconstruction**: System state can be rebuilt by replaying past events.
- **Flexibility**: The same event stream can power multiple projections or views.
- **Concurrency**: Built-in support for concurrent event processing.
- **Scalability**: Partitioned streams for high-throughput event handling.

## Features

- **Aggregate Management**: Type-safe aggregates with automatic event application.
- **Stream Processing**: Configurable event streams with partitioning support.
- **Projections**: Real-time and rebuild-capable read models.
- **Event Store**: Pluggable storage backends (includes PostgreSQL and in-memory).
- **Command Handling**: Structured command processing with validation.
- **Snapshots**: Automatic aggregate state snapshots for faster loading.

## Installation

```bash
go get github.com/golly-go/plugins/eventsource
```

## Quick Start

### 1. Define Your Domain Events

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

// Event handlers are automatically discovered
func (o *Order) ApplyOrderCreated(event OrderCreated) {
    o.ID = event.ID
    o.Amount = event.Amount
}

func (o *Order) ApplyOrderStatusChanged(event OrderStatusChanged) {
    o.Status = event.Status
}
```

### 3. Set Up the Engine

```go
// Initialize with your preferred store
engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

// Register aggregate and its events
engine.RegisterAggregate(&Order{}, []any{
    OrderCreated{},
    OrderStatusChanged{},
})

// Start processing
engine.Start()
defer engine.Stop()
```

### 4. Create a Projection

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

```go
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
    DB: db, // your GORM DB instance
}

// In-Memory Store (great for testing)
store := &eventsource.InMemoryStore{}

engine := eventsource.NewEngine(store)
```

### Stream Partitioning

Configure streams with custom partitioning for better throughput:

```go
engine.RegisterProjection(projection,
    eventsource.WithStream("orders", true, 8),  // 8 partitions
    eventsource.WithStreamBufferSize(1000))
```

### Snapshots

Automatic snapshot support for faster aggregate loading:

```go
// Configure snapshot frequency
engine.SetSnapshotFrequency(100) // Every 100 events

// Load aggregate with latest snapshot
agg, err := engine.LoadAggregate(ctx, "Order", "order_123")
```

## Testing

The library includes an in-memory store perfect for testing:

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

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the [MIT License](./LICENSE).  
See the `LICENSE` file for details.
