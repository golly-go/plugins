# Event Sourcing with Golang

This project implements a simple event-sourcing pattern in Golang. It enables the use of aggregates, streams, and event handlers to manage state changes in a consistent and traceable manner.

## Overview

Event sourcing is a pattern where changes to an application's state are stored as a series of events. This allows for:

- **Auditability**: Every change is recorded as an immutable event.
- **Reconstruction**: System state can be reconstructed by replaying past events.
- **Flexibility**: The same event stream can drive multiple projections or views.

This implementation includes:

- **Aggregate Management**: Aggregates represent domain objects that emit events.
- **Streams**: Streams handle event distribution and aggregation.
- **Commands and Events**: Commands trigger state changes which are recorded as events.

---

## Installation

```bash
go get github.com/golly-go/plugins/eventsource
```

---

## Usage

### 1. Register Aggregates and Events

First, define aggregates and register the events they will handle:

```go
package main

import (
	"fmt"
	"github.com/golly-go/plugins/eventsource"
)

func main() {
	eventsource.Aggregates().Register(&MyAggregate{}, []any{
		Created{},
		Updated{},
		SomeRelationAdded{},
	})
}
```

- `MyAggregate` is the aggregate representing your domain object.
- `Created`, `Updated`, and `SomeRelationAdded` are events that the aggregate can handle.

### 2. Update Handlers for Events

Define event handlers in the aggregate by creating methods that follow the pattern `<EventName>Handler`:

```go
func (ma *MyAggregate) CreatedHandler(event eventsource.Event) {
	evt := event.Data.(Created)
	ma.ID = evt.ID
	ma.Name = evt.Name
}

func (ma *MyAggregate) UpdatedHandler(event eventsource.Event) {
	evt := event.Data.(Updated)
	ma.Name = evt.Name
}

func (ma *MyAggregate) SomeRelationAddedHandler(event eventsource.Event) {
	evt := event.Data.(SomeRelationAdded)
	ma.SomeRelation = golly.Unique(append(ma.SomeRelation, evt.Relation))
}
```

These handlers allow aggregates to apply state changes when events are replayed.

### 3. Stream Event Handling

Attach handlers to process events for specific aggregates:

```go
eventsource.DefaultStream().Aggregate(eventsource.ObjectName(MyAggregate{}), func(e eventsource.Event) {
	fmt.Printf("Event: %#v\n", e)
})
```

This will print each event processed by the `MyAggregate` to the console.

### 4. Execute Commands

Commands represent user actions or requests that trigger state changes:

```go
err := eventsource.Execute(gctx, &agg, UpdateMyAggregate{
	Name: args[1],
})
if err != nil {
	return err
}
```

- `UpdateMyAggregate` is a command that updates the aggregate.
- The command performs validation and emits an `Updated` event.

---

## Command Example

### Define a Command
```go
type UpdateMyAggregate struct {
	Name string
}

func (cmd UpdateMyAggregate) Validate(gctx golly.Context, agg eventsource.Aggregate) error {
	if cmd.Name == "" {
		return fmt.Errorf("name should be present")
	}
	return nil
}

func (cmd UpdateMyAggregate) Perform(gctx golly.Context, agg eventsource.Aggregate) error {
	agg.Record(agg, Updated{Name: cmd.Name})
	return nil
}
```
- `Validate` ensures the command's data is correct before proceeding.
- `Perform` applies the event to the aggregate.

---

## Key Concepts

### Aggregates
Aggregates represent the core business entities that emit events. They ensure consistency and encapsulate state changes.

### Events
Events are immutable records of state changes. They represent facts that have occurred in the system.

### Streams
Streams manage event distribution. They allow handlers to listen to and react to specific events or aggregates.

### Commands
Commands trigger actions that may result in new events. They represent user intent.

---

## Testing

To test commands and aggregate behavior, simulate command execution and verify that the correct events are emitted:

```go
func TestUpdateMyAggregate(t *testing.T) {
	agg := MyAggregate{}
	cmd := UpdateMyAggregate{Name: "NewName"}
	err := eventsource.Execute(gctx, &agg, cmd)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
```

---

## Event Stores
Prebuilt event stores can be found at:

```bash
github.com/golly-go/plugins/eventsource/eventstore/<name>
```

These event stores can be used to persist and manage events across different backends.

---

## Conclusion

This event sourcing pattern in Golang provides a scalable way to manage state through immutable events. By structuring your application around aggregates, events, and streams, you ensure traceability, consistency, and flexibility in handling complex state transitions.

