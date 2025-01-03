package eventsource

import (
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
)

// EventFilter represents criteria for filtering events during retrieval.
type EventFilter struct {
	AggregateType string    // Filter by aggregate type (e.g., "Order", "User")
	AggregateID   string    // Filter by a specific aggregate ID
	EventType     string    // Filter by event type (e.g., "OrderCreated", "UserUpdated")
	FromVersion   int       // Minimum event version to load
	ToVersion     int       // Maximum event version to load
	FromTime      time.Time // Events created after this time
	ToTime        time.Time // Events created before this time
	Limit         int       // Maximum number of events to return
}

// EventStore is an interface for managing event persistence and retrieval.
type EventStore interface {
	// Save persists one or more events to the event store.
	// Events are stored atomically to ensure consistency.
	Save(ctx golly.Context, events ...*Event) error

	// LoadEvents retrieves all events across different aggregates.
	// This can be useful for projections or rebuilding read models.
	LoadEvents(ctx golly.Context, filters ...EventFilter) ([]Event, error)

	// IsNewEvent checks if an event is new by inspecting its metadata (e.g., ID, version).
	IsNewEvent(event Event) bool

	// Exists checks if an event exists by its ID or unique key.
	Exists(ctx golly.Context, eventID uuid.UUID) (bool, error)

	// DeleteEvent removes an event by ID. Useful for GDPR or soft deletes.
	DeleteEvent(ctx golly.Context, eventID uuid.UUID) error

	// AggregateSnapshot persists a snapshot of an aggregate state.
	SaveSnapshot(ctx golly.Context, snapshot Aggregate) error

	// LoadSnapshot retrieves the latest snapshot of an aggregate for faster loading.
	LoadSnapshot(ctx golly.Context, aggregateType, aggregateID string) (Event, error)
}
