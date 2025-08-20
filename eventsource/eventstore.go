package eventsource

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrVersionConflict = errors.New("version conflict")
)

// EventFilter represents criteria for filtering events during retrieval.
type EventFilter struct {
	AggregateType  string   // Filter by aggregate type (e.g., "Order", "User")
	AggregateID    string   // Filter by a specific aggregate ID
	AggregateTypes []string // Filter by multiple aggregate types
	EventType      []string // Filter by event type (e.g., "OrderCreated", "UserUpdated")
	Topics         []string // Filter by topics (e.g., "order", "user")

	FromVersion int // Minimum event version to load
	ToVersion   int // Maximum event version to load

	FromGlobalVersion int // Minimum global version to load
	ToGlobalVersion   int // Maximum global version to load

	FromTime time.Time // Events created after this time

	ToTime time.Time // Events created before this time
	Limit  int       // Maximum number of events to return

}

// EventStore is an interface for managing event persistence and retrieval.
type EventStore interface {
	// Save persists one or more events to the event store.
	// Events are stored atomically to ensure consistency.
	Save(ctx context.Context, events ...*Event) error

	// LoadEvents retrieves all events across different aggregates.
	// This can be useful for projections or rebuilding read models. (This is expected to return in order of GlobalVersion)
	LoadEvents(ctx context.Context, filters ...EventFilter) ([]PersistedEvent, error)

	// LoadEventsInBatches loads the events in batches of batch size calling handler for each batch
	// if handler returns error processing is stopped (These is expected to return in order of GlobalVersion)
	LoadEventsInBatches(ctx context.Context, batchSize int, handler func([]PersistedEvent) error, filters ...EventFilter) error

	// IsNewEvent checks if an event is new by inspecting its metadata (e.g., ID, version).
	IsNewEvent(event Event) bool

	// Exists checks if an event exists by its ID or unique key.
	Exists(ctx context.Context, eventID uuid.UUID) (bool, error)

	// DeleteEvent removes an event by ID. Useful for GDPR or soft deletes.
	DeleteEvent(ctx context.Context, eventID uuid.UUID) error

	// AggregateSnapshot persists a snapshot of an aggregate state.
	SaveSnapshot(ctx context.Context, snapshot Aggregate) error

	// LoadSnapshot retrieves the latest snapshot of an aggregate for faster loading.
	LoadSnapshot(ctx context.Context, aggregateType, aggregateID string) (PersistedEvent, error)

	IncrementGlobalVersion(ctx context.Context) (int64, error)

	// Lock(Aggregate) error
	// Unlock(Aggregate) error

	// Hydrate(repo *EventRepository, event any) (Event, error)
}
