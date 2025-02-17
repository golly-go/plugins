package gormstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/golly-go/plugins/eventsource"
	"github.com/golly-go/plugins/orm"
	"github.com/google/uuid"
	"github.com/jinzhu/gorm/dialects/postgres"
	"gorm.io/gorm"
)

// Event wraps eventsource.Event to manage GORM-specific raw data handling.
type Event struct {
	eventsource.Event

	RawData     postgres.Jsonb `json:"-" gorm:"type:jsonb;column:data"`
	RawMetadata postgres.Jsonb `json:"-" gorm:"type:jsonb;column:metadata"`
}

type GlobalVersion struct {
	ID      string `gorm:"primaryKey"`
	Version int64  `gorm:"not null"`
}

var (
	Models = []any{
		&Event{},
		&GlobalVersion{},
	}
)

func Migrate(db *gorm.DB) error {
	return db.AutoMigrate(Models...)
}

func (e *Event) Hydrate(engine *eventsource.Engine) (eventsource.Event, error) {
	if err := e.Event.Hydrate(engine, e.RawData.RawMessage, e.RawMetadata.RawMessage); err != nil {
		return eventsource.Event{}, err
	}
	return e.Event, nil
}

func (e *Event) GlobalVersion() int64 {
	return e.Event.GlobalVersion
}

var _ eventsource.PersistedEvent = (*Event)(nil)

// Store provides event persistence through GORM.
type Store struct{}

// Load retrieves an object or aggregate by its primary key.
func (s *Store) Load(ctx context.Context, object interface{}) error {
	return orm.DB(ctx).Model(object).First(object).Error
}

// LoadEvents retrieves all events across aggregates.
func (s *Store) LoadEvents(ctx context.Context, filters ...eventsource.EventFilter) ([]eventsource.PersistedEvent, error) {
	var events []Event
	query := orm.DB(ctx)

	applyFilters(query, filters...)

	err := query.Find(&events).Error

	evts := make([]eventsource.PersistedEvent, len(events))
	for pos := range events {
		evts[pos] = &events[pos]
	}

	return evts, err
}

// IncrementGlobalVersion atomically increments the global version and returns the new value.
func (s *Store) IncrementGlobalVersion(ctx context.Context) (int64, error) {
	var newVersion int64

	err := orm.DB(ctx).Transaction(func(tx *gorm.DB) error {
		// Use INSERT ... ON CONFLICT to atomically increment the version
		err := tx.Raw(`
			INSERT INTO global_versions (id, version)
			VALUES (1, 1)
			ON CONFLICT (id)
			DO UPDATE SET version = global_versions.version + 1
			RETURNING version
		`).Scan(&newVersion).Error

		if err != nil {
			return fmt.Errorf("failed to increment global version: %w", err)
		}

		return nil
	})

	if err != nil {
		return 0, err
	}

	return newVersion, nil
}

// LoadEventsInBatches loads events in batches, calling handler for each batch.
// If handler returns an error, processing stops and that error is returned.
func (s *Store) LoadEventsInBatches(
	ctx context.Context,
	batchSize int,
	handler func([]eventsource.PersistedEvent) error,
	filters ...eventsource.EventFilter,
) error {
	// We'll build the base query in ascending global_version:
	baseQuery := orm.DB(ctx).Model(&Event{}).Order("global_version ASC")

	// Apply user-provided filters (e.g., FromVersion, FromTime, etc.)
	applyFilters(baseQuery, filters...)

	offset := 0
	for {
		// Clone the base query to avoid mutating it
		var batchModels []Event
		q := baseQuery.Limit(batchSize).Offset(offset)

		if err := q.Find(&batchModels).Error; err != nil {
			return fmt.Errorf("failed to load events batch: %w", err)
		}
		if len(batchModels) == 0 {
			break // no more events
		}

		// Convert models to domain events
		evts := make([]eventsource.PersistedEvent, len(batchModels))

		for i := range batchModels {
			evts[i] = &batchModels[i]
		}

		// Call the user's handler
		if err := handler(evts); err != nil {
			return err
		}

		offset += len(batchModels)
		// If we got fewer than batchSize, we've reached the end.
		if len(batchModels) < batchSize {
			break
		}
	}
	return nil
}

// Save persists one or more events to the database.
func (*Store) Save(ctx context.Context, events ...*eventsource.Event) error {
	batch, err := mapBatchToDB(events)
	if err != nil {
		return err
	}
	return orm.NewDB(ctx).CreateInBatches(batch, len(batch)).Error
}

// IsNewEvent checks if the aggregate is new by verifying the ID.
func (*Store) IsNewEvent(event eventsource.Event) bool {
	return event.AggregateID == uuid.Nil.String()
}

// Exists checks if an event exists by its ID.
func (*Store) Exists(ctx context.Context, eventID uuid.UUID) (bool, error) {
	if eventID == uuid.Nil {
		return false, nil
	}

	var count int64

	err := orm.DB(ctx).
		Model(&Event{}).
		Where("id = ?", eventID).
		Count(&count).
		Error

	return count > 0, err
}

// DeleteEvent removes an event by ID.
func (*Store) DeleteEvent(ctx context.Context, eventID uuid.UUID) error {
	return orm.DB(ctx).Where("id = ?", eventID).Delete(&Event{}).Error
}

// SaveSnapshot persists an aggregate snapshot.
func (store *Store) SaveSnapshot(ctx context.Context, aggregate eventsource.Aggregate) error {
	snapshot := eventsource.NewSnapshot(aggregate)

	return store.Save(ctx, &snapshot)
}

// LoadSnapshot retrieves the latest snapshot for an aggregate.
func (*Store) LoadSnapshot(ctx context.Context, aggregateType, aggregateID string) (eventsource.PersistedEvent, error) {
	var snapshot Event

	err := orm.
		DB(ctx).
		Where("aggregate_id = ? AND aggregate_type = ? AND kind = ?",
			aggregateID,
			aggregateType,
			eventsource.EventKindSnapshot).
		Order("id DESC").
		First(&snapshot).
		Error

	return &snapshot, err
}

// mapToDB converts an eventsource.Event to a database-compatible Event.
func mapToDB(evt *eventsource.Event) (Event, error) {
	var err error
	ret := Event{Event: *evt}

	ret.RawMetadata.RawMessage, err = json.Marshal(evt.Metadata)
	if err != nil {
		return ret, err
	}

	ret.RawData.RawMessage, err = json.Marshal(evt.Data)
	if err != nil {
		return ret, err
	}

	return ret, nil
}

// mapBatchToDB maps a batch of events for batch insertion.
func mapBatchToDB(events []*eventsource.Event) ([]Event, error) {
	var result []Event
	for _, evt := range events {
		mapped, err := mapToDB(evt)
		if err != nil {
			return nil, err
		}
		result = append(result, mapped)
	}
	return result, nil
}

func applyFilters(query *gorm.DB, filters ...eventsource.EventFilter) *gorm.DB {
	if len(filters) > 0 {
		filter := filters[0]

		if filter.AggregateID != "" {
			query = query.Where("aggregate_id = ?", filter.AggregateID)
		}
		if filter.AggregateType != "" {
			query = query.Where("aggregate_type = ?", filter.AggregateType)
		}

		if len(filter.AggregateTypes) > 0 {
			query = query.Where("aggregate_type IN ?", filter.AggregateTypes)
		}

		if len(filter.EventType) > 0 {
			query = query.Where("type IN ?", filter.EventType)
		}
		if filter.FromVersion > 0 {
			query = query.Where("version >= ?", filter.FromVersion)
		}
		if filter.ToVersion > 0 {
			query = query.Where("version <= ?", filter.ToVersion)
		}
		if !filter.FromTime.IsZero() {
			query = query.Where("timestamp >= ?", filter.FromTime)
		}
		if !filter.ToTime.IsZero() {
			query = query.Where("timestamp <= ?", filter.ToTime)
		}
		if filter.FromGlobalVersion > 0 {
			query = query.Where("global_version >= ", filter.FromGlobalVersion)
		}
		if filter.ToGlobalVersion > 0 {
			query = query.Where("global_version <= ", filter.ToGlobalVersion)
		}

		if filter.Limit > 0 {
			query = query.Limit(filter.Limit)
		}

	}

	return query.Order("global_version ASC")
}

var _ eventsource.EventStore = (*Store)(nil)
