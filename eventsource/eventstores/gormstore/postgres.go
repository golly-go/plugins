package gormstore

import (
	"context"
	"errors"
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

const (
	GlobalVersionID = "global"
)

type EventSourceVersion struct {
	ID      string `gorm:"primaryKey"`
	Version int64  `gorm:"not null"`
}

var (
	ErrVersionNotFound = errors.New("version not found")

	Models = []any{
		&Event{},
		&EventSourceVersion{},
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
	return IncrementEventSourceVersion(ctx, GlobalVersionID)
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

// Save persists one or more events to the database with concurrency checks.
func (s *Store) Save(ctx context.Context, events ...*eventsource.Event) error {
	if len(events) == 0 {
		return nil
	}

	batch, err := mapBatchToDB(events)
	if err != nil {
		return err
	}

	desiredStartVersion := batch[0].Version - 1

	db := orm.NewDB(ctx)

	// Use the same DB connection in a transaction, so advisory lock is maintained
	return db.Transaction(func(tx *gorm.DB) error {
		if db.Dialector.Name() == "postgres" {
			// 1) Acquire advisory lock
			lockKey := aggregatorLockKey(batch[0].AggregateType, batch[0].AggregateID)

			if err := tx.Exec("SELECT pg_advisory_xact_lock(?);", lockKey).Error; err != nil {
				return err
			}
		}

		var currentVersion int64
		err := tx.
			Model(&Event{}).
			Where("aggregate_type = ? AND aggregate_id = ?", batch[0].AggregateType, batch[0].AggregateID).
			Select("COALESCE(MAX(version), 0)").
			Scan(&currentVersion).
			Error

		if err != nil {
			return err
		}

		if currentVersion != desiredStartVersion {
			return eventsource.ErrVersionConflict
		}

		if err := tx.CreateInBatches(batch, len(batch)).Error; err != nil {
			return err
		}

		return nil
	})
}

// // Save persists one or more events to the database.
// func (*Store) Save(ctx context.Context, events ...*eventsource.Event) error {
// 	batch, err := mapBatchToDB(events)
// 	if err != nil {
// 		return err
// 	}
// 	return orm.NewDB(ctx).CreateInBatches(batch, len(batch)).Error
// }

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

var _ eventsource.EventStore = (*Store)(nil)

func IncrementEventSourceVersion(ctx context.Context, versionID string) (int64, error) {
	var newVersion int64

	db := orm.NewDB(ctx)

	err := db.Transaction(func(tx *gorm.DB) error {

		if db.Dialector.Name() == "postgres" {
			lockKey := aggregatorLockKey("event_source_versions", versionID)

			if err := tx.Exec("SELECT pg_advisory_xact_lock(?);", lockKey).Error; err != nil {
				return err
			}
		}

		// Use INSERT ... ON CONFLICT to atomically increment the version
		err := tx.Raw(`
			INSERT INTO event_source_versions (id, version)
			VALUES (?, 1)
			ON CONFLICT (id)
			DO UPDATE SET version = event_source_versions.version + 1
			RETURNING version
		`, versionID).
			Scan(&newVersion).Error

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

func SetEventSourceVersion(ctx context.Context, versionID string, version int64) error {
	db := orm.NewDB(ctx)

	return db.Transaction(func(tx *gorm.DB) error {
		if db.Dialector.Name() == "postgres" {
			lockKey := aggregatorLockKey("event_source_versions", versionID)

			if err := tx.Exec("SELECT pg_advisory_xact_lock(?);", lockKey).Error; err != nil {
				return err
			}
		}

		return tx.Exec(`
			INSERT INTO event_source_versions (id, version)
			VALUES (?, ?)
			ON CONFLICT (id)
			DO UPDATE SET version = ?
		`, versionID, version, version).Error
	})
}

func GetEventSourceVersion(ctx context.Context, versionID string) (int64, error) {
	var version int64

	err := orm.
		DB(ctx).
		Model(&EventSourceVersion{}).
		Where("id = ?", versionID).
		Select("version").
		Scan(&version).
		Error

	if version == 0 {
		return 0, ErrVersionNotFound
	}

	return version, err
}

// Query loads all events matching the provided scopes.
func Query(ctx context.Context, scopes ...func(db *gorm.DB) *gorm.DB) ([]eventsource.PersistedEvent, error) {
	var events []Event

	err := orm.DB(ctx).Model(&Event{}).Scopes(scopes...).Find(&events).Error

	if err != nil {
		return nil, err
	}

	evts := make([]eventsource.PersistedEvent, len(events))
	for pos := range events {
		evts[pos] = &events[pos]
	}

	return evts, nil
}

// QueryInBatches loads Event rows in batches and calls handler for each batch.
// - Respects ctx cancellation (via orm.DB(ctx))
// - Applies any provided GORM scopes
// - Converts []Event -> []eventsource.PersistedEvent (as *Event)
func QueryInBatches(
	ctx context.Context,
	batchSize int,
	handler func([]eventsource.PersistedEvent) error,
	scopes ...func(*gorm.DB) *gorm.DB,
) error {
	if batchSize <= 0 {
		batchSize = 1000
	}

	db := orm.DB(ctx).Model(&Event{}).Scopes(scopes...)

	var events []Event
	res := db.FindInBatches(&events, batchSize, func(tx *gorm.DB, batch int) error {
		if len(events) == 0 {
			return nil
		}
		// Wrap current batch as []PersistedEvent (pointing at the batch items)
		evts := make([]eventsource.PersistedEvent, len(events))
		for i := range events {
			evts[i] = &events[i]
		}
		return handler(evts)
	})

	return res.Error
}

// func QueryInBatches(ctx context.Context, batchSize int, handler func([]eventsource.PersistedEvent) error, scopes ...func(db *gorm.DB) *gorm.DB) error {
// 	var events []Event

// 	query := orm.DB(ctx).Model(&Event{}).Scopes(scopes...)

// 	return nil
// }
