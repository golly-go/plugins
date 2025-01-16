package gorm

import (
	"context"
	"encoding/json"

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

// GormEventStore provides event persistence through GORM.
type GormEventStore struct{}

// AfterFind hydrates the event after it is loaded from the database.
func (e *Event) AfterFind(tx *gorm.DB) error {
	return e.Event.Hydrate(e.RawData.RawMessage)
}

// Load retrieves an object or aggregate by its primary key.
func (GormEventStore) Load(ctx context.Context, object interface{}) error {
	return orm.DB(ctx).Model(object).First(object).Error
}

// LoadEvents retrieves all events across aggregates.
func (GormEventStore) LoadEvents(ctx context.Context, filters ...eventsource.EventFilter) ([]eventsource.Event, error) {
	var events []Event
	query := orm.DB(ctx).Order("version ASC")

	// Apply filters if provided
	if len(filters) > 0 {
		filter := filters[0]

		if filter.AggregateID != "" {
			query = query.Where("aggregate_id = ?", filter.AggregateID)
		}
		if filter.AggregateType != "" {
			query = query.Where("aggregate_type = ?", filter.AggregateType)
		}
		if filter.EventType != "" {
			query = query.Where("type = ?", filter.EventType)
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
		if filter.Limit > 0 {
			query = query.Limit(filter.Limit)
		}
	}

	err := query.Find(&events).Error

	evts := make([]eventsource.Event, len(events))
	for pos := range events {
		evts[pos] = events[pos].Event
	}

	return evts, err
}

// Save persists one or more events to the database.
func (GormEventStore) Save(ctx context.Context, events ...*eventsource.Event) error {
	batch, err := mapBatchToDB(events)
	if err != nil {
		return err
	}
	return orm.NewDB(ctx).CreateInBatches(batch, len(batch)).Error
}

// IsNewEvent checks if the aggregate is new by verifying the ID.
func (GormEventStore) IsNewEvent(event eventsource.Event) bool {
	return event.AggregateID == uuid.Nil.String()
}

// Exists checks if an event exists by its ID.
func (GormEventStore) Exists(ctx context.Context, eventID uuid.UUID) (bool, error) {
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
func (GormEventStore) DeleteEvent(ctx context.Context, eventID uuid.UUID) error {
	return orm.DB(ctx).Where("id = ?", eventID).Delete(&Event{}).Error
}

// SaveSnapshot persists an aggregate snapshot.
func (store GormEventStore) SaveSnapshot(ctx context.Context, aggregate eventsource.Aggregate) error {
	snapshot := eventsource.NewSnapshot(aggregate)

	return store.Save(ctx, &snapshot)
}

// LoadSnapshot retrieves the latest snapshot for an aggregate.
func (GormEventStore) LoadSnapshot(ctx context.Context, aggregateType, aggregateID string) (eventsource.Event, error) {
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

	return snapshot.Event, err
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

var _ eventsource.EventStore = &GormEventStore{}
