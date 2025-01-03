package eventsource

import (
	"sync"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/mitchellh/mapstructure"
	"gorm.io/gorm"
)

// TestEventStore is an in-memory implementation of EventStore for testing purposes.
type TestEventStore struct {
	data []Event
	sync.RWMutex

	SaveFail error
}

// Load retrieves an object or aggregate by its primary key.
func (s *TestEventStore) Load(ctx golly.Context, object interface{}) error {
	s.RLock()
	defer s.RUnlock()

	if len(s.data) == 0 {
		return gorm.ErrRecordNotFound
	}
	return mapstructure.Decode(s.data[0], object)
}

// LoadEventsForAggregate fetches all events for a given aggregate.
func (s *TestEventStore) LoadEventsForAggregate(ctx golly.Context, aggregateType, aggregateID string) ([]Event, error) {
	s.RLock()
	defer s.RUnlock()

	var results []Event
	for _, event := range s.data {
		if event.AggregateID == aggregateID && event.AggregateType == aggregateType {
			results = append(results, event)
		}
	}
	return results, nil
}

// LoadEvents retrieves all events.
func (s *TestEventStore) LoadEvents(ctx golly.Context, filters ...EventFilter) ([]Event, error) {
	s.RLock()
	defer s.RUnlock()

	var results []Event
	for _, event := range s.data {
		results = append(results, event)
	}
	return results, nil
}

// Save persists one or more events in-memory.
func (s *TestEventStore) Save(ctx golly.Context, events ...*Event) error {
	if s.SaveFail != nil {
		return s.SaveFail
	}

	s.Lock()
	defer s.Unlock()

	for _, evt := range events {
		s.data = append(s.data, *evt)
	}
	return nil
}

// IsNewEvent checks if an event is new.
func (s *TestEventStore) IsNewEvent(event Event) bool {
	s.RLock()
	defer s.RUnlock()

	for _, e := range s.data {
		if e.AggregateID == event.AggregateID && e.Version == event.Version {
			return false
		}
	}
	return true
}

// Exists checks if an event exists by its ID.
func (s *TestEventStore) Exists(ctx golly.Context, eventID uuid.UUID) (bool, error) {
	s.RLock()
	defer s.RUnlock()

	for _, event := range s.data {
		if event.ID == eventID {
			return true, nil
		}
	}
	return false, nil
}

// DeleteEvent removes an event by ID.
func (s *TestEventStore) DeleteEvent(ctx golly.Context, eventID uuid.UUID) error {
	s.Lock()
	defer s.Unlock()

	for i, event := range s.data {
		if event.ID == eventID {
			s.data = append(s.data[:i], s.data[i+1:]...)
			return nil
		}
	}
	return gorm.ErrRecordNotFound
}

// SaveSnapshot persists an aggregate snapshot in-memory.
func (s *TestEventStore) SaveSnapshot(ctx golly.Context, aggregate Aggregate) error {
	snapshot := NewSnapshot(aggregate)
	return s.Save(ctx, &snapshot)
}

// LoadSnapshot retrieves the latest snapshot for an aggregate.
func (s *TestEventStore) LoadSnapshot(ctx golly.Context, aggregateType, aggregateID string) (Event, error) {
	s.RLock()
	defer s.RUnlock()

	for _, event := range s.data {

		if event.AggregateID == aggregateID && event.Kind == EventKindSnapshot {
			return event, nil
		}
	}
	return Event{}, gorm.ErrRecordNotFound
}
