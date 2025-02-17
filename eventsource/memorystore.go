package eventsource

import (
	"context"
	"errors"
	"sync"

	"github.com/google/uuid"
)

// InMemoryStore used for testing primarily your welcome to sue it in your tests
// would i use it in production definitely not

// InMemoryEvent wraps an Event and implements the PersistedEvent interface.
// It can be as simple as returning the embedded Event on Hydrate(...).
type InMemoryEvent struct {
	Event
}

// Hydrate satisfies PersistedEvent. In this test store scenario,
// we simply return the embedded Event without transformation.
// In a real store, you'd parse raw data or do reflection if needed.
func (t InMemoryEvent) Hydrate(engine *Engine) (Event, error) {
	// Return the underlying Event directly.
	return t.Event, nil
}

func (t InMemoryEvent) GlobalVersion() int64 {
	return t.Event.GlobalVersion
}

// InMemoryStore is an in-memory implementation of EventStore for testing purposes.
type InMemoryStore struct {
	data []Event
	sync.RWMutex

	SaveFail error

	gobalVersion int64
}

func (s *InMemoryStore) IncrementGlobalVersion(context.Context) (int64, error) {
	s.Lock()
	defer s.Unlock()

	s.gobalVersion++
	return s.gobalVersion, nil
}

// Save persists one or more events atomically in memory.
func (s *InMemoryStore) Save(ctx context.Context, events ...*Event) error {
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

// LoadEvents retrieves all events (optionally filtered) as []PersistedEvent.
func (s *InMemoryStore) LoadEvents(ctx context.Context, filters ...EventFilter) ([]PersistedEvent, error) {
	s.RLock()
	defer s.RUnlock()

	var f EventFilter
	if len(filters) > 0 {
		f = filters[0]
	}
	filtered := applyInMemoryFilters(s.data, f)

	// Transform []Event => []PersistedEvent
	result := make([]PersistedEvent, len(filtered))
	for i, evt := range filtered {
		result[i] = InMemoryEvent{evt}
	}

	return result, nil
}

// LoadEventsInBatches loads events in ascending global version order in batches.
func (s *InMemoryStore) LoadEventsInBatches(
	ctx context.Context,
	batchSize int,
	handler func([]PersistedEvent) error,
	filters ...EventFilter,
) error {
	s.RLock()
	defer s.RUnlock()

	var f EventFilter
	if len(filters) > 0 {
		f = filters[0]
	}
	filtered := applyInMemoryFilters(s.data, f)

	offset := 0
	for {
		if offset >= len(filtered) {
			break
		}
		end := offset + batchSize
		if end > len(filtered) {
			end = len(filtered)
		}
		batchSlice := filtered[offset:end]

		// Convert to PersistedEvent
		pEvents := make([]PersistedEvent, len(batchSlice))
		for i, evt := range batchSlice {
			pEvents[i] = InMemoryEvent{evt}
		}

		if err := handler(pEvents); err != nil {
			return err
		}
		offset += len(batchSlice)
	}
	return nil
}

// GlobalVersion returns the highest global version in this test store.
func (s *InMemoryStore) GlobalVersion(ctx context.Context) (int64, error) {
	s.RLock()
	defer s.RUnlock()

	if len(s.data) == 0 {
		return 0, nil
	}
	lastEvent := s.data[len(s.data)-1]
	return lastEvent.GlobalVersion, nil
}

// IsNewEvent checks if an event is new by aggregate ID and version.
func (s *InMemoryStore) IsNewEvent(event Event) bool {
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
func (s *InMemoryStore) Exists(ctx context.Context, eventID uuid.UUID) (bool, error) {
	s.RLock()
	defer s.RUnlock()

	for _, event := range s.data {
		if event.ID == eventID {
			return true, nil
		}
	}
	return false, nil
}

// SaveSnapshot persists a snapshot of an aggregate in memory.
func (s *InMemoryStore) SaveSnapshot(ctx context.Context, agg Aggregate) error {
	snapshot := NewSnapshot(agg)
	return s.Save(ctx, &snapshot)
}

// LoadSnapshot retrieves the latest snapshot for an aggregate as a PersistedEvent.
func (s *InMemoryStore) LoadSnapshot(ctx context.Context, aggregateType, aggregateID string) (PersistedEvent, error) {
	s.RLock()
	defer s.RUnlock()

	// Find the most recent snapshot. For simplicity, we'll return the *first* we find
	// matching your criteria. Or you might iterate to find the max version.
	for i := len(s.data) - 1; i >= 0; i-- {
		event := s.data[i]
		if event.AggregateID == aggregateID &&
			event.Kind == EventKindSnapshot &&
			event.AggregateType == aggregateType {
			// Return it wrapped as PersistedEvent
			return InMemoryEvent{event}, nil
		}
	}
	return nil, errors.New("record not found")
}

// DeleteEvent removes an event by ID.
func (s *InMemoryStore) DeleteEvent(ctx context.Context, eventID uuid.UUID) error {
	s.Lock()
	defer s.Unlock()

	for i, event := range s.data {
		if event.ID == eventID {
			s.data = append(s.data[:i], s.data[i+1:]...)
			return nil
		}
	}
	return errors.New("record not found")
}

var _ EventStore = (*InMemoryStore)(nil)

func applyInMemoryFilters(events []Event, f EventFilter) []Event {
	var out []Event
	for _, e := range events {
		gv := e.Version

		// Filter by fromVersion/toVersion
		if f.FromVersion > 0 && gv < int64(f.FromVersion) {
			continue
		}
		if f.ToVersion > 0 && gv > int64(f.ToVersion) {
			continue
		}
		// Filter by time
		if !f.FromTime.IsZero() && e.CreatedAt.Before(f.FromTime) {
			continue
		}
		if !f.ToTime.IsZero() && e.CreatedAt.After(f.ToTime) {
			continue
		}
		// Filter by aggregate type/ID, event type, etc. if needed
		// ...

		out = append(out, e)
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}
	return out
}
