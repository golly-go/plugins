package eventsource

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/google/uuid"
)

var errSnapshotNotFound = errors.New("record not found")

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
	lock sync.RWMutex

	SaveFail error

	gobalVersion int64
}

func (s *InMemoryStore) IncrementGlobalVersion(ctx context.Context) (int64, error) {
	ret := atomic.AddInt64(&s.gobalVersion, 1)
	return ret, nil
}

// Save persists one or more events atomically in memory.
func (s *InMemoryStore) Save(ctx context.Context, events ...*Event) error {
	if s.SaveFail != nil {
		return s.SaveFail
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	for _, evt := range events {
		// State is transient (gorm:"-" in gormstore); don't persist it.
		c := *evt
		c.State = ""
		s.data = append(s.data, c)
	}
	return nil
}

// LoadEvents retrieves all events (optionally filtered) as []PersistedEvent.
func (s *InMemoryStore) LoadEvents(ctx context.Context, filters ...EventFilter) ([]PersistedEvent, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

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
	s.lock.RLock()
	defer s.lock.RUnlock()

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
	s.lock.RLock()
	defer s.lock.RUnlock()

	if len(s.data) == 0 {
		return 0, nil
	}
	lastEvent := s.data[len(s.data)-1]
	return lastEvent.GlobalVersion, nil
}

// IsNewEvent checks if an event is new by aggregate ID and version.
func (s *InMemoryStore) IsNewEvent(event Event) bool {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, e := range s.data {
		if e.AggregateID == event.AggregateID && e.Version == event.Version {
			return false
		}
	}
	return true
}

// Exists checks if an event exists by its ID.
func (s *InMemoryStore) Exists(ctx context.Context, eventID uuid.UUID) (bool, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	for _, event := range s.data {
		if event.ID == eventID {
			return true, nil
		}
	}
	return false, nil
}

// SaveSnapshot persists a snapshot event, bypassing the OCC version check.
// Snapshots share the aggregate version of the last applied event so the
// standard Save path (which checks MAX(version)==desired-1) would conflict.
func (s *InMemoryStore) SaveSnapshot(ctx context.Context, snapshot Event) error {
	if snapshot.Kind != EventKindSnapshot {
		return errors.New("event is not a snapshot")
	}

	s.lock.Lock()
	defer s.lock.Unlock()

	s.data = append(s.data, snapshot)
	return nil
}

// LoadSnapshot retrieves the highest-version snapshot for an aggregate.
// Iterates the full set to find max version, matching gormstore's ORDER BY version DESC.
func (s *InMemoryStore) LoadSnapshot(ctx context.Context, aggregateType, aggregateID string) (PersistedEvent, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()

	var best *Event
	for i := range s.data {
		e := &s.data[i]
		if e.AggregateID != aggregateID ||
			e.AggregateType != aggregateType ||
			e.Kind != EventKindSnapshot {
			continue
		}
		if best == nil || e.Version > best.Version {
			best = e
		}
	}

	if best == nil {
		return nil, errSnapshotNotFound
	}
	return InMemoryEvent{*best}, nil
}

// DeleteEvent removes an event by ID.
func (s *InMemoryStore) DeleteEvent(ctx context.Context, eventID uuid.UUID) error {
	s.lock.Lock()
	defer s.lock.Unlock()

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
	n := 0
	for i := range events {
		if matchInMemory(&events[i], &f) {
			n++
			if f.Limit > 0 && n >= f.Limit {
				break
			}
		}
	}

	out := make([]Event, 0, n)
	for i := range events {
		if !matchInMemory(&events[i], &f) {
			continue
		}

		out = append(out, events[i])
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
	}

	if f.OrderByVersion {
		slices.SortFunc(out, func(a, b Event) int {
			if c := cmp.Compare(a.Version, b.Version); c != 0 {
				return c
			}
			return cmp.Compare(a.GlobalVersion, b.GlobalVersion)
		})
	}

	return out
}

func matchInMemory(e *Event, f *EventFilter) bool {
	// Filter by AggregateType
	if f.AggregateType != "" && e.AggregateType != f.AggregateType {
		return false
	}
	// Filter by AggregateID
	if f.AggregateID != "" && e.AggregateID != f.AggregateID {
		return false
	}
	// Filter by AggregateTypes
	if len(f.AggregateTypes) > 0 && !slices.Contains(f.AggregateTypes, e.AggregateType) {
		return false
	}
	// Filter by EventType
	if len(f.EventType) > 0 && !slices.Contains(f.EventType, e.Type) {
		return false
	}
	// Filter by Topics
	if len(f.Topics) > 0 && !slices.Contains(f.Topics, e.Topic) {
		return false
	}

	// Filter by version (aggregate-local)
	if f.FromVersion > 0 && e.Version < int64(f.FromVersion) {
		return false
	}
	if f.ToVersion > 0 && e.Version > int64(f.ToVersion) {
		return false
	}

	// Filter by GlobalVersion
	if f.FromGlobalVersion > 0 && e.GlobalVersion < int64(f.FromGlobalVersion) {
		return false
	}
	if f.ToGlobalVersion > 0 && e.GlobalVersion > int64(f.ToGlobalVersion) {
		return false
	}

	// Filter by time
	if !f.FromTime.IsZero() && e.CreatedAt.Before(f.FromTime) {
		return false
	}
	if !f.ToTime.IsZero() && e.CreatedAt.After(f.ToTime) {
		return false
	}

	// ExcludeKinds
	if len(f.ExcludeKinds) > 0 && slices.Contains(f.ExcludeKinds, e.Kind) {
		return false
	}

	return true
}

func NewInMemoryStore(data ...Event) *InMemoryStore {
	return &InMemoryStore{data: data}
}
