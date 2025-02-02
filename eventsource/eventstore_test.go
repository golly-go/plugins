package eventsource

import (
	"context"

	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
)

type testPersistedEvent struct {
	// Arbitrary fields you may want to track for the test
	ID         string
	Raw        []byte
	HydrateErr error // To simulate an error during Hydrate
}

// Hydrate implements PersistedEvent.
// It either returns an error (if HydrateErr is non-nil)
// or a basic Event with your chosen test fields.
func (t *testPersistedEvent) Hydrate(eng *Engine) (Event, error) {
	if t.HydrateErr != nil {
		return Event{}, t.HydrateErr
	}
	// Construct a minimal Event for testing.
	// In a real test, you might parse t.Raw or set fields from t.ID, etc.
	evt := Event{
		ID:   uuid.New(),      // For example, generate a new UUID or parse from t.ID
		Type: "TestEventType", // Hard-coded or from your raw data
		Data: t.Raw,           // Possibly store the raw bytes in Data
	}
	return evt, nil
}

// MockStore is a mock implementation of the EventStore interface.
type MockStore struct {
	mock.Mock
}

func (m *MockStore) Save(ctx context.Context, events ...*Event) error {
	args := m.Called(ctx, events)
	return args.Error(0)
}

func (m *MockStore) LoadEvents(ctx context.Context, filters ...EventFilter) ([]PersistedEvent, error) {
	args := m.Called(ctx, filters)
	if events, ok := args.Get(0).([]PersistedEvent); ok {
		return events, args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *MockStore) LoadEventsInBatches(ctx context.Context, batchSize int,
	handler func([]PersistedEvent) error,
	filters ...EventFilter) error {

	args := m.Called(ctx, batchSize, handler, filters)
	return args.Error(0)
}

// The rest of the methods are stubs or we can mock them similarly
func (m *MockStore) GlobalVersion(ctx context.Context) (int64, error) {
	args := m.Called(ctx)
	return int64(args.Int(0)), args.Error(1)
}

func (m *MockStore) IsNewEvent(event Event) bool {
	args := m.Called(event)
	return args.Bool(0)
}

func (m *MockStore) Exists(ctx context.Context, eventID uuid.UUID) (bool, error) {
	args := m.Called(ctx, eventID)
	return args.Bool(0), args.Error(1)
}

func (m *MockStore) DeleteEvent(ctx context.Context, eventID uuid.UUID) error {
	args := m.Called(ctx, eventID)
	return args.Error(0)
}

func (m *MockStore) SaveSnapshot(ctx context.Context, snapshot Aggregate) error {
	args := m.Called(ctx, snapshot)
	return args.Error(0)
}

func (m *MockStore) LoadSnapshot(ctx context.Context, aggregateType, aggregateID string) (PersistedEvent, error) {
	args := m.Called(ctx, aggregateType, aggregateID)
	if pe, ok := args.Get(0).(PersistedEvent); ok {
		return pe, args.Error(1)
	}
	return nil, args.Error(1)
}
