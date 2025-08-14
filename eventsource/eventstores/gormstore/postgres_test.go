package gormstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/segmentio/encoding/json"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/golly-go/plugins/orm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
)

type TestAggregate struct {
	eventsource.AggregateBase

	ID           string
	AppliedEvent *Event
}

func (ta *TestAggregate) ApplytestEvent(event Event)         { ta.AppliedEvent = &event }
func (ta *TestAggregate) GetID() string                      { return ta.ID }
func (ta *TestAggregate) SetID(id string)                    { ta.ID = id }
func (ta *TestAggregate) EventStore() eventsource.EventStore { return nil }
func (ta *TestAggregate) IsNewRecord() bool                  { return false }

type TestEvent struct {
	Key string `json:"key"`
}

func TestGormRepository_Save(t *testing.T) {
	store := Store{}

	engine := eventsource.NewEngine(&store)
	engine.
		Aggregates().
		Register(&TestAggregate{}, []any{&TestEvent{}})

	ctx := orm.CreateTestContext(golly.NewContext(context.Background()), Event{})
	defer orm.Close(ctx)

	testEvent := eventsource.Event{
		ID:            uuid.New(),
		CreatedAt:     time.Now(),
		Type:          "gorm.TestEvent",
		AggregateID:   uuid.New().String(),
		AggregateType: "gorm.TestAggregate",
		Version:       1,
		Data:          TestEvent{Key: "value"},
		Metadata: eventsource.Metadata{
			"UserID": "123",
		},
	}

	db := orm.DB(ctx)

	err := store.Save(ctx, &testEvent)
	assert.NoError(t, err)

	var loaded Event
	err = db.Model(&loaded).First(&loaded).Error

	assert.NoError(t, err)
	assert.Equal(t, testEvent.ID, loaded.ID)

	var unmarshaledData map[string]interface{}
	json.Unmarshal(loaded.RawData.RawMessage, &unmarshaledData)
	assert.Equal(t, "value", unmarshaledData["key"])
}

func TestGormRepository_IncrementEventSourceVersion(t *testing.T) {
	ctx := orm.CreateTestContext(golly.NewContext(context.Background()), EventSourceVersion{})
	defer orm.Close(ctx)

	t.Run("already exists", func(t *testing.T) {
		orm.DB(ctx).Create(&EventSourceVersion{ID: "test", Version: 1})

		version, err := IncrementEventSourceVersion(ctx, "test")
		assert.NoError(t, err)

		assert.Equal(t, int64(2), version)
	})

	t.Run("does not exist", func(t *testing.T) {
		version, err := IncrementEventSourceVersion(ctx, "does-not-exist")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), version)
	})
}

func TestGormRepository_GetEventSourceVersion(t *testing.T) {
	ctx := orm.CreateTestContext(golly.NewContext(context.Background()), EventSourceVersion{})
	defer orm.Close(ctx)

	t.Run("exists", func(t *testing.T) {
		orm.DB(ctx).Create(&EventSourceVersion{ID: "test", Version: 1})

		version, err := GetEventSourceVersion(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), version)
	})

	t.Run("does not exist", func(t *testing.T) {
		version, err := GetEventSourceVersion(ctx, "does-not-exist")
		assert.Error(t, err)
		assert.Equal(t, int64(0), version)
	})
}

func TestGormRepository_SetEventSourceVersion(t *testing.T) {
	ctx := orm.CreateTestContext(golly.NewContext(context.Background()), EventSourceVersion{})
	defer orm.Close(ctx)

	t.Run("exists", func(t *testing.T) {
		err := SetEventSourceVersion(ctx, "test", 2)
		assert.NoError(t, err)

		version, err := GetEventSourceVersion(ctx, "test")
		assert.NoError(t, err)
		assert.Equal(t, int64(2), version)
	})

	t.Run("does not exist", func(t *testing.T) {
		err := SetEventSourceVersion(ctx, "does-not-exist", 1)
		assert.NoError(t, err)

		version, err := GetEventSourceVersion(ctx, "does-not-exist")
		assert.NoError(t, err)
		assert.Equal(t, int64(1), version)
	})
}

func seedEvents(ctx context.Context, t *testing.T, n int) []Event {
	t.Helper()
	db := orm.DB(ctx)
	// ensure schema
	assert.NoError(t, db.AutoMigrate(&Event{}))

	events := make([]Event, n)
	for i := 0; i < n; i++ {
		ev := eventsource.Event{
			ID:            uuid.New(),
			Type:          fmt.Sprintf("TestEvent_%d", i),
			AggregateID:   uuid.New().String(),
			AggregateType: "TestAgg",
			Version:       int64(i + 1),
			Data:          map[string]any{"i": i},
		}
		mapped, err := mapBatchToDB([]*eventsource.Event{&ev})
		assert.NoError(t, err)
		events[i] = mapped[0]
	}
	// set global versions incrementally
	for i := range events {
		events[i].Event.GlobalVersion = int64(i + 1)
	}
	assert.NoError(t, db.Create(&events).Error)
	return events
}

func TestQuery(t *testing.T) {
	ctx := orm.CreateTestContext(golly.NewContext(context.Background()), Event{})
	defer orm.Close(ctx)

	seeded := seedEvents(ctx, t, 5)

	// scope: order by global_version desc, limit 3
	scopes := []func(*gorm.DB) *gorm.DB{
		func(db *gorm.DB) *gorm.DB { return db.Order("global_version DESC").Limit(3) },
	}

	out, err := Query(ctx, scopes...)
	assert.NoError(t, err)
	assert.Len(t, out, 3)

	// verify ordering by checking the first returned has max global_version
	first := out[0].(*Event)
	assert.Equal(t, seeded[len(seeded)-1].GlobalVersion(), first.GlobalVersion())
}

func TestQueryInBatches(t *testing.T) {
	ctx := orm.CreateTestContext(golly.NewContext(context.Background()), Event{})
	defer orm.Close(ctx)

	_ = seedEvents(ctx, t, 10)

	var seen int
	var batches int
	err := QueryInBatches(ctx, 4, func(batch []eventsource.PersistedEvent) error {
		batches++
		seen += len(batch)
		// ensure items are *Event
		for _, pe := range batch {
			_, ok := pe.(*Event)
			assert.True(t, ok)
		}
		return nil
	}, func(db *gorm.DB) *gorm.DB { return db.Order("global_version ASC") })

	assert.NoError(t, err)
	assert.Equal(t, 10, seen)
	assert.Equal(t, 3, batches) // 4 + 4 + 2
}
