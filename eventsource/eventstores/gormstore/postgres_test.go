package gormstore

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/golly-go/plugins/orm"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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
