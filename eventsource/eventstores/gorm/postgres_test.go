package gorm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/golly-go/plugins/orm"
	"github.com/google/uuid"
	"github.com/jinzhu/gorm/dialects/postgres"
	"github.com/stretchr/testify/assert"
)

type TestAggregate struct {
	eventsource.AggregateBase

	ID           string
	AppliedEvent *Event
}

func (ta *TestAggregate) ApplytestEvent(event Event)         { ta.AppliedEvent = &event }
func (ta *TestAggregate) GetID() string                      { return ta.ID }
func (ta *TestAggregate) EventStore() eventsource.EventStore { return nil }
func (ta *TestAggregate) IsNewRecord() bool                  { return false }

type TestEvent struct {
	Key string `json:"key"`
}

func TestGormRepository_Save(t *testing.T) {
	eventsource.Aggregates().Register(&TestAggregate{}, []any{&TestEvent{}})

	ctx := orm.CreateTestContext(golly.NewContext(context.Background()), Event{})
	defer orm.Close(ctx)

	repo := GormEventStore{}

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

	err := repo.Save(ctx, &testEvent)
	assert.NoError(t, err)

	var loaded Event
	err = db.Model(&loaded).First(&loaded).Error

	assert.NoError(t, err)
	assert.Equal(t, testEvent.ID, loaded.ID)

	var unmarshaledData map[string]interface{}
	json.Unmarshal(loaded.RawData.RawMessage, &unmarshaledData)
	assert.Equal(t, "value", unmarshaledData["key"])
}

func TestEvent_AfterFind(t *testing.T) {
	eventsource.Aggregates().Register(&TestAggregate{}, []any{&TestEvent{}})

	rawData := TestEvent{Key: "test"}
	rawDataBytes, _ := json.Marshal(rawData)

	event := Event{
		Event: eventsource.Event{
			ID:            uuid.New(),
			CreatedAt:     time.Now(),
			Type:          "gorm.TestEvent",
			AggregateID:   uuid.New().String(),
			AggregateType: "gorm.TestAggregate",
			Version:       1,
		},
		RawData: postgres.Jsonb{RawMessage: rawDataBytes},
	}

	// Simulate Gorm AfterFind behavior
	err := event.AfterFind(nil)
	assert.NoError(t, err)

	x, ok := event.Data.(TestEvent)
	assert.True(t, ok)

	assert.Equal(t, "test", x.Key)
}
