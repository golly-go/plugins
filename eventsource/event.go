package eventsource

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"github.com/golly-go/golly/utils"
	"github.com/google/uuid"
)

var (
	eventBackend EventBackend
)

type EventBackend interface {
	Repository

	PublishEvent(golly.Context, Aggregate, ...Event)
}

func SetEventRepository(backend EventBackend) {
	eventBackend = backend
}

type Metadata map[string]interface{}

func (m1 Metadata) Merge(m2 Metadata) {
	if len(m2) == 0 {
		return
	}

	if m1 == nil {
		m1 = Metadata{}
	}

	for k, v := range m2 {
		m1[k] = v
	}
}

type Event struct {
	ID        uuid.UUID `json:"id"`
	CreatedAt time.Time `json:"eventAt"`

	Event   string `json:"event"`
	Version uint   `json:"version"`

	AggregateID   string `json:"aggregateID"`
	AggregateType string `json:"aggregateType"`

	Data     interface{} `json:"data" gorm:"-"`
	Metadata Metadata    `json:"metadata" gorm:"-"`

	commit   bool
	commited bool
	publish  bool
}

type Events []Event

func (evts Events) HasCommited() bool {
	for _, event := range evts {
		if event.commit {
			return true
		}
	}
	return false
}

func (event *Event) MarkCommited() {
	event.commited = true
}

func (evts Events) Uncommited() Events {
	return evts.Filter(func(e Event) bool { return !e.commited })
}

func (evts Events) Filter(filterFnc func(e Event) bool) Events {
	ret := Events{}

	for _, event := range evts {
		if filterFnc(event) {
			ret = append(ret, event)
		}
	}
	return ret
}

func (evts Events) Find(finder func(e Event) bool) *Event {
	for _, event := range evts {
		if finder(event) {
			return &event
		}
	}
	return nil
}

func (evts Events) FindByName(name string) *Event {
	return evts.Find(func(event Event) bool {
		return strings.EqualFold(name, utils.GetType(event)) ||
			strings.EqualFold(name, utils.GetTypeWithPackage(event))
	})
}

func NewEvent(evtData interface{}) Event {
	id, _ := uuid.NewRandom()

	return Event{
		ID:        id,
		commit:    true,
		commited:  false,
		Event:     utils.GetTypeWithPackage(evtData),
		Metadata:  Metadata{},
		Data:      evtData,
		CreatedAt: time.Now(),
	}
}

// Decode return a deserialized event, ready to user
func UnmarshalEvent(data []byte) (*Event, error) {
	event := Event{}

	if err := json.Unmarshal(data, &event); err != nil {
		return nil, errors.WrapUnprocessable(err)
	}

	definition := registry.FindDefinition(event.AggregateType)

	if definition == nil {
		return nil, errors.WrapNotFound(fmt.Errorf("aggregate not found"))
	}

	var evt reflect.Type
	for _, e := range definition.Events {
		if event.Event == utils.GetTypeWithPackage(e) {
			evt = reflect.TypeOf(e)
			break
		}
	}

	dataValue := reflect.New(evt)

	marshal := dataValue.Elem().Addr()
	b, _ := json.Marshal(event.Data)

	if err := json.Unmarshal(b, marshal.Interface()); err != nil {
		return nil, errors.WrapGeneric(fmt.Errorf("error when decoding %s %#v", event.Event, err))
	}

	event.Data = marshal.Elem().Interface()

	return &event, nil
}
