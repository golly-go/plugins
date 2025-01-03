package eventsource

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
)

// EventState represents the state of an event using bitwise flags.
type EventState string

type EventKind string

const (
	EventStateReady     EventState = "ready"
	EventStateApplied   EventState = "applied"
	EventStateFailed    EventState = "failed"
	EventStateCompleted EventState = "completed"
	EventStateRetry     EventState = "retry"
	EventStateCanceled  EventState = "canceled"
)

const (
	EventKindSnapshot EventKind = "snapshot"
	EventKindEvent    EventKind = "event"
)

// Event represents a single event in the system.
type Event struct {
	ID        uuid.UUID `json:"id"`
	Kind      EventKind `json:"kind"`
	CreatedAt time.Time `json:"createdAt"`

	Type string `json:"eventType"`

	AggregateID   string `json:"aggregateId"`
	AggregateType string `json:"aggregateType"`

	Version int64 `json:"version"`

	State EventState `json:"state,omitempty" gorm:"-"`

	Data     any      `json:"data" gorm:"-"`
	Identity any      `json:"identity,omitempty" gorm:"-"`
	Metadata Metadata `json:"metadata" gorm:"-"`
}

// SetID assigns a UUID to the event.
func (e *Event) SetID(id uuid.UUID) {
	e.ID = id
}

// GetState returns the current state of the event.
func (e *Event) GetState() EventState {
	return e.State
}

// SetState adds a state to the event if it doesn't exist.
func (e *Event) SetState(s EventState) {
	e.State = s
}

// HasState checks if the event has a specific state.
func (e *Event) InState(s EventState) bool {
	return e.State == s
}

// NewEvent creates a new Event with the provided data and aggregate information.
func NewEvent(data any, state EventState, metadata Metadata) Event {
	id, _ := uuid.NewV7()

	if metadata == nil {
		metadata = make(Metadata)
	}

	s := EventStateReady
	if state != "" {
		s = state
	}

	return Event{
		ID:        id,
		CreatedAt: time.Now(),
		Type:      ObjectName(data),
		State:     s,
		Data:      data,
		Metadata:  metadata,
		Kind:      EventKindEvent,
	}
}

type Events []Event

func (evts Events) Ready() Events {
	return golly.Filter(evts, func(e Event) bool { return e.InState(EventStateReady) })
}

func (evts Events) Uncommitted() Events {
	return golly.Filter(evts, func(e Event) bool { return e.InState(EventStateApplied) })
}

func (evts Events) Completed() Events {
	return golly.Filter(evts, func(e Event) bool { return e.InState(EventStateCompleted) })
}

func (evts Events) Ptr() []*Event {
	return golly.Map(evts, func(e Event) *Event { return &e })
}

func (evts Events) MarkFailed() Events {
	for i := range evts {
		if !evts[i].InState(EventStateCompleted) {
			evts[i].SetState(EventStateFailed)
		}
	}
	return evts
}

func (evts Events) MarkComplete() Events {
	for i := range evts {
		if !evts[i].InState(EventStateCanceled) {
			evts[i].SetState(EventStateCompleted)
		}
	}
	return evts
}

// Hydrate reconstructs the event from JSON data and matches the appropriate type from the global aggregate registry.
func (e *Event) Hydrate(data any) error {
	if e.Type == "" || e.AggregateType == "" {
		return fmt.Errorf("cannot unmarshal Type and Aggregate not defined for event (%s)", e.ID)
	}

	var instance reflect.Value

	if e.Kind == EventKindSnapshot {
		instance = reflect.ValueOf(&AggregateSnapshottedEvent{})
	} else {
		// Use global aggregate registry
		evtType, exists := Aggregates().GetEventType(e.AggregateType, e.Type)
		if !exists {
			return fmt.Errorf("cannot unmarshal event type (%s) not registered for (%s)", e.Type, e.ID)
		}
		instance = reflect.New(evtType.Elem())
	}

	switch v := data.(type) {
	case json.RawMessage:
		if err := json.Unmarshal(v, instance.Interface()); err != nil {
			return err
		}
	case []byte:
		if err := json.Unmarshal(v, instance.Interface()); err != nil {
			return err
		}
	case map[string]interface{}:
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(b, instance.Interface()); err != nil {
			return err
		}
	default:
		return fmt.Errorf("do not know how to handle data %v", v)
	}

	e.Data = instance.Elem().Interface()
	return nil
}
