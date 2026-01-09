package eventsource

import (
	"bytes"
	"fmt"
	"time"

	"github.com/segmentio/encoding/json"

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
	ID            uuid.UUID   `json:"id"`
	Kind          EventKind   `json:"kind"`
	CreatedAt     time.Time   `json:"createdAt"`
	Type          string      `json:"eventType" gorm:"index:idx_events_type"`
	AggregateID   string      `json:"aggregateId" gorm:"index:idx_events_aggregate"`
	AggregateType string      `json:"aggregateType" gorm:"index:idx_events_aggregate"`
	Version       int64       `json:"version"`
	GlobalVersion int64       `json:"globalVersion"`
	State         EventState  `json:"state,omitempty" gorm:"-"`
	Data          interface{} `json:"data" gorm:"-"`
	TenantID      string      `json:"tenantID"`
	UserID        string      `json:"userID"`
	Topic         string      `json:"topic"`
}

type PersistedEvent interface {
	Hydrate(*Engine) (Event, error)
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
func NewEvent(data any, state EventState) Event {
	id, _ := uuid.NewV7()

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
		Kind:      EventKindEvent,
	}
}

type Events []Event

func (evts Events) ByState(state EventState) Events {
	events := Events{}

	for pos := range evts {
		if evts[pos].InState(state) { // Use the provided state parameter
			events = append(events, evts[pos])
		}
	}

	return events
}

func (evts Events) Ready() Events {
	return evts.ByState(EventStateReady)
}

func (evts Events) Uncommitted() Events {
	return evts.ByState(EventStateApplied)
}

func (evts Events) Completed() Events {
	return evts.ByState(EventStateCompleted)
}

func (evts Events) Ptr() []*Event {
	events := make([]*Event, len(evts))
	for pos := range evts {
		events[pos] = &evts[pos]

	}
	return events
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

func (e *Event) Hydrate(engine *Engine, data, metadata any) error {

	// Example: If you do special handling for snapshots, you can branch here:
	if e.Kind == "snapshot" {
		// Just an example approach:
		snap := AggregateSnapshotted{}
		if err := unmarshal(&snap, data); err != nil {
			return err
		}

		e.Data = snap
		return nil
	}

	// Otherwise, it's a normal event
	if e.Type == "" || e.AggregateType == "" {
		return fmt.Errorf("missing event type or aggregate type on event ID=%s", e.ID)
	}

	codec := engine.aggregates.GetEventCodec(e.AggregateType, e.Type)
	if codec == nil {
		return fmt.Errorf("no codec found for aggregate=%s, event=%s", e.AggregateType, e.Type)
	}

	val, err := codec.UnmarshalFn(data)
	if err != nil {
		return err
	}

	e.Data = val
	return nil
}

func unmarshal(instance, raw any) error {
	switch v := raw.(type) {
	case json.RawMessage:
		if err := json.Unmarshal(v, instance); err != nil {
			return err
		}
	case string:
		if err := json.Unmarshal([]byte(v), instance); err != nil {
			return err
		}
	case []byte:
		if err := json.Unmarshal(v, instance); err != nil {
			return err
		}
	default:
		return fmt.Errorf("do not know how to handle data %v", v)
	}

	return nil
}

func EventDataToMap(data any) (map[string]any, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(jsonData))
	decoder.UseNumber()

	var m map[string]any
	if err := decoder.Decode(&m); err != nil {
		return nil, err
	}

	return m, nil
}
