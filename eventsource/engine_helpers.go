package eventsource

import "github.com/golly-go/golly"

type AggregateDefinition struct {
	Aggregate Aggregate
	Events    []any
}

type TestEngineOptions struct {
	Aggregates  []AggregateDefinition
	Projections []Projection
	Data        []any
}

type TestEventData struct {
	AggregateID   string
	AggregateType string
	Data          any
	// Optional metadata
	Metadata map[string]any

	// if not set, will be set to the version
	Version int64
	// if not set, will be set to the version
	GlobalVersion int64
}

// NewInMemoryEngine creates a new in-memory engine with the given options.
// for testing purposes.
func NewInMemoryEngine(opts TestEngineOptions) *Engine {
	engine := NewEngine(
		WithStore(&InMemoryStore{data: buildInMemoryEvents(opts.Data)}),
	)

	for _, agg := range opts.Aggregates {
		engine.RegisterAggregate(agg.Aggregate, agg.Events)
	}

	for _, proj := range opts.Projections {
		engine.RegisterProjection(proj)
	}

	return engine
}

func buildInMemoryEvents(data []any) []Event {
	events := make([]Event, len(data))
	for i, evt := range data {
		if data, ok := evt.(TestEventData); ok {
			events[i] = testEventToEvent(data, int64(i+1))
			continue
		}

		if data, ok := evt.(Event); ok {
			events[i] = data

			if events[i].GlobalVersion == 0 {
				events[i].GlobalVersion = int64(i + 1)
			}
		}
	}

	return events
}

func testEventToEvent(e TestEventData, version int64) Event {
	evt := NewEvent(e.Data, EventStateCompleted, e.Metadata)
	evt.Data = e.Data

	evt.AggregateID = e.AggregateID
	evt.AggregateType = e.AggregateType
	evt.Type = golly.TypeNoPtr(e.Data).String()

	if e.GlobalVersion == 0 {
		evt.GlobalVersion = version
	} else {
		evt.GlobalVersion = e.GlobalVersion
	}

	if e.Version == 0 {
		evt.Version = version
	} else {
		evt.Version = e.Version
	}

	return evt
}
