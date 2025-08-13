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
	Metadata      Metadata
}

// NewInMemoryEngine creates a new in-memory engine with the given options.
// for testing purposes.
func NewInMemoryEngine(opts TestEngineOptions) *Engine {
	engine := NewEngine(&InMemoryStore{
		data: buildInMemoryEvents(opts.Data),
	})

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
		} else if data, ok := evt.(Event); ok {
			events[i] = data
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
	evt.GlobalVersion = version

	return evt
}
