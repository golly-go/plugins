package eventsource

type AggregateDefinition struct {
	Aggregate Aggregate
	Events    []any
}

type TestEngineOptions struct {
	Aggregates  []AggregateDefinition
	Projections []Projection
	Events      []any
}

// NewInMemoryEngine creates a new in-memory engine with the given options.
// for testing purposes.
func NewInMemoryEngine(opts TestEngineOptions) *Engine {
	events := make([]Event, len(opts.Events))
	if opts.Events != nil {
		for i, evt := range opts.Events {
			events[i] = NewEvent(evt, EventStateReady, nil)
			events[i].Data = evt
		}
	}

	engine := NewEngine(&InMemoryStore{
		data: events,
	})

	for _, agg := range opts.Aggregates {
		engine.RegisterAggregate(agg.Aggregate, agg.Events)
	}

	for _, proj := range opts.Projections {
		engine.RegisterProjection(proj)
	}

	return engine
}
