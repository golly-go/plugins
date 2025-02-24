package eventsource

type AggregateDefinition struct {
	Aggregate Aggregate
	Events    []any
}

type TestEngineOptions struct {
	Aggregates  []AggregateDefinition
	Projections []Projection
}

// NewInMemoryEngine creates a new in-memory engine with the given options.
// for testing purposes.
func NewInMemoryEngine(opts TestEngineOptions) *Engine {
	engine := NewEngine(&InMemoryStore{})

	for _, agg := range opts.Aggregates {
		engine.RegisterAggregate(agg.Aggregate, agg.Events)
	}

	for _, proj := range opts.Projections {
		engine.RegisterProjection(proj)
	}

	return engine
}
