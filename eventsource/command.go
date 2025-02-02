package eventsource

import (
	"fmt"

	"github.com/golly-go/golly"
)

var (
	ErrorRepositoryIsNil         = fmt.Errorf("eventstore is nil for aggregate")
	ErrorAggregateNotFound       = fmt.Errorf("aggregate is not found in registry")
	ErrorNoEventsFound           = fmt.Errorf("no events found matching this aggregation")
	ErrorNoAggregateID           = fmt.Errorf("no aggregate id was defined after processing events (no such stream)")
	ErrorAggregateNotInitialized = fmt.Errorf("aggregate was not created properly and IsNewRecord is still true after events")
)

type Command interface {
	Perform(*golly.Context, Aggregate) error
}

type CommandValidator interface {
	Validate(*golly.Context, Aggregate) error
}

type CommandRollback interface {
	Rollback(*golly.Context, Aggregate, error)
}
