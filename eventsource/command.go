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

// Execute handles command execution, including loading the aggregate, replaying events, validating, and persisting changes.
func Execute(ctx *golly.Context, agg Aggregate, cmd Command) (err error) {
	var estore EventStore

	estore = agg.EventStore()
	if estore == nil {
		return handleExecutionError(ctx, agg, cmd, ErrorRepositoryIsNil)
	}

	if err = Replay(ctx, agg); err != nil {
		return handleExecutionError(ctx, agg, cmd, err)
	}

	if v, ok := cmd.(CommandValidator); ok {
		if err = v.Validate(ctx, agg); err != nil {
			return handleExecutionError(ctx, agg, cmd, err)
		}
	}

	// Perform the given command on the aggregate
	if err = cmd.Perform(ctx, agg); err != nil {
		return handleExecutionError(ctx, agg, cmd, err)
	}

	if agg.GetID() == "" {
		return handleExecutionError(ctx, agg, cmd, ErrorNoAggregateID)
	}

	// Apply changes to the aggregate
	agg.ProcessChanges(ctx, agg)

	if agg.IsNewRecord() {
		return handleExecutionError(ctx, agg, cmd, ErrorAggregateNotInitialized)
	}

	changes := agg.Changes().Uncommitted()
	if err = estore.Save(ctx, changes.Ptr()...); err != nil {
		return handleExecutionError(ctx, agg, cmd, err)
	}

	streamManager.Send(ctx, changes...)

	agg.Changes().MarkComplete()

	return nil
}

// handleExecutionError processes errors and rolls back if necessary
func handleExecutionError(ctx *golly.Context, agg Aggregate, cmd Command, err error) error {
	if agg != nil {
		agg.SetChanges(agg.Changes().MarkFailed())
	}

	if r, ok := cmd.(CommandRollback); ok {
		r.Rollback(ctx, agg, err)
	}

	return err
}
