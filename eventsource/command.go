package eventsource

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"github.com/golly-go/golly/utils"
)

type Command interface {
	Perform(golly.Context, Aggregate) error
}

type CommandValidator interface {
	Validate(golly.Context, Aggregate) error
}

func Call(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	repo := Repo(ctx, ag)

	if validator, ok := cmd.(CommandValidator); ok {
		if err := validator.Validate(ctx, ag); err != nil {
			return errors.WrapUnprocessable(err)
		}
	}

	if err := LoadIfNotNew(ctx, ag); err != nil {
		return err
	}

	return repo.Transaction(ctx, func(ctx golly.Context, repo Repository) error {
		return Execute(ctx, ag, cmd, metadata)
	})
}

func LoadIfNotNew(ctx golly.Context, ag Aggregate) error {
	repo := Repo(ctx, ag)

	if !repo.IsNewRecord(ag) {
		if err := repo.Load(ctx, ag); err != nil {
			return errors.WrapNotFound(err)
		}
	}
	return nil
}

func Repo(ctx golly.Context, ag Aggregate) Repository {
	// Tired of not being able to stub this - so now we can (This still doesnt fully help if someone runs)
	// something within their command that calls ag.Repo(ctx) - but it's a start
	if repo := repoFromContext(ctx); repo != nil {
		return repo
	}

	return ag.Repo(ctx)

}

// Execute executes the command, ensuring that events are saved to the backend before in-memory processing.
func Execute(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	repo := Repo(ctx, ag)

	// Perform the given command on the aggregate
	if err := cmd.Perform(ctx, ag); err != nil {
		return errors.WrapUnprocessable(err)
	}

	changes := ag.Changes().Uncommited()

	if len(changes) == 0 {
		return nil
	}

	// If there are uncommitted changes, first save them to the backend
	if !changes.HasCommited() {
		return nil
	}

	if err := repo.Save(ctx, ag); err != nil {
		return errors.WrapUnprocessable(err)
	}

	for _, change := range changes {
		change.AggregateID = ag.GetID()
		change.AggregateType = utils.GetTypeWithPackage(ag)
		change.MarkCommited()
		change.Metadata.Merge(metadata)

		// Save event to the backend event store (assuming it's committed)
		if eventBackend != nil && ag.Topic() != "" && change.commit {
			if err := eventBackend.Save(ctx, &change); err != nil {
				return errors.WrapGeneric(err)
			}
		}
	}

	// Only after confirming event persistence, invoke in-memory subscriptions
	if err := FireSubscription(ctx, ag, changes...); err != nil {
		return errors.WrapGeneric(err)
	}

	if eventBackend != nil {
		eventBackend.PublishEvent(ctx, ag, changes...)
	}

	return nil
}
