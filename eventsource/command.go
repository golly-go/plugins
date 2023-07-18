package eventsource

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
)

type Command interface {
	Perform(golly.Context, Aggregate) error
}

type CommandValidator interface {
	Validate(golly.Context, Aggregate) error
}

func Call(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	repo := ag.Repo(ctx)

	if validator, ok := cmd.(CommandValidator); ok {
		if err := validator.Validate(ctx, ag); err != nil {
			return errors.WrapUnprocessable(err)
		}
	}

	if err := LoadIfNotNew(ctx, ag); err != nil {
		return err
	}

	return repo.Transaction(func(repo Repository) error {
		return Execute(ctx, ag, cmd, metadata)
	})
}

func LoadIfNotNew(ctx golly.Context, ag Aggregate) error {
	repo := ag.Repo(ctx)

	if !repo.IsNewRecord(ag) {
		if err := repo.Load(ctx, ag); err != nil {
			return errors.WrapNotFound(err)
		}
	}
	return nil
}

// Execute executs the command assuming all the aggregates are loaded
func Execute(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	repo := ag.Repo(ctx)

	if err := cmd.Perform(ctx, ag); err != nil {
		return errors.WrapUnprocessable(err)
	}

	changes := ag.Changes().Uncommited()

	if changes.HasCommited() {
		if err := repo.Save(ctx, ag); err != nil {
			return errors.WrapUnprocessable(err)
		}
	}

	for _, change := range changes {
		change.AggregateID = ag.GetID()
		change.AggregateType = ag.Type()

		change.MarkCommited()

		change.Metadata.Merge(metadata)

		if eventBackend != nil && ag.Topic() != "" {
			if change.commit {
				if err := eventBackend.Save(ctx, &change); err != nil {
					return errors.WrapGeneric(err)
				}
			}
		}
		eventBackend.PublishEvent(ctx, ag, changes...)
	}

	return nil
}
