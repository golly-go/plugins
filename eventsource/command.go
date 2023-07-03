package eventsource

import (
	"encoding/json"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"github.com/sirupsen/logrus"
)

type Command interface {
	Perform(golly.Context, Aggregate) error
	Validate(golly.Context, Aggregate) error
}

func Call(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	repo := ag.Repo(ctx)

	if !repo.IsNewRecord(ag) {
		if err := repo.Load(ctx, ag); err != nil {
			return errors.WrapNotFound(err)
		}
	}

	if err := cmd.Validate(ctx, ag); err != nil {
		return errors.WrapUnprocessable(err)
	}

	return repo.Transaction(func(repo Repository) error {
		if err := cmd.Perform(ctx, ag); err != nil {
			return errors.WrapUnprocessable(err)
		}

		changes := ag.Changes()

		if changes.HasCommited() {
			if err := repo.Save(ctx, ag); err != nil {
				return errors.WrapUnprocessable(err)
			}
		}

		for _, change := range changes {
			change.AggregateID = ag.GetID()
			change.AggregateType = ag.Type()

			change.Metadata.Merge(metadata)

			if eventBackend != nil && ag.Topic() != "" {
				if change.commit {
					if err := eventBackend.Save(ctx, &change); err != nil {
						return errors.WrapGeneric(err)
					}
				}

				if ctx.Logger().Logger.IsLevelEnabled(logrus.DebugLevel) {
					x, _ := json.Marshal(change)
					ctx.Logger().Debugf("[publish: %s] %s", ag.Topic(), string(x))
				}

				eventBackend.PublishEvent(ctx, ag, change)
			}
		}
		return nil
	})
}
