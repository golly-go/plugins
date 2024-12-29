package eventsource

import (
	"github.com/go-playground/validator/v10"
	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
)

const (
	commandHandlerKey golly.ContextKeyT = "commandHandler"
)

var (
	validate = validator.New(validator.WithRequiredStructEnabled())
)

type AggregateType interface {
	Type() string
}

type Command interface {
	Perform(golly.Context, Aggregate) error
}

type CommandValidator interface {
	Validate(golly.Context, Aggregate) error
}

type CommandRollback interface {
	Rollback(golly.Context, Aggregate, error)
}

func Repo(ctx golly.Context, ag Aggregate) Repository {
	// Tired of not being able to stub this - so now we can (This still doesnt fully help if someone runs)
	// something within their command that calls ag.Repo(ctx) - but it's a start
	if repo := repoFromContext(ctx); repo != nil {
		return repo
	}

	return ag.Repo(ctx)

}

type CommandHandler interface {
	Call(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error
}

type DefaultCommandHandler struct{}

func (ch DefaultCommandHandler) Call(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	repo := Repo(ctx, ag)

	if err := validate.Struct(cmd); err != nil {
		return errors.WrapUnprocessable(err)
	}

	if validator, ok := cmd.(CommandValidator); ok {
		if err := validator.Validate(ctx, ag); err != nil {
			return errors.WrapUnprocessable(err)
		}
	}

	return repo.Transaction(ctx, func(ctx golly.Context, repo Repository) error {
		err := ch.execute(ctx, ag, cmd, metadata)
		if err != nil {
			if rbk, ok := cmd.(CommandRollback); ok {
				rbk.Rollback(ctx, ag, err)
			}

			ag.Changes().MarkFailed()
		}
		return err
	})
}

// Execute executes the command, ensuring that events are saved to the backend before in-memory processing.
func (DefaultCommandHandler) execute(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	repo := Repo(ctx, ag)

	// Perform the given command on the aggregate
	if err := cmd.Perform(ctx, ag); err != nil {
		return errors.WrapUnprocessable(err)
	}

	changes := ag.Changes().Uncommited()

	if len(changes) == 0 || !changes.HasCommited() {
		return nil
	}

	if err := repo.Save(ctx, ag); err != nil {
		return errors.WrapUnprocessable(err)
	}

	ag.SetPersisted()

	for pos, change := range changes {
		change.AggregateID = ag.GetID()

		if identityFunc != nil {
			change.Identity = identityFunc(ctx)
		}

		change.Metadata.Merge(metadata)

		// Save event to the backend event store
		if eventBackend != nil && change.commit {
			if err := eventBackend.Save(ctx, &change); err != nil {
				return errors.WrapGeneric(err)
			}
		}

		changes[pos] = change
	}

	ag.ClearChanges()

	// Only after confirming event persistence, invoke in-memory subscriptions
	if err := FireSubscription(ctx, ag, changes...); err != nil {
		return errors.WrapGeneric(err)
	}

	if eventBackend != nil {
		eventBackend.PublishEvent(ctx, ag, changes...)
	}

	return nil
}

func Call(gctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
	return Handler(gctx).Call(gctx, ag, cmd, metadata)
}

func Handler(gctx golly.Context) CommandHandler {
	if handler, ok := gctx.Get(commandHandlerKey); ok {
		return handler.(CommandHandler)
	}
	return DefaultCommandHandler{}
}

var _ CommandHandler = DefaultCommandHandler{}
