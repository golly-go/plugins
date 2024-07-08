package eventsource

import (
	"github.com/go-playground/validator/v10"
	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"github.com/golly-go/golly/utils"
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

func Repo(ctx golly.Context, ag Aggregate) Repository {
	// Tired of not being able to stub this - so now we can (This still doesnt fully help if someone runs)
	// something within their command that calls ag.Repo(ctx) - but it's a start
	if repo := repoFromContext(ctx); repo != nil {
		return repo
	}

	return ag.Repo(ctx)

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

type CommandHandler interface {
	Call(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error
	Execute(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error
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

	if err := LoadIfNotNew(ctx, ag); err != nil {
		return err
	}

	return repo.Transaction(ctx, func(ctx golly.Context, repo Repository) error {
		return ch.Execute(ctx, ag, cmd, metadata)
	})
}

// Execute executes the command, ensuring that events are saved to the backend before in-memory processing.
func (DefaultCommandHandler) Execute(ctx golly.Context, ag Aggregate, cmd Command, metadata Metadata) error {
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

	for pos, change := range changes {
		change.AggregateID = ag.GetID()

		if inf, ok := ag.(AggregateType); ok {
			change.AggregateType = inf.Type()
		} else {
			change.AggregateType = utils.GetTypeWithPackage(ag)
		}

		change.MarkCommited()
		change.Metadata.Merge(metadata)

		// Save event to the backend event store (assuming it's committed)
		if eventBackend != nil && ag.Topic() != "" && change.commit {
			if err := eventBackend.Save(ctx, &change); err != nil {
				return errors.WrapGeneric(err)
			}
		}

		changes[pos] = change
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
