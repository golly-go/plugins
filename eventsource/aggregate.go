package eventsource

import (
	"github.com/slimloans/golly"
)

type Aggregate interface {
	Repo(golly.Context) Repository

	Apply(golly.Context, Event)

	Type() string
	Topic() string

	IncrementVersion()

	Changes() Events
	Append(...Event)
	ClearChanges()

	GetVersion() uint

	GetID() string
	SetID(string)
}

// AggregateBase holds the base aggregate for the db
type AggregateBase struct {
	Version uint `json:"version"`

	changes Events
}

func (ab *AggregateBase) IncrementVersion() {
	ab.Version++
}

// GetID return the aggregatebase id
func (ab *AggregateBase) GetVersion() uint {
	return ab.Version
}

func (ab *AggregateBase) Changes() Events {
	return ab.changes
}

func (ab *AggregateBase) ClearChanges() {
	ab.changes = []Event{}
}

func (ab *AggregateBase) Append(events ...Event) {
	ab.changes = append(ab.changes, events...)
}

func Apply(ctx golly.Context, aggregate Aggregate, edata interface{}) {
	ApplyExt(ctx, aggregate, edata, nil, true)
}

func NoCommit(ctx golly.Context, aggregate Aggregate, edata interface{}) {
	ApplyExt(ctx, aggregate, edata, nil, false)
}

func ApplyExt(ctx golly.Context, aggregate Aggregate, edata interface{}, meta Metadata, commit bool) {
	if edata == nil {
		return
	}

	event := NewEvent(edata)
	event.commit = commit
	event.Metadata.Merge(meta)

	aggregate.Apply(ctx, event)

	if commit {
		aggregate.IncrementVersion()
	}

	event.Version = aggregate.GetVersion()

	aggregate.Append(event)
}
