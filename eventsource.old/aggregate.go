package eventsource

import (
	"github.com/golly-go/golly"
)

type Aggregate interface {
	Repo(golly.Context) Repository

	Apply(Event)

	Topic() string

	IncrementVersion()

	Changes() Events
	Append(...Event)
	ClearChanges()

	GetVersion() uint

	GetID() string
	SetID(string)

	Persisted() bool
	SetPersisted()
}

var (
	identityFunc func(golly.Context) any = nil
)

func SetIdentityFunc(fn func(golly.Context) any) {
	identityFunc = fn
}

// AggregateBase holds the base aggregate for the db
type AggregateBase struct {
	Version uint `json:"version"`

	changes Events `gorm:"-" bson:"-"`

	// TODO:
	// Events Events `gorm:"-" bson:"events"`

	persisted bool
}

func (ab *AggregateBase) IncrementVersion() {
	ab.Version++
}

func (ab *AggregateBase) SetPersisted() {
	ab.persisted = true
}

func (ab *AggregateBase) Persisted() bool {
	return ab.persisted
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
