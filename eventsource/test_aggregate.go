package eventsource

import (
	"github.com/golly-go/golly"
)

type TestAggregate struct {
	AggregateBase
}

func (*TestAggregate) Topic() string                      { return "test.aggregate" }
func (*TestAggregate) Repo(golly.Context) Repository      { return &MockRepository{} }
func (*TestAggregate) GetID() string                      { return "" }
func (*TestAggregate) SetID(id string)                    {}
func (*TestAggregate) Apply(ctx golly.Context, evt Event) {}
