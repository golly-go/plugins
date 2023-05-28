package eventsource

import (
	"github.com/golly-go/golly"
	"gorm.io/gorm"
)

type TestAggregate struct {
	AggregateBase
}

func (*TestAggregate) Topic() string                      { return "test.aggregate" }
func (*TestAggregate) Type() string                       { return "test_aggregates" }
func (*TestAggregate) Repo(golly.Context) Repository      { return TestRepostory{} }
func (*TestAggregate) GetID() string                      { return "" }
func (*TestAggregate) SetID(id string)                    {}
func (*TestAggregate) Apply(ctx golly.Context, evt Event) {}

type TestRepostory struct{}

func (TestRepostory) Load(golly.Context, interface{}) error {
	return nil
}

func (TestRepostory) LoadScope(*gorm.DB, interface{}) error {
	return nil
}
func (TestRepostory) Save(golly.Context, interface{}) error {
	return nil
}

func (t TestRepostory) IsNewRecord(obj interface{}) bool            { return true }
func (t TestRepostory) Transaction(fn func(Repository) error) error { return fn(t) }
