package eventsource

import (
	"github.com/golly-go/golly"
)

type testAggregate struct {
	AggregateBase

	repo Repository
}

func (a *testAggregate) Repo(golly.Context) Repository { return a.repo }
func (*testAggregate) Topic() string                   { return "test/topic" }
func (*testAggregate) Apply(golly.Context, Event)      {}
func (*testAggregate) GetID() string                   { return "" }
func (*testAggregate) SetID(id string)                 {}

type testRepostoryBase struct {
	loadCalled       int
	saveCalled       int
	trasactionCalled int
}

func (r *testRepostoryBase) Load(golly.Context, interface{}) error {
	r.loadCalled++
	return nil
}

func (r *testRepostoryBase) Save(golly.Context, interface{}) error {
	r.saveCalled++
	return nil
}

func (r *testRepostoryBase) Transaction(ctx golly.Context, handler func(golly.Context, Repository) error) error {
	r.trasactionCalled++

	return handler(ctx, r)
}

func (r *testRepostoryBase) IsNewRecord(interface{}) bool {
	return true
}
