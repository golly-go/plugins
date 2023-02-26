package eventsource

import "github.com/slimloans/golly"

type TestCommandEmpty struct {
	Test bool `json:"test"`
}

func (TestCommandEmpty) Perform(golly.Context, Aggregate) error  { return nil }
func (TestCommandEmpty) Validate(golly.Context, Aggregate) error { return nil }
