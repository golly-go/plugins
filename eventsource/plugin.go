package eventsource

import (
	"github.com/golly-go/golly"
	"github.com/spf13/cobra"
)

type EventsourcePlugin struct {
	store EventStore
}

func (*EventsourcePlugin) Deinitialize(app *golly.Application) error { return nil }
func (*EventsourcePlugin) Commands() []*cobra.Command                { return []*cobra.Command{} }
func (e *EventsourcePlugin) Initialize(app *golly.Application) error { return nil }

var _ golly.Plugin = (*EventsourcePlugin)(nil)
