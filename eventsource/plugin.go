package eventsource

import (
	"github.com/golly-go/golly"
	"github.com/spf13/cobra"
)

// // Plugin defines the structure for a plugin in the Golly framework.
// // Plugins should implement initialization, command provision, and deinitialization logic.
// type Plugin interface {
// 	// Initialize is called when the plugin is loaded into the application.
// 	// This is where resources such as database connections or configurations should be initialized.
// 	Initialize(app *Application) error

// 	// Commands returns the list of CLI commands provided by the plugin.
// 	Commands() []*cobra.Command

// 	// Deinitialize is called when the application is shutting down.
// 	// This is where resources should be cleaned up, such as closing database connections or committing transactions.
// 	Deinitialize(app *Application) error
// }

// Example usuage in another app
// func Initializer(app *golly.Application) error {
// 	Engine.Start()

// 	app.On(golly.EventShutdown, func(*golly.Context, *golly.Event) {
// 		Engine.Stop()
// 	})

// 	return nil
// }

const (
	PluginName = "eventsource"
)

// Plugin implements the Plugin interface for the eventsource
type EventsourcePlugin struct {
	config Options
	engine *Engine
}

// NewPlugin creates a new Plugin with the given store
func NewPlugin(store EventStore) *EventsourcePlugin {
	return &EventsourcePlugin{
		engine: NewEngine(store),
	}
}

// Name returns the name of the plugin
func (p *EventsourcePlugin) Name() string { return PluginName }

// Engine returns the initialized engine
func (p *EventsourcePlugin) Engine() *Engine { return p.engine }

// ConfigureEngine allows additional configuration or usage of the engine
func (p *EventsourcePlugin) ConfigureEngine(configure func(*Engine)) *EventsourcePlugin {
	configure(p.engine)
	return p
}

// Initialize sets up the engine and starts it
func (p *EventsourcePlugin) Initialize(app *golly.Application) error {
	p.engine.Start()
	return nil
}

// Deinitialize stops the engine
func (p *EventsourcePlugin) Deinitialize(app *golly.Application) error {
	p.engine.Stop()
	return nil
}

// Commands returns the list of CLI commands provided by the plugin
func (p *EventsourcePlugin) Commands() []*cobra.Command {
	return []*cobra.Command{}
}

// Ensure Plugin implements the Plugin interface
var _ golly.Plugin = (*EventsourcePlugin)(nil)

// Plugin returns the eventsource plugin from the application
func Plugin(app *golly.Application) *EventsourcePlugin {
	if p, ok := app.Plugins().Get(PluginName).(*EventsourcePlugin); ok {
		return p
	}
	return nil
}
