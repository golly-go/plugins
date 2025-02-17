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

// Plugin implements the Plugin interface for the eventsource
type Plugin struct {
	config Options
	engine *Engine
}

// NewPlugin creates a new Plugin with the given store
func NewPlugin(store EventStore) *Plugin {
	return &Plugin{
		engine: NewEngine(store),
	}
}

// Name returns the name of the plugin
func (p *Plugin) Name() string { return "eventsource" }

// Engine returns the initialized engine
func (p *Plugin) Engine() *Engine { return p.engine }

// ConfigureEngine allows additional configuration or usage of the engine
func (p *Plugin) ConfigureEngine(configure func(*Engine)) *Plugin {
	configure(p.engine)
	return p
}

// Initialize sets up the engine and starts it
func (p *Plugin) Initialize(app *golly.Application) error {
	p.engine.Start()
	return nil
}

// Deinitialize stops the engine
func (p *Plugin) Deinitialize(app *golly.Application) error {
	p.engine.Stop()
	return nil
}

// Commands returns the list of CLI commands provided by the plugin
func (p *Plugin) Commands() []*cobra.Command {
	return []*cobra.Command{}
}

// Ensure Plugin implements the Plugin interface
var _ golly.Plugin = (*Plugin)(nil)
