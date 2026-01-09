package eventsource

import (
	"context"

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

var (
	engineKey golly.ContextKey = "eventsource.engine"
)

type PluginOptions struct {
	store   EventStore
	engine  *Engine
	streams []StreamPublisher

	userInfoFunc func(context.Context) UserInfo
}

type PluginOption func(*PluginOptions)

func PluginWithUserInfoFunc(fnc func(context.Context) UserInfo) PluginOption {
	return func(opt *PluginOptions) { opt.userInfoFunc = fnc }
}

func PluginWithStore(store EventStore) PluginOption {
	return func(opt *PluginOptions) { opt.store = store }
}

func PluginWithEngine(engine *Engine) PluginOption {
	return func(opt *PluginOptions) { opt.engine = engine }
}

func PluginWithStreams(streams ...StreamPublisher) PluginOption {
	return func(opt *PluginOptions) { opt.streams = append(opt.streams, streams...) }
}

const (
	PluginName = "eventsource"
)

// Plugin implements the Plugin interface for the eventsource
type EventsourcePlugin struct{ engine *Engine }

// NewPlugin creates a new Plugin with the given store
func NewPlugin(opts ...PluginOption) *EventsourcePlugin {
	cfg := PluginOptions{}
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.userInfoFunc != nil {
		SetUserInfoFunc(cfg.userInfoFunc)
	}

	if cfg.engine == nil {
		// default engine with store only (in-memory stream for projections)
		cfg.engine = NewEngine(WithStore(cfg.store), WithStreams(cfg.streams...))
	}

	return &EventsourcePlugin{engine: cfg.engine}
}

// Name returns the name of the plugin
func (p *EventsourcePlugin) Name() string { return PluginName }

// Engine returns the initialized engine
func (p *EventsourcePlugin) Engine() *Engine { return p.engine }

// ConfigureEngine allows additional configuration or usage of the engine
func (p *EventsourcePlugin) Configure(configure func(*EventsourcePlugin)) *EventsourcePlugin {
	configure(p)
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
func (p *EventsourcePlugin) Commands() []*cobra.Command { return []*cobra.Command{} }

// Ensure Plugin implements the Plugin interface
var _ golly.Plugin = (*EventsourcePlugin)(nil)

// Plugin returns the eventsource plugin from the application
func Plugin() *EventsourcePlugin {
	if pm := golly.CurrentPlugins(); pm != nil {
		if p, ok := pm.Get(PluginName).(*EventsourcePlugin); ok {
			return p
		}
	}
	return nil
}

func DefaultEngine() *Engine {

	if plugin := Plugin(); plugin != nil {
		return plugin.engine
	}
	return nil
}

func GetEngine(tracker any) *Engine {
	p := golly.GetPlugin[*EventsourcePlugin](tracker, PluginName)
	if p == nil {
		return nil
	}

	return p.engine
}

func FromApp(app *golly.Application) *Engine {
	plugin := golly.GetPluginFromApp[*EventsourcePlugin](app, PluginName)
	if plugin == nil {
		return nil
	}
	return plugin.engine
}

func SetEngine(parent context.Context, engine *Engine) context.Context {
	return golly.WithValue(parent, engineKey, engine)
}
