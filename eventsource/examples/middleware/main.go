package main

import (
	"context"
	"time"

	"github.com/golly-go/plugins/eventsource"
)

func main() {
	// Create engine with store and sync bus
	engine := eventsource.NewEngine(
		eventsource.WithStore(&eventsource.InMemoryStore{}),
		eventsource.WithBus(eventsource.NewSyncBus()),
	)

	// Publish an example event
	engine.Send(context.TODO(), eventsource.Event{
		Type: "UserAction",
		Data: map[string]interface{}{
			"action": "login",
			"userID": "user_123",
		},
	})

	// Wait a moment to see the output
	time.Sleep(time.Millisecond * 100)
}
