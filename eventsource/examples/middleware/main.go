package main

import (
	"context"
	"log"
	"time"

	"github.com/golly-go/plugins/eventsource"
)

func main() {
	// Create engine with store and sync bus
	engine := eventsource.NewEngine(
		eventsource.WithStore(&eventsource.InMemoryStore{}),
		eventsource.WithBus(eventsource.NewSyncBus()),
	)

	// Add middleware-like subscription on topic
	engine.Subscribe("UserAction", func(ctx context.Context, evt eventsource.Event) error {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("Recovered from panic: %v", r)
			}
		}()

		start := time.Now()
		log.Printf("Event received: %s", evt.Type)
		defer func() {
			log.Printf("Event processed in %v", time.Since(start))
		}()
		return nil
	})

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
