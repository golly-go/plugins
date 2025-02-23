package main

import (
	"context"
	"log"
	"time"

	"github.com/golly-go/plugins/eventsource"
)

func main() {
	// Create engine with store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	// Add error handling middleware using the default stream
	engine.Subscribe("UserAction", func(ctx context.Context, evt eventsource.Event) {
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
