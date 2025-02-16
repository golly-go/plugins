package main

import (
	"context"
	"log"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
)

// UserCreated event
type UserCreated struct {
	ID   string
	Name string
}

// User aggregate
type User struct {
	eventsource.AggregateBase
	ID   string
	Name string
}

func (u *User) GetID() string {
	return u.ID
}

func (u *User) ApplyUserCreated(event UserCreated) {
	u.ID = event.ID
	u.Name = event.Name
}

// CreateUser command
type CreateUser struct {
	ID   string
	Name string
}

func (c *CreateUser) Perform(ctx *golly.Context, agg eventsource.Aggregate) error {
	user := agg.(*User)
	user.Record(eventsource.Event{
		Type: "UserCreated",
		Data: UserCreated{
			ID:   c.ID,
			Name: c.Name,
		},
	})
	return nil
}

func main() {
	// Create engine with in-memory store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	// Register the User aggregate
	engine.RegisterAggregate(&User{}, []any{UserCreated{}})

	// Create a new user
	user := &User{ID: "user_1"}

	// Execute the CreateUser command
	ctx := golly.NewContext(context.Background())
	cmd := &CreateUser{ID: "user_1", Name: "Alice"}
	if err := engine.Execute(ctx, user, cmd); err != nil {
		log.Fatalf("Failed to execute command: %v", err)
	}

	// Print the user state
	log.Printf("User ID: %s", user.ID)
	log.Printf("User Name: %s", user.Name)
}
