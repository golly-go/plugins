package main

import (
	"context"
	"fmt"
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

func (u *User) GetID() string { return u.ID }

func (u *User) UserCreatedHandler(event UserCreated) {
	u.ID = event.ID
	u.Name = event.Name
}

// CreateUser command
type CreateUser struct {
	ID   string
	Name string
}

func (c CreateUser) Perform(ctx context.Context, agg eventsource.Aggregate) error {
	agg.Record(UserCreated(c))
	return nil
}

func main() {
	// Create engine with in-memory store
	engine := eventsource.NewEngine(&eventsource.InMemoryStore{})

	engine.Start()
	defer engine.Stop()

	// Register the User aggregate
	engine.RegisterAggregate(&User{}, []any{UserCreated{}})

	user := &User{}

	// Execute the CreateUser command
	ctx := golly.NewContext(context.Background())

	cmd := CreateUser{ID: "user_1", Name: "Alice"}

	if err := engine.Execute(ctx, user, cmd); err != nil {
		log.Fatalf("Failed to execute command: %v", err)
	}

	// Print the user state
	golly.Say("green", "Created User")

	fmt.Printf("User ID: %s\n", user.ID)
	fmt.Printf("User Name: %s\n", user.Name)

	u2 := &User{ID: "user_1"}

	engine.Load(ctx, u2)

	golly.Say("green", "Loaded User")
	fmt.Printf("User ID: %s\n", u2.ID)
	fmt.Printf("User Name: %s\n", u2.Name)

}
