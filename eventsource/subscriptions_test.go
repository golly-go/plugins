package eventsource

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
)

func TestFireSubscription(t *testing.T) {
	var testAggregate = testAggregate{
		repo: &testRepostoryBase{},
	}

	ctx := golly.NewContext(context.Background())

	tests := []struct {
		name          string
		aggregate     Aggregate
		events        []Event
		handlers      []SubscriptionHandler
		expectedError bool
	}{
		{
			name:          "No Handlers",
			aggregate:     &testAggregate,
			events:        []Event{},
			handlers:      []SubscriptionHandler{},
			expectedError: false,
		},
		{
			name:      "Handler Error",
			aggregate: &testAggregate,

			handlers: []SubscriptionHandler{
				func(ctx golly.Context, ag Aggregate, e Event) error {
					return errors.New("handler error")
				},
			},
			expectedError: true,
		},
		{
			name:      "Multiple Handlers",
			aggregate: &testAggregate,

			handlers: []SubscriptionHandler{
				func(ctx golly.Context, ag Aggregate, e Event) error {
					return nil
				},
				func(ctx golly.Context, ag Aggregate, e Event) error {
					return errors.New("another error")
				},
			},

			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			subscriptions = make(map[string]map[string][]SubscriptionHandler)

			Subscribe("eventsource.testAggregate", "UserCreated", tt.handlers...)

			err := FireSubscription(ctx, tt.aggregate, Event{Event: "UserCreated"})

			fmt.Printf("%#v\n", err)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
