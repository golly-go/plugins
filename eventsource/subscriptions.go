package eventsource

import (
	"reflect"
	"sync"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
)

type SubscriptionHandler func(golly.Context, Aggregate, Event) error

type Subscription struct {
	All     bool
	Event   reflect.Type
	Handler SubscriptionHandler
}

var (
	subscriptions = make(map[reflect.Type]map[string][]SubscriptionHandler)
	allEventType  = Event{}

	mutex sync.RWMutex
)

func Subscribe(ag Aggregate, e any, handler SubscriptionHandler) {
	mutex.Lock()
	defer mutex.Unlock()

	agg := reflect.TypeOf(ag)

	eventType := utils.GetTypeWithPackage(e)

	if _, exists := subscriptions[agg]; !exists {
		subscriptions[agg] = make(map[string][]SubscriptionHandler)
	}

	subscriptions[agg][eventType] = append(subscriptions[agg][eventType], handler)
}

// SubscribeAll now uses the precomputed allEventType.
func SubscribeAll(ag Aggregate, handler SubscriptionHandler) {
	Subscribe(ag, allEventType, handler)
}

func FireSubscription(ctx golly.Context, ag Aggregate, events ...Event) error {
	mutex.RLock()
	defer mutex.RUnlock()

	agg := reflect.TypeOf(ag)

	for _, event := range events {
		eventType := utils.GetTypeWithPackage(event.Data)

		// Execute handlers for the specific event type
		if specificSubscribers, exists := subscriptions[agg][eventType]; exists {
			for _, handler := range specificSubscribers {
				if err := handler(ctx, ag, event); err != nil {
					return err
				}
			}
		}

		// Execute handlers that listen to all events using the precomputed allEventType.
		if allEventSubscribers, exists := subscriptions[agg][utils.GetTypeWithPackage(allEventType)]; exists {
			for pos, handler := range allEventSubscribers {
				ctx.Logger().Debugf("Executing handler: %d %#v\n", pos, handler)

				if err := handler(ctx, ag, event); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
