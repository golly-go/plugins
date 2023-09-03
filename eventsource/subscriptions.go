package eventsource

import (
	"reflect"
	"sync"

	"github.com/golly-go/golly"
)

type SubscriptionHandler func(golly.Context, Aggregate, Event) error

type Subscription struct {
	All     bool
	Event   reflect.Type
	Handler SubscriptionHandler
}

var (
	subscriptions = make(map[Aggregate]map[reflect.Type][]SubscriptionHandler)
	allEventType  = reflect.TypeOf(nil)
	mutex         sync.RWMutex
)

func Subscribe(ag Aggregate, e any, handler SubscriptionHandler) {
	mutex.Lock()
	defer mutex.Unlock()

	eventType := reflect.TypeOf(e)
	if _, exists := subscriptions[ag]; !exists {
		subscriptions[ag] = make(map[reflect.Type][]SubscriptionHandler)
	}

	subscriptions[ag][eventType] = append(subscriptions[ag][eventType], handler)
}

// SubscribeAll now uses the precomputed allEventType.
func SubscribeAll(ag Aggregate, handler SubscriptionHandler) {
	Subscribe(ag, allEventType, handler)
}

func FireSubscription(ctx golly.Context, ag Aggregate, events ...Event) error {
	mutex.RLock()
	defer mutex.RUnlock()

	for _, event := range events {
		eventType := reflect.TypeOf(event.Data)

		// Execute handlers for the specific event type
		if specificSubscribers, exists := subscriptions[ag][eventType]; exists {
			for _, handler := range specificSubscribers {
				if err := handler(ctx, ag, event); err != nil {
					return err
				}
			}
		}

		// Execute handlers that listen to all events using the precomputed allEventType.
		if allEventSubscribers, exists := subscriptions[ag][allEventType]; exists {
			for _, handler := range allEventSubscribers {
				if err := handler(ctx, ag, event); err != nil {
					return err
				}
			}
		}
	}

	return nil
}
