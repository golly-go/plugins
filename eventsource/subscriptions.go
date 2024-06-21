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
	subscriptions = make(map[string]map[string][]SubscriptionHandler)
	allEventType  = "*"

	mutex sync.RWMutex
)

func Subscribe(aggregateWithPackage string, e string, handler SubscriptionHandler) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, exists := subscriptions[aggregateWithPackage]; !exists {
		subscriptions[aggregateWithPackage] = make(map[string][]SubscriptionHandler)
	}

	subscriptions[aggregateWithPackage][e] = append(subscriptions[aggregateWithPackage][e], handler)
}

// SubscribeAll now uses the precomputed allEventType.
func SubscribeAll(aggregateWithPackage string, handler SubscriptionHandler) {
	Subscribe(aggregateWithPackage, "*", handler)
}

func FireSubscription(ctx golly.Context, ag Aggregate, events ...Event) error {
	mutex.RLock()
	defer mutex.RUnlock()

	aggName := utils.GetTypeWithPackage(ag)

	for _, event := range events {
		eventType := utils.GetTypeWithPackage(event.Data)

		// Execute handlers for the specific event type
		if specificSubscribers, exists := subscriptions[aggName][eventType]; exists {
			for _, handler := range specificSubscribers {
				if err := handler(ctx, ag, event); err != nil {
					return err
				}
			}
		}

		// Execute handlers that listen to all events using the precomputed allEventType.
		if allEventSubscribers, exists := subscriptions[aggName][utils.GetTypeWithPackage(allEventType)]; exists {
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
