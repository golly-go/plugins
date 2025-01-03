package eventsource

import (
	"errors"
	"strings"
	"sync"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
)

type SubscriptionHandler func(golly.Context, Aggregate, Event) error

var (
	subscriptions = make(map[string]map[string][]SubscriptionHandler)
	allEventType  = "*"

	mutex sync.RWMutex
)

func Subscribe(aggWithPackage string, e string, handler ...SubscriptionHandler) {
	mutex.Lock()
	defer mutex.Unlock()

	if _, exists := subscriptions[aggWithPackage]; !exists {
		subscriptions[aggWithPackage] = make(map[string][]SubscriptionHandler)
	}

	subscriptions[aggWithPackage][e] = append(subscriptions[aggWithPackage][e], handler...)
}

func SubscribeAll(aggregateWithPackage string, handler ...SubscriptionHandler) {
	Subscribe(aggregateWithPackage, allEventType, handler...)
}

func FireSubscription(ctx golly.Context, ag Aggregate, events ...Event) error {
	aggName := utils.GetTypeWithPackage(ag)
	var errs []error

	handlers := getHandlers(aggName, events)
	for _, event := range events {
		for _, handler := range handlers {
			if err := handler(ctx, ag, event); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return aggregateErrors(errs)
}

func getHandlers(aggName string, events []Event) []SubscriptionHandler {
	mutex.RLock()
	defer mutex.RUnlock()

	handlers := []SubscriptionHandler{}
	for _, event := range events {
		if specificHandlers, exists := subscriptions[aggName][event.Event]; exists {
			handlers = append(handlers, specificHandlers...)
		}
		if allHandlers, exists := subscriptions[aggName][allEventType]; exists {
			handlers = append(handlers, allHandlers...)
		}
	}
	return handlers
}

func aggregateErrors(errs []error) error {
	if len(errs) == 0 {
		return nil
	}

	return errors.New(
		strings.Join(golly.Map(errs, func(e error) string {
			return e.Error()
		}), "; "))
}
