package eventsource

import (
	"reflect"

	"github.com/golly-go/golly"
)

type SubscriptionHandler func(golly.Context, Aggregate, Event)

type Subscription struct {
	All     bool
	Event   reflect.Type
	Handler SubscriptionHandler
}

var (
	subscriptions = make(map[Aggregate][]Subscription)
)

func Subscribe(ag Aggregate, e any, handler SubscriptionHandler) {
	subs := []Subscription{}

	if s, ok := subscriptions[ag]; ok {
		subs = s
	}

	subs = append(subs, Subscription{
		All:     e == nil,
		Handler: handler,
		Event:   reflect.TypeOf(e),
	})

	subscriptions[ag] = subs
}

func SubscribeAll(ag Aggregate, handler SubscriptionHandler) {
	Subscribe(ag, nil, handler)
}

func FireSubscription(ctx golly.Context, ag Aggregate, events ...Event) {
	if subs, ok := subscriptions[ag]; ok {
		for _, event := range events {
			for _, s := range subs {
				if !s.All && reflect.TypeOf(event.Data) != s.Event {
					continue
				}

				s.Handler(ctx, ag, event)
			}
		}
	}
}
