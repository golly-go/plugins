package eventsource

import (
	"context"
	"reflect"
	"sync"
)

// InMemoryBus is a simple Bus implementation.
// Currently synchronous like SyncBus; kept for future async expansion.
type InMemoryBus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler // topic -> handlers
}

func NewInMemoryBus() *InMemoryBus {
	return &InMemoryBus{handlers: make(map[string][]Handler)}
}

func (b *InMemoryBus) Start() {}
func (b *InMemoryBus) Stop()  {}

func (b *InMemoryBus) Subscribe(topic string, handler Handler) {
	b.mu.Lock()
	b.handlers[topic] = append(b.handlers[topic], handler)
	b.mu.Unlock()
}

func (b *InMemoryBus) Unsubscribe(topic string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()

	hs := b.handlers[topic]
	for i, h := range hs {
		if reflect.ValueOf(h).Pointer() == reflect.ValueOf(handler).Pointer() {
			b.handlers[topic] = append(hs[:i], hs[i+1:]...)
			break
		}
	}
}

func (b *InMemoryBus) Publish(ctx context.Context, topic string, evt Event) error {
	b.mu.RLock()
	list := append([]Handler(nil), b.handlers[topic]...)
	b.mu.RUnlock()
	for i := range list {
		_ = list[i](ctx, evt)
	}
	return nil
}
