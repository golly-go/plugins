package eventsource

import (
	"context"
	"reflect"
	"sync"
)

// SyncBus is a simple synchronous Bus implementation for tests.
// Handlers are invoked inline in the publisher's goroutine.
type SyncBus struct {
	mu       sync.RWMutex
	handlers map[string][]Handler
}

func NewSyncBus() *SyncBus {
	return &SyncBus{handlers: make(map[string][]Handler)}
}

func (b *SyncBus) Start() {}
func (b *SyncBus) Stop()  {}

func (b *SyncBus) Subscribe(topic string, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[topic] = append(b.handlers[topic], handler)
}

func (b *SyncBus) Unsubscribe(topic string, handler Handler) {
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

func (b *SyncBus) Publish(ctx context.Context, topic string, evt Event) error {
	b.mu.RLock()
	list := append([]Handler(nil), b.handlers[topic]...)
	b.mu.RUnlock()
	for i := range list {
		_ = list[i](ctx, evt)
	}
	return nil
}
