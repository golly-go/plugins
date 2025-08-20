package eventsource

import (
	"context"
)

// SyncBus is a no-op publisher suitable for tests.
type SyncBus struct{}

func NewSyncBus() *SyncBus { return &SyncBus{} }

func (b *SyncBus) Publish(ctx context.Context, topic string, evt Event) error { return nil }
