package eventsource

import (
	"context"
	"sync/atomic"
)

type UserInfo struct {
	UserID   string
	TenantID string
	Metadata map[string]any
}

// userInfoFunc is a single pointer swapped atomically; reads take no lock.
var userInfoFunc atomic.Pointer[func(context.Context) UserInfo]

func SetUserInfoFunc(fnc func(context.Context) UserInfo) {
	if fnc == nil {
		userInfoFunc.Store(nil)
		return
	}

	userInfoFunc.Store(&fnc)
}
