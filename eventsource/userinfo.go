package eventsource

import (
	"context"
	"sync"
)

type UserInfo struct {
	UserID   string
	TenantID string
	Metadata map[string]any
}

var (
	userInfoFunc func(context.Context) UserInfo

	lock sync.RWMutex
)

func SetUserInfoFunc(fnc func(context.Context) UserInfo) {
	lock.Lock()
	defer lock.Unlock()

	userInfoFunc = fnc
}
