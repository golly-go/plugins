package eventsource

import (
	"context"
	"sync"
)

var (
	identityFunc  func(context.Context) any
	tententIDFunc func(context.Context) string
	userIDFunc    func(context.Context) string

	lock sync.RWMutex
)

func SetUserIDFunc(fnc func(context.Context) string) {
	lock.Lock()
	defer lock.Unlock()

	userIDFunc = fnc
}

func SetTenantIDFunc(fnc func(context.Context) string) {
	lock.Lock()
	defer lock.Unlock()

	tententIDFunc = fnc
}

func SetIdentityFunc(fnc func(context.Context) any) {
	lock.Lock()
	defer lock.Unlock()

	identityFunc = fnc
}
