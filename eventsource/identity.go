package eventsource

import (
	"context"
	"sync"
)

var (
	identiyFunc func(context.Context) any

	lock sync.RWMutex
)

func IdentityFunc(fnc func(context.Context) any) {
	lock.Lock()
	defer lock.Unlock()

	identiyFunc = fnc
}
