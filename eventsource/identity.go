package eventsource

import (
	"sync"

	"github.com/golly-go/golly"
)

var (
	identiyFunc func(golly.Context) any

	lock sync.RWMutex
)

func IdentityFunc(fnc func(golly.Context) any) {
	lock.Lock()
	defer lock.Unlock()

	identiyFunc = fnc
}
