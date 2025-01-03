package eventsource

import "sync"

var (
	lock sync.RWMutex
)

func Initializer() error {
	lock.Lock()
	defer lock.Unlock()

	return nil
}
