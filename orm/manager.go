package orm

import (
	"sync"

	"gorm.io/gorm"
)

type ConnectionManager struct {
	lock        sync.RWMutex
	connections map[string]*gorm.DB
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		connections: make(map[string]*gorm.DB),
	}
}

func (cm *ConnectionManager) Add(name string, connection *gorm.DB) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	cm.connections[name] = connection
}

func (cm *ConnectionManager) Get(name string) (*gorm.DB, bool) {
	cm.lock.Lock()
	defer cm.lock.Unlock()

	con, found := cm.connections[name]
	return con, found
}

var Manager = NewConnectionManager()
