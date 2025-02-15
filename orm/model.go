package orm

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Model default model struct (Can add additional functionality here)
type Model struct {
	ID        uint           `json:"id" gorm:"primaryKey" faker:"-"`
	CreatedAt time.Time      `json:"createdAt,omitempty" faker:"-"`
	UpdatedAt time.Time      `json:"updatedAt,omitempty" faker:"-"`
	DeletedAt gorm.DeletedAt `json:"deletedAt,omitempty" faker:"-"`
}

// ModelUUID is a UUID version of model
type ModelUUID struct {
	ID        uuid.UUID      `gorm:"type:uuid;" json:"id" fake:"-"`
	CreatedAt time.Time      `json:"createdAt" faker:"-"`
	UpdatedAt time.Time      `json:"updatedAt" faker:"-"`
	DeletedAt gorm.DeletedAt `json:"deletedAt,omitempty" faker:"-"`
}

func TestModelUUID() ModelUUID {
	return NewModelUUID()
}

func NewModelUUID() ModelUUID {
	uuid1, _ := uuid.NewUUID()
	return ModelUUID{ID: uuid1}
}
