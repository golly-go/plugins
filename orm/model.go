package orm

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

// Model default model struct (Can add additional functionality here)
type Model struct {
	ID        uint           `json:"id" gorm:"primaryKey" faker:"-"`
	CreatedAt time.Time      `json:"created_at,omitempty" faker:"-"`
	UpdatedAt time.Time      `json:"updated_at,omitempty" faker:"-"`
	DeletedAt gorm.DeletedAt `json:"deleted_at,omitempty" faker:"-"`
}

// ModelUUID is a UUID version of model
type ModelUUID struct {
	ID        uuid.UUID      `gorm:"type:uuid;" json:"id" fake:"-"`
	CreatedAt time.Time      `json:"created_at" faker:"-"`
	UpdatedAt time.Time      `json:"updated_at" faker:"-"`
	DeletedAt gorm.DeletedAt `json:"deleted_at,omitempty" faker:"-"`
}

func (base *ModelUUID) BeforeCreate(tx *gorm.DB) error {
	if base.ID == uuid.Nil {
		uuid, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		base.ID = uuid
	}
	return nil
}

func TestModelUUID() ModelUUID {
	return NewModelUUID()
}

func NewModelUUID() ModelUUID {
	uuid1, _ := uuid.NewUUID()
	return ModelUUID{ID: uuid1}
}
