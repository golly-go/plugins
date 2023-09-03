package eventsource

import (
	"github.com/golly-go/golly"
)

// Repository is a very light wrapper around a datastore
// not all incomposing but read models should be implemented outside of this
type Repository interface {
	Load(ctx golly.Context, object interface{}) error
	Save(ctx golly.Context, object interface{}) error

	Transaction(func(Repository) error) error
	IsNewRecord(obj interface{}) bool
}
