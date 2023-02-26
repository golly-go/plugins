package eventsource

import (
	"github.com/slimloans/golly"
)

// Repository is a very light wrapper around a datastore
// not all incomposing but read models should be implemented outside of this
type Repository interface {
	Load(ctx golly.Context, object interface{}) error
	Save(ctx golly.Context, object interface{}) error

	Transaction(func(Repository) error) error
	IsNewRecord(obj interface{}) bool
}

// type RepositoryBase struct{}
// func (RepositoryBase) IsNewRecord() bool {
// 	return false
// }
//
// func (Repository) Load(ctx golly.Context, obj interface{}) error {
// 	return orm.DB(ctx).
// 		Model(obj).
// 		Find(&member, "id = ?", member.ID).
// 		Error
// }
//
// func (member *Member) Create(db *gorm.DB) error { return db.Create(&member).Error }
// func (member *Member) Save(db *gorm.DB, original interface{}) error {
// 	return db.Save(&member).Error
// }
