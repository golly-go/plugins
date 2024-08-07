package orm

import (
	"fmt"

	"github.com/golly-go/golly"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// this is used for testing makes things easier.
// NewInMemoryConnection creates a new database connection and migrates any passed in model
func NewSQLiteConnection(app string, modelToMigrate ...interface{}) *gorm.DB {
	dbName := fmt.Sprintf("db/%s.sqlite", golly.Env())

	db, _ := gorm.Open(sqlite.Open(dbName), &gorm.Config{Logger: NewLogger(dbName, false)})

	if len(modelToMigrate) > 0 {
		db.AutoMigrate(modelToMigrate...)
	}

	return db
}

// this is used for testing makes things easier.
// NewInMemoryConnection creates a new database connection and migrates any passed in model
func NewInMemoryConnection(modelToMigrate ...interface{}) *gorm.DB {
	db, _ := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
		Logger: NewLogger("in-memory", false),
	})

	if len(modelToMigrate) > 0 {
		db.AutoMigrate(modelToMigrate...)
	}

	return db
}
