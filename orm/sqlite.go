package orm

import (
	"errors"
	"fmt"

	"github.com/golly-go/golly"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

var (
	ErrorDatabaseNotDefined = errors.New("database is required but not defined")
)

type SQLiteConfig struct {
	InMemory bool
	Database string
}

func sqliteConnection(config SQLiteConfig, modelsToMigrate ...any) (*gorm.DB, error) {
	if config.InMemory {
		return NewInMemoryConnection(modelsToMigrate...), nil
	}

	return NewSQLiteConnection(config, modelsToMigrate...)
}

// this is used for testing makes things easier.
// NewInMemoryConnection creates a new database connection and migrates any passed in model
func NewSQLiteConnection(config SQLiteConfig, modelToMigrate ...interface{}) (*gorm.DB, error) {
	if config.Database == "" {
		return nil, ErrorDatabaseNotDefined
	}

	dbName := fmt.Sprintf("db/%s.sqlite", golly.Env())

	db, _ := gorm.Open(sqlite.Open(dbName), &gorm.Config{Logger: NewLogger(dbName, false)})

	if len(modelToMigrate) > 0 {
		if err := db.AutoMigrate(modelToMigrate...); err != nil {
			return db, err
		}
	}

	return db, nil
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
