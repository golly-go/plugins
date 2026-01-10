package orm

import (
	"errors"
	"fmt"
	"os"

	"github.com/glebarez/sqlite"
	"github.com/golly-go/golly"
	"gorm.io/gorm"
)

var (
	ErrorDatabaseNotDefined = errors.New("database is required but not defined")
)

type SQLiteConfig struct {
	Logger           bool
	InMemory         bool
	Database         string
	Path             string
	ConnectionString string
}

func sqliteConnection(config SQLiteConfig, modelsToMigrate ...any) (*gorm.DB, error) {
	if config.InMemory {
		return NewInMemoryConnection("", modelsToMigrate...), nil
	}

	return NewSQLiteConnection(config, modelsToMigrate...)
}

// this is used for testing makes things easier.
// NewInMemoryConnection creates a new database connection and migrates any passed in model
func NewSQLiteConnection(config SQLiteConfig, modelToMigrate ...interface{}) (*gorm.DB, error) {
	connectionString := makeConnectionString(config)

	golly.DefaultLogger().Tracef("Connecting to sqlite database: %s", connectionString)

	db, _ := gorm.Open(sqlite.Open(connectionString), &gorm.Config{Logger: NewLogger(connectionString, !config.Logger)})

	if len(modelToMigrate) > 0 {
		if err := db.AutoMigrate(modelToMigrate...); err != nil {
			return db, err
		}
	}

	return db, nil
}

func getDisableLogger() bool {
	if os.Getenv("ENABLE_DB_LOGGER_IN_TEST") == "true" {
		return false
	}

	if os.Getenv("DATABASE_LOGGER") == "true" {
		return true
	}

	return true
}

// this is used for testing makes things easier.
// NewInMemoryConnection creates a new database connection and migrates any passed in model
func NewInMemoryConnection(fileName string, modelToMigrate ...interface{}) *gorm.DB {
	if fileName == "" {
		fileName = ":memory:"
	}
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", fileName)

	db, _ := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: NewLogger("in-memory", getDisableLogger()),
	})

	if len(modelToMigrate) > 0 {
		db.AutoMigrate(modelToMigrate...)
	}

	return db
}

func makeConnectionString(config SQLiteConfig) string {
	if config.ConnectionString != "" {
		return config.ConnectionString
	}

	if config.Database == "" {
		return ""
	}

	path := config.Path
	if path == "" {
		path = "db/"
	}

	if path[len(path)-1] != '/' {
		path += "/"
	}

	return fmt.Sprintf("%s%s.sqlite", path, config.Database)
}
