package orm

import (
	"context"
	"fmt"
	"sync"

	"github.com/golly-go/golly"

	"gorm.io/gorm"
)

const (
	contextKey        golly.ContextKey = "database"
	DefaultConnection                  = "default"

	InMemoryDriver Driver = "in-memory"
	SQLiteDriver   Driver = "sqlite"
	PostgresDriver Driver = "postgres"
)

type Driver string

type Config struct {
	Driver Driver

	Host     string
	User     string
	Database string
	Password string
	Port     int

	SSL bool
}

var (
	db   *gorm.DB
	lock sync.RWMutex
)

func GlobalConnection() *gorm.DB {
	lock.RLock()
	defer lock.RUnlock()

	return db
}

// Initializer sets up the database connection and middleware for an application.
func Initializer(config Config, plugins ...gorm.Plugin) golly.AppFunc {
	return func(app *golly.Application) error {
		var err error

		switch config.Driver {
		case InMemoryDriver:
			db = NewInMemoryConnection()
		case SQLiteDriver:
			db = NewSQLiteConnection(config.Database)
		case PostgresDriver:
			db, err = NewPostgresConnection(app, config)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported database driver: %s", config.Driver)
		}

		// Register GORM plugins
		for pos := range plugins {
			if err := db.Use(plugins[pos]); err != nil {
				return fmt.Errorf("Failed to initialize plugin %s (%w)", plugins[pos].Name(), err)
			}
		}

		// Use middleware to attach connection to the context
		app.Routes().Use(middlewareWithConnection(db))

		return nil
	}
}

// middlewareWithConnection attaches a GORM database connection to the request context.
func middlewareWithConnection(db *gorm.DB) golly.MiddlewareFunc {
	return func(next golly.HandlerFunc) golly.HandlerFunc {
		return func(wctx *golly.WebContext) {
			wctx.Context = ToContext(wctx.Context, db)
			next(wctx)
		}
	}
}

// RetrieveDB retrieves the database connection from the context.
func DB(ctx context.Context) *gorm.DB {
	if ctx != nil {
		if d, ok := ctx.Value(contextKey).(*gorm.DB); ok {
			return d
		}
	}
	return GlobalConnection()
}

func ToContext(ctx context.Context, db *gorm.DB) *golly.Context {
	return golly.WithValue(ctx, contextKey, db)
}
