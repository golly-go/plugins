package orm

import (
	"context"
	"fmt"
	"sync"

	"github.com/golly-go/golly"
	"github.com/google/uuid"

	"gorm.io/gorm"
)

const (
	DefaultConnection = "default"
)

const (
	InMemoryDriver Driver = "in-memory"
	SQLiteDriver   Driver = "sqlite"
	PostgresDriver Driver = "postgres"
)

type contextKeyT struct{}

var contextKey = &contextKeyT{}

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

// middlewareWithConnection attaches a GORM database connection to the request context.
func middlewareWithConnection(db *gorm.DB) golly.MiddlewareFunc {
	return func(next golly.HandlerFunc) golly.HandlerFunc {
		return func(wctx *golly.WebContext) {
			wctx.WithContext(golly.WithValue(wctx.Context(), contextKey, db))
			next(wctx)
		}
	}
}

// RetrieveDB retrieves the database connection from the context.
func DB(ctx context.Context) *gorm.DB {
	if db := FromContext(ctx); db != nil {
		return db
	}
	return GlobalConnection()
}

func NewDB(c context.Context) *gorm.DB {
	return DB(c).Session(&gorm.Session{NewDB: true})
}

func ToContext(parent context.Context, db *gorm.DB) context.Context {
	return golly.WithValue(parent, contextKey, db)
}

func FromContext(ctx context.Context) *gorm.DB {
	if ctx != nil {
		if d, ok := ctx.Value(contextKey).(*gorm.DB); ok {
			return d
		}
	}
	return nil
}

func CreateTestContext(c context.Context, modelsToMigration ...interface{}) context.Context {
	fileName := fmt.Sprintf("memdb_%s", uuid.New().String())

	return golly.WithValue(c, contextKey, NewInMemoryConnection(fileName, modelsToMigration...))
}

func Close(c context.Context) error {
	db, err := DB(c).DB()
	if err != nil {
		return err
	}

	return db.Close()
}

func trace(format string, args ...interface{}) {
	// Ensure Trace always prints when LOG_LEVEL=trace
	lg := golly.DefaultLogger()
	if lg != nil {
		lg.Tracef("[ORM] "+format, args...)
	}
}
