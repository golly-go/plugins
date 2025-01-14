package orm

import (
	"context"
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

// middlewareWithConnection attaches a GORM database connection to the request context.
func middlewareWithConnection(db *gorm.DB) golly.MiddlewareFunc {
	return func(next golly.HandlerFunc) golly.HandlerFunc {
		return func(wctx *golly.WebContext) {
			wctx.Context = ToGollyContext(wctx.Context, db)
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
	return context.WithValue(parent, contextKey, db)
}

func ToGollyContext(parent *golly.Context, db *gorm.DB) *golly.Context {
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
