package orm

import (
	"context"
	"fmt"
	"sync"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"gorm.io/gorm"
)

var db *gorm.DB
var lock sync.RWMutex

const contextKey = "database"

const DefaultConnection = "default"

type DriverT string

const (
	InMemoryDriver DriverT = "in-memory"
	SQLiteDriver   DriverT = "sqlite"
	PostgresDriver DriverT = "postgres"
)

type Config struct {
	ConnectionName string
	Driver         DriverT

	Host     string
	User     string
	Database string
	Password string
	Port     int

	SSL bool
}

func InitializerWithMigration(app golly.Application, config Config, modelsToMigrate ...interface{}) golly.GollyAppFunc {
	return func(app golly.Application) error {
		fnc := Initializer(config)

		if err := fnc(app); err != nil {
			return err
		}
		return db.AutoMigrate(modelsToMigrate...)
	}
}

func SetConnection(newDB *gorm.DB) {
	lock.Lock()
	defer lock.Unlock()

	db = newDB
}

// Initializer golly initializer setting up the databse
// todo: mkae this more dynamic going forward with interfaces etc
// since right now we only support gorm
func Initializer(config Config) func(app golly.Application) error {

	return func(app golly.Application) error {
		switch config.Driver {
		case InMemoryDriver:
			SetConnection(NewInMemoryConnection())
		case SQLiteDriver:
			SetConnection(NewSQLiteConnection(config.ConnectionName))
		case PostgresDriver:
			d, err := NewPostgresConnection(app.Config, config)
			if err != nil {
				return errors.WrapGeneric(err)
			}
			SetConnection(d)
		default:
			return errors.WrapGeneric(fmt.Errorf("database drive %s not supported", config.Driver))
		}

		app.Routes().Use(middleware)
		return nil
	}
}

func Connection() *gorm.DB {
	return db
}

// Not sure i want to go back to having a global database
// but for now lets do this
func DB(c golly.Context) *gorm.DB {
	if db := GetDBFromContext(c); db != nil {
		return db
	}
	return Connection()
}

func NewDB(c golly.Context) *gorm.DB {
	return DB(c).Session(&gorm.Session{NewDB: true})
}

func ToContext(parent context.Context, db *gorm.DB) context.Context {
	return context.WithValue(parent, gorm.DB{}, db)
}

func FromContext(ctx context.Context) *gorm.DB {
	if db, ok := ctx.Value(gorm.DB{}).(*gorm.DB); ok {
		return db
	}
	return nil
}

func middleware(next golly.HandlerFunc) golly.HandlerFunc {
	return func(c golly.WebContext) {
		c.Context = SetNewSessionOnContext(c.Context, Connection())

		next(c)
	}
}

func SetDBOnContext(c golly.Context, db *gorm.DB) golly.Context {
	return c.Set(contextKey, db)
}

func SetNewSessionOnContext(c golly.Context, db *gorm.DB) golly.Context {
	return SetDBOnContext(c, db.Session(&gorm.Session{NewDB: true}))
}

func CreateTestContext(c golly.Context, modelsToMigration ...interface{}) golly.Context {
	return SetNewSessionOnContext(c, NewInMemoryConnection(modelsToMigration...))
}

func GetDBFromContext(c golly.Context) *gorm.DB {
	if db, ok := c.Get(contextKey); ok {
		return db.(*gorm.DB)
	}
	return nil
}
