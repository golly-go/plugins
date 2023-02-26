package orm

import (
	"context"
	"fmt"
	"sync"

	"github.com/slimloans/golly"
	"github.com/slimloans/golly/errors"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

var db *gorm.DB
var lock sync.RWMutex

const contextKey = "database"

func InitializerWithMigration(app golly.Application, modelsToMigrate ...interface{}) golly.GollyAppFunc {
	return func(app golly.Application) error {
		if err := Initializer(app); err != nil {
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
func Initializer(app golly.Application) error {

	v := setConfigDefaults(app.Name, app.Config)

	driver := v.GetString(fmt.Sprintf("%s.db.driver", app.Name))

	switch driver {
	case "in-memory":
		SetConnection(NewInMemoryConnection())
	case "sqlite":
		SetConnection(NewSQLiteConnection(app.Name))
	case "postgres":
		d, err := NewPostgresConnection(v, app.Name)
		if err != nil {
			return errors.WrapGeneric(err)
		}
		SetConnection(d)
	default:
		return errors.WrapGeneric(fmt.Errorf("database drive %s not supported", driver))
	}

	app.Routes().Use(middleware)
	return nil
}

func Connection() *gorm.DB {
	return db
}

// Not sure i want to go back to having a global database
// but for now lets do this
func DB(c golly.Context) *gorm.DB {
	if db, found := c.Get(contextKey); found {
		return db.(*gorm.DB)
	}
	return Connection()
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
		SetDBOnContext(c.Context, Connection())
		next(c)
	}
}

func SetDBOnContext(c golly.Context, db *gorm.DB) {
	c.Set(contextKey, db.Session(&gorm.Session{NewDB: true}))
}

func CreateTestContext(c golly.Context, modelsToMigration ...interface{}) golly.Context {
	SetDBOnContext(c, NewInMemoryConnection(modelsToMigration...))
	return c
}

// Sane defaults TODO: Clean this up
func setConfigDefaults(appName string, v *viper.Viper) *viper.Viper {
	v.SetDefault(appName, map[string]interface{}{
		"db": map[string]interface{}{
			"host":     "127.0.0.1",
			"port":     "5432",
			"username": "app",
			"password": "password",
			"name":     appName,
			"driver":   "postgres",
		},
	})
	return v
}
