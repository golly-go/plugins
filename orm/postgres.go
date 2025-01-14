package orm

import (
	"fmt"

	"github.com/golly-go/golly"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// NewDBConnection new db connection
func NewPostgresConnection(app *golly.Application, config Config) (*gorm.DB, error) {
	disableLog := false

	if str := app.Config().GetString("database.logger"); str != "" {
		disableLog = true
	}

	db, err := gorm.Open(postgres.Open(postgressConnectionString(app, config)), &gorm.Config{
		Logger: NewLogger("postgres", disableLog),
	})

	return db, err
}

func postgressConnectionString(app *golly.Application, config Config) string {
	if url := app.Config().GetString("database.url"); url != "" {
		return url
	}

	sslMode := "sslmode=disable"
	if config.SSL {
		sslMode = ""
	}

	return fmt.Sprintf("dbname=%s host=%s port=%d user=%s password=%s %s",
		config.Database,
		config.Host,
		config.Port,
		config.User,
		config.Password,
		sslMode,
	)
}
