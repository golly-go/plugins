package orm

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// NewDBConnection new db connection
func NewPostgresConnection(v *viper.Viper, config Config) (*gorm.DB, error) {
	disableLog := false

	if str := viper.GetString("DISABLE_DB_LOG"); str != "" {
		disableLog = true
	}

	db, err := gorm.Open(postgres.Open(postgressConnectionString(v, config)), &gorm.Config{
		Logger: NewLogger("postgres", disableLog),
	})

	return db, err
}

func postgressConnectionString(v *viper.Viper, config Config) string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
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
