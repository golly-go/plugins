package orm

import (
	"fmt"
	"os"

	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// NewDBConnection new db connection
func NewPostgresConnection(v *viper.Viper, prefixKey string) (*gorm.DB, error) {
	db, err := gorm.Open(postgres.Open(postgressConnectionString(v, prefixKey)), &gorm.Config{Logger: newLogger("postgres")})
	return db, err
}

func postgressConnectionString(v *viper.Viper, prefixKey string) string {
	if url := os.Getenv("DATABASE_URL"); url != "" {
		return url
	}

	return fmt.Sprintf("dbname=%s host=%s port=%d user=%s password=%s sslmode=disable",
		v.GetString(prefixKey+".db.name"),
		v.GetString(prefixKey+".db.host"),
		v.GetInt(prefixKey+".db.port"),
		v.GetString(prefixKey+".db.username"),
		v.GetString(prefixKey+".db.password"),
	)
}
