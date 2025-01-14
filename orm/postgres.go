package orm

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// PostgresConfig defines the configuration required to connect to a PostgreSQL database.
type PostgresConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSL      bool
	URL      string // Optional: Use a full database URL if provided.
	Logger   bool   // Disable logger if false.
}

// NewPostgresConnection creates a new PostgreSQL connection.
func NewPostgresConnection(config PostgresConfig) (*gorm.DB, error) {
	connectionString := BuildPostgresConnectionString(config)

	db, err := gorm.Open(postgres.Open(connectionString), &gorm.Config{
		Logger: NewLogger("postgres", !config.Logger),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}

	return db, nil
}

// BuildPostgresConnectionString constructs a PostgreSQL connection string.
func BuildPostgresConnectionString(config PostgresConfig) string {
	if config.URL != "" {
		return config.URL
	}

	sslMode := "sslmode=disable"
	if config.SSL {
		sslMode = "sslmode=require"
	}

	return fmt.Sprintf(
		"dbname=%s host=%s port=%d user=%s password=%s %s",
		config.Database,
		config.Host,
		config.Port,
		config.User,
		config.Password,
		sslMode,
	)
}
