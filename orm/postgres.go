package orm

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
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

	AuthToken     func() (string, error)
	BeforeConnect func(ctx context.Context, config *pgx.ConnConfig) error
}

func beforeConnectWrapper(pconf PostgresConfig) func(ctx context.Context, config *pgx.ConnConfig) error {
	return func(ctx context.Context, config *pgx.ConnConfig) error {
		if pconf.AuthToken != nil {
			token, err := pconf.AuthToken()
			if err != nil {
				return err
			}
			config.Password = token
		}

		if pconf.BeforeConnect != nil {
			if err := pconf.BeforeConnect(ctx, config); err != nil {
				return err
			}
		}

		return nil
	}
}

// NewPostgresConnection creates a new PostgreSQL connection.
func NewPostgresConnection(config PostgresConfig) (*gorm.DB, error) {
	connectionString := BuildPostgresConnectionString(config)

	psConfig, err := pgx.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL connection string: %w", err)
	}

	conn := stdlib.OpenDB(*psConfig, stdlib.OptionBeforeConnect(beforeConnectWrapper(config)))
	db, err := gorm.Open(postgres.New(postgres.Config{Conn: conn}), &gorm.Config{
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

	password := ""
	if config.Password != "" {
		password = fmt.Sprintf(" password=%s", config.Password)
	}

	return fmt.Sprintf(
		"dbname=%s host=%s port=%d user=%s%s %s",
		config.Database,
		config.Host,
		config.Port,
		config.User,
		password,
		sslMode,
	)
}
