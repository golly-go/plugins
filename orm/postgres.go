package orm

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"
)

// PostgresConfig defines the configuration required to connect to a PostgreSQL database.
type PostgresConfig struct {
	ConnectionString string

	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSL      bool
	URL      string // Optional: Use a full database URL if provided.
	Logger   bool   // Disable logger if false.

	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	MaxIdleTime     time.Duration

	ConnectionTimeout time.Duration

	AuthToken        string
	AuthTokenFnc     func() (string, error)
	BeforeConnectFnc func(ctx context.Context, config *pgx.ConnConfig) error
}

func beforeConnectWrapper(pconf PostgresConfig) func(ctx context.Context, config *pgx.ConnConfig) error {
	return func(ctx context.Context, config *pgx.ConnConfig) error {
		// Allow authtoken to be passed for local development
		if pconf.AuthToken != "" {
			config.Password = pconf.AuthToken
			return nil
		}

		if pconf.AuthTokenFnc != nil {
			token, err := pconf.AuthTokenFnc()
			if err != nil {
				return err
			}
			config.Password = token
		}

		if pconf.BeforeConnectFnc != nil {
			if err := pconf.BeforeConnectFnc(ctx, config); err != nil {
				return err
			}
		}
		return nil
	}
}

func NewPostgresConnection(config PostgresConfig) (*gorm.DB, error) {

	connString := BuildPostgresConnectionString(config)

	pgxConfig, err := pgx.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PostgreSQL connection string: %w", err)
	}

	trace("Connecting to PostgreSQL: %s", pgxConfig.ConnString())

	var dbConn *sql.DB
	if config.AuthTokenFnc != nil || config.BeforeConnectFnc != nil {
		dbConn = stdlib.OpenDB(*pgxConfig, stdlib.OptionBeforeConnect(beforeConnectWrapper(config)))
	} else {
		dbConn = stdlib.OpenDB(*pgxConfig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := dbConn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("Ping failed: %w", err)
	}

	setConnectionPoolSettings(dbConn, config)

	// critical fix: use the custom dialector explicitly here
	gormDB, err := gorm.Open(&CustomDialector{DB: dbConn}, &gorm.Config{
		Logger: NewLogger("postgres", config.Logger),
	})

	if err != nil {
		return nil, fmt.Errorf("gorm.Open failed: %w", err)
	}

	return gormDB, nil
}

func setConnectionPoolSettings(db *sql.DB, config PostgresConfig) {
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.MaxIdleTime)
}

func BuildPostgresConnectionString(config PostgresConfig) string {
	if config.ConnectionString != "" {
		return config.ConnectionString
	}

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

	if config.ConnectionTimeout == 0 {
		config.ConnectionTimeout = 30 * time.Second
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
