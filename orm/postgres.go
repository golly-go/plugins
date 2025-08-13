package orm

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"gorm.io/gorm"
)

// PostgresConfig defines the configuration required to connect to a PostgreSQL database.
type PostgresConfig struct {
	ConnectionString string
	ApplicationName  string

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

	trace("Connecting to PostgreSQL: %s (Logging = %v)", pgxConfig.ConnString(), config.Logger)

	// Respect caller-provided timeout during connect and ping
	if config.ConnectionTimeout > 0 {
		pgxConfig.ConnectTimeout = config.ConnectionTimeout
	}

	var dbConn *sql.DB
	if config.AuthToken != "" || config.AuthTokenFnc != nil || config.BeforeConnectFnc != nil {
		dbConn = stdlib.OpenDB(*pgxConfig, stdlib.OptionBeforeConnect(beforeConnectWrapper(config)))
	} else {
		dbConn = stdlib.OpenDB(*pgxConfig)
	}

	setConnectionPoolSettings(dbConn, config)

	// Use stock postgres dialector to isolate behavior
	gormDB, err := gorm.Open(&CustomDialector{DB: dbConn}, &gorm.Config{
		Logger: NewLogger("postgres", !config.Logger),
		// DisableAutomaticPing:   true, // we already pinged with context
		SkipDefaultTransaction: true,
	})

	trace("Databsae connection complete (hasError = %v)", err != nil)

	if err != nil {
		return nil, fmt.Errorf("gorm.Open failed: %w", err)
	}

	return gormDB, nil
}

func setConnectionPoolSettings(db *sql.DB, config PostgresConfig) {
	if config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(config.MaxIdleConns)
	}
	if config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(config.MaxOpenConns)
	}
	if config.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(config.ConnMaxLifetime)
	}
	if config.MaxIdleTime > 0 {
		db.SetConnMaxIdleTime(config.MaxIdleTime)
	}

	if config.ConnectionTimeout > 0 {
		db.SetConnMaxLifetime(config.ConnectionTimeout)
	}
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

	name := strings.ReplaceAll(os.Args[0], "/", "-")
	if config.ApplicationName != "" {
		name = config.ApplicationName
	}

	if name[0] == '-' {
		name = name[1:]
	}

	if len(name) > 20 {
		name = name[:20]
	}

	return fmt.Sprintf(
		"dbname=%s host=%s port=%d user=%s%s application_name=%s %s",
		config.Database,
		config.Host,
		config.Port,
		config.User,
		password,
		name,
		sslMode,
	)
}
