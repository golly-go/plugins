package orm

import (
	"fmt"

	"github.com/golly-go/golly"
	"github.com/spf13/cobra"
)

// OrmConfig provides a generic configuration wrapper for the ORM plugin.
type OrmPlugin[T any] struct {
	UseGormMigrations bool

	Database T // Generic configuration, specific to the database driver
}

// NewOrmPlugin creates a new instance of OrmPlugin with default values.
func NewOrmPlugin[T any](config T) *OrmPlugin[T] {
	return &OrmPlugin[T]{Database: config}
}

// Initialize sets up the database connection and middleware.
func (p *OrmPlugin[T]) Initialize(app *golly.Application) error {
	var err error
	lock.Lock()
	defer lock.Unlock()

	switch cfg := any(p.Database).(type) {
	case SQLiteConfig:
		db, err = sqliteConnection(cfg)
		if err != nil {
			return err
		}
	case PostgresConfig:
		// PostgreSQL requires a more complex configuration
		db, err = NewPostgresConnection(cfg)
		if err != nil {
			return fmt.Errorf("failed to connect to Postgres: %w", err)
		}
	default:
		return fmt.Errorf("unsupported configuration type: %T", p.Database)
	}

	// Add middleware to attach the database connection to the context.
	app.Routes().Use(middlewareWithConnection(db))

	return nil
}

// Deinitialize closes the global database connection.
func (p *OrmPlugin[T]) Deinitialize(app *golly.Application) error {
	connection := GlobalConnection()

	lock.Lock()
	defer lock.Unlock()

	dbConn, err := connection.DB()
	if err != nil {
		return fmt.Errorf("failed to retrieve database connection: %w", err)
	}

	return dbConn.Close()
}

// Commands returns CLI commands for migration and database tasks.
func (p *OrmPlugin[T]) Commands() []*cobra.Command {
	if p.UseGormMigrations {
		return []*cobra.Command{}
	}

	rootCmd.AddCommand(commands...)
	return []*cobra.Command{rootCmd}
}