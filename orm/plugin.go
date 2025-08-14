package orm

import (
	"fmt"

	"github.com/golly-go/golly"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

type ConfigFunc[T any] func(a *golly.Application) (T, error)
type AfterFunc func(db *gorm.DB) error

// OrmConfig provides a generic configuration wrapper for the ORM plugin.
type OrmPlugin[T any] struct {
	UseGormMigrations bool

	config ConfigFunc[T]

	after []AfterFunc

	Database T // Generic configuration, specific to the database driver
}

// NewOrmPlugin creates a new instance of OrmPlugin with default values.
func NewOrmPlugin[T any](config ConfigFunc[T]) *OrmPlugin[T] {
	return &OrmPlugin[T]{config: config}
}

func (*OrmPlugin[T]) Name() string { return "orm" }

// After allows you to hook on to after the connection is estabilished
// to enable any gorm specific stuff you want like CTEs etc
func (p *OrmPlugin[T]) After(handlers ...AfterFunc) *OrmPlugin[T] {
	p.after = append(p.after, handlers...)
	return p
}

// Initialize sets up the database connection and middleware.
func (p *OrmPlugin[T]) Initialize(app *golly.Application) error {
	var err error
	lock.Lock()
	defer lock.Unlock()

	if p.config != nil {
		if d, err := p.config(app); err != nil {
			return err
		} else {
			p.Database = d
		}
	}

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

	for pos := range p.after {
		if err := p.after[pos](db); err != nil {
			return err
		}
	}

	trace("Connected to database with config %T", p.Database)

	// Add middleware to attach the database connection to the context.
	app.Routes().Use(middlewareWithConnection(db))

	return nil
}

func (p *OrmPlugin[T]) Deinitialize() error { return nil }

// AfterDeinitialize closes the global database connection.
func (p *OrmPlugin[T]) AfterDeinitialize(app *golly.Application) error {
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
