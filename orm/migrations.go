package orm

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/golly-go/golly"
	"github.com/spf13/cobra"
	"gorm.io/gorm"
)

type SchemaMigration struct {
	Version string `gorm:"primaryKey;autoIncrement:false"`
	File    string
}

const (
	dbFolder      = "db"
	migrationPath = dbFolder + "/migrations/"
)

var (
	rootCmd = &cobra.Command{
		Use:              "migration",
		Aliases:          []string{"migrate", "migrations"},
		TraverseChildren: true,
	}

	commands = []*cobra.Command{
		{
			Use:     "run",
			Aliases: []string{"migrate"},
			Short:   "Run all pending migrations to bring the database up to date",
			Run:     wrapCommand(MigrationPerform),
		},
		{
			Use:     "create",
			Aliases: []string{"generate", "make"},
			Short:   "Generate a new migration file with the specified name",
			Run:     wrapCommand(MigrationGenerate),
			Args:    cobra.MinimumNArgs(1),
		},
		{
			Use:   "down",
			Short: "Revert the specified migration version",
			Run:   wrapCommand(MigrationDown),
			Args:  cobra.MinimumNArgs(1),
		},
		{
			Use:     "init",
			Short:   "Initialize the application for migrations, setting up the migration table",
			Aliases: []string{"setup"},
			Run:     wrapCommand(MigrationInit),
		},
		{
			Use:   "version",
			Short: "Display the current database version and any pending migrations",
			Run:   wrapCommand(MigrationVersion),
		},
	}
)

type dbCliCommand func(db *gorm.DB, args []string) error

func wrapCommand(dbCmd dbCliCommand) func(cmd *cobra.Command, args []string) {
	return golly.Command(func(app *golly.Application, cmd *cobra.Command, args []string) error {
		return dbCmd(GlobalConnection(), args)
	})
}

// MigrationInit initializes the migration folder and creates the schema migrations table.
func MigrationInit(db *gorm.DB, args []string) error {
	if err := createDBFolderIfNotExist(); err != nil {
		return err
	}

	if err := db.AutoMigrate(&SchemaMigration{}); err != nil {
		return err
	}

	golly.Say("green", "Initialized schema migrations table.")
	return nil
}

// MigrationGenerate creates new migration files with up and down SQL templates.
func MigrationGenerate(db *gorm.DB, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("Migration name is required")
	}

	version := newVersionString()
	fileSlug := fmt.Sprintf("%s_%s", version, formatSlug(args[0]))

	upFile := filepath.Join(migrationPath, fileSlug+".up.sql")
	downFile := filepath.Join(migrationPath, fileSlug+".down.sql")

	if err := os.WriteFile(upFile, []byte("-- Up Migration "+version+" "+args[0]), 0644); err != nil {
		return err
	}

	if err := os.WriteFile(downFile, []byte("-- Down Migration "+version+" "+args[0]), 0644); err != nil {
		os.Remove(upFile)
		return err
	}

	golly.Say("green", "Generated migration files:\n  Up: %s\n  Down: %s", upFile, downFile)
	return nil
}

// MigrationVersion prints the current database version and lists any missing migrations.
func MigrationVersion(db *gorm.DB, args []string) error {
	sm := SchemaMigration{}

	if err := db.Model(&sm).Last(&sm).Error; err != nil {
		golly.Say("yellow", "Database Version: No migrations")
	} else {
		golly.Say("green", "Database Version: %s", sm.Version)
	}

	golly.Say("yellow", "\nMissing Migrations:")
	for _, migration := range missingMigrations(db) {
		golly.Say("red", "  - %s", migration[0])
	}

	return nil
}

// MigrationDown reverts a specific migration by version number.
func MigrationDown(db *gorm.DB, args []string) error {
	version := args[0]
	sm := SchemaMigration{}

	if err := db.Model(&sm).Find(&sm, "version = ?", version).Error; err != nil {
		return fmt.Errorf("Migration version does not exist: %w", err)
	}

	files, err := filepath.Glob(fmt.Sprintf("%s%s*.down.sql", migrationPath, version))
	if err != nil || len(files) == 0 {
		return fmt.Errorf("No down file for migration - migration is probably unsafe")
	}

	golly.Say("yellow", "Running down migration for version: %s", version)
	if err := executeSQL(db, files[0]); err != nil {
		return err
	}

	return db.Delete(&sm).Error
}

// MigrationPerform applies all missing migrations to bring the database up to date.
func MigrationPerform(db *gorm.DB, args []string) error {
	missing := missingMigrations(db)

	if len(missing) == 0 {
		golly.Say("green", "Nothing to migrate")
		return nil
	}

	for _, migration := range missing {
		sm := SchemaMigration{Version: migration[0], File: migration[1]}

		golly.Say("yellow", "Running Migration: %s", migration[0])
		if err := executeSQL(db, migration[1]); err != nil {
			return err
		}

		if err := db.Create(&sm).Error; err != nil {
			return err
		}
		golly.Say("green", "Migrated to: %s", migration[0])
	}
	return nil
}

// MigrationSeed runs a seed script located in the seed.go file.
func MigrationSeed(db *gorm.DB, args []string) error {
	seedFile := filepath.Join(dbFolder, "seed.go")

	if _, err := os.Stat(seedFile); os.IsNotExist(err) {
		return fmt.Errorf("No seed file specified in %s", seedFile)
	}

	out, _ := exec.Command("go", "run", seedFile).Output()
	golly.Say("green", "Seed Output:\n%s", string(out))
	return nil
}
