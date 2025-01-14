package orm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/golly-go/golly"
	"gorm.io/gorm"
)

// newVersionString generates a new version string based on the current timestamp.
func newVersionString() string {
	t := time.Now()
	tStamp := strconv.Itoa(int(t.Unix()))[0:10]
	return fmt.Sprintf("%s%s%s%s%s",
		t.Format("2006"),
		t.Format("01"),
		t.Format("02"),
		t.Format("01"),
		tStamp)
}

// createDBFolderIfNotExist ensures the migration directory exists.
func createDBFolderIfNotExist() error {
	dbFolderPath := "./" + migrationPath

	if _, err := os.Stat(dbFolderPath); !os.IsNotExist(err) {
		return nil
	}

	golly.Say("yellow", "Creating migration folder: %s", dbFolderPath)
	return os.MkdirAll(dbFolderPath, 0700)
}

// formatSlug formats a string into a slug by replacing spaces with underscores.
func formatSlug(str string) string {
	return strings.ReplaceAll(str, " ", "_")
}

// executeSQL executes SQL statements from a file.
func executeSQL(db *gorm.DB, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	statements := strings.Split(string(content), "-- endStatement")
	for _, stmt := range statements {
		stmt = strings.TrimSpace(strings.ReplaceAll(stmt, "-- beginStatement", ""))
		if stmt != "" {
			if err := db.Exec(stmt).Error; err != nil {
				return err
			}
		}
	}

	return nil
}

// missingMigrations returns a list of migrations that have not yet been applied.
func missingMigrations(db *gorm.DB) [][]string {
	if err := db.AutoMigrate(&SchemaMigration{}); err != nil {
		golly.Say("red", "Cannot create schema migrations table: %v", err)
		return [][]string{}
	}

	files, _ := filepath.Glob(migrationPath + "*.up.sql")
	var missing [][]string

	for _, file := range files {
		version := strings.Split(filepath.Base(file), "_")[0]

		var sm SchemaMigration
		if err := db.Find(&sm, "version = ?", version).Error; err != nil || sm.Version == "" {
			missing = append(missing, []string{version, file})
		}
	}

	return missing
}
