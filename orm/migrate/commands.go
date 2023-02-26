package migrate

import (
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/slimloans/golly/errors"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

type SchemaMigration struct {
	Version string `gorm:"primaryKey;autoIncrement:false"`
	File    string
}

var dbFolder = "db"
var migrationPath string = fmt.Sprintf("%s/migrations/", dbFolder)

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

func createDBFolderIfNotExist() error {
	dbFolderPath := "./" + migrationPath

	if _, err := os.Stat(dbFolderPath); !os.IsNotExist(err) {
		return nil
	}

	fmt.Println("Creating migration folder: ", dbFolderPath)
	return os.MkdirAll(dbFolderPath, 0700)
}

func MigrationInit(v *viper.Viper, db *gorm.DB, args []string) error {
	if err := createDBFolderIfNotExist(); err != nil {
		return err
	}

	if err := db.AutoMigrate(&SchemaMigration{}); err != nil {
		return err
	}

	fmt.Println("Created schema migrations table.")
	return nil
}

func MigrationGenerate(v *viper.Viper, db *gorm.DB, args []string) error {
	version := newVersionString()

	fileSlug := fmt.Sprintf("%s_%s", version, formatSlug(args[0]))

	upFile := migrationPath + fileSlug + ".up.sql"
	downFile := migrationPath + fileSlug + ".down.sql"

	if file, err := os.Create(upFile); err == nil {
		file.WriteString("-- Up Migration " + version + " " + args[0])
	} else {
		return err
	}

	if file, err := os.Create(downFile); err == nil {
		file.WriteString("-- Down Migration " + version + " " + args[0])
	} else {
		os.Remove(upFile)
		return err
	}

	fmt.Println("Generated: ", upFile)
	fmt.Println("Generated: ", downFile)
	return nil
}

func MigrationVersion(v *viper.Viper, db *gorm.DB, args []string) error {
	sm := SchemaMigration{}

	if err := db.Model(&sm).Last(&sm).Error; err != nil {
		fmt.Println("Database Version: No migrations")
	} else {
		fmt.Println("Database Version: ", sm.Version)
	}

	fmt.Println("\nMissing Migrations")
	for _, migration := range missingMigrations(db) {
		fmt.Println("\t", migration[0])
	}

	return nil
}

func MigrationDown(v *viper.Viper, db *gorm.DB, args []string) error {
	var sm SchemaMigration

	defer func(start time.Time) {
		fmt.Printf("Completd migration in %v\n", time.Now().Sub(start))
	}(time.Now())

	if err := db.Model(&sm).Find(&sm, "version = ?", args[0]).Error; err != nil {
		fmt.Println("Migration version does not exist")
		return err
	}

	files, err := filepath.Glob(fmt.Sprintf("%s%s*.down.sql",
		migrationPath,
		args[0]))

	if err != nil {
		return err
	}

	fmt.Println("Running down on version: ", args[0])
	if len(files) == 0 {
		return fmt.Errorf("no down file for migration -  migration is probably unsafe.")
	}

	file := files[0]
	if err := executeSQL(db, file); err != nil {
		return db.Error
	}

	return db.Model(&sm).Delete(sm).Error
}

func MigrationPerform(v *viper.Viper, db *gorm.DB, args []string) error {
	defer func(start time.Time) {
		fmt.Printf("Completd migration in %v\n", time.Now().Sub(start))
	}(time.Now())

	return migrate(db)
}

func migrate(db *gorm.DB) error {
	missing := missingMigrations(db)
	if len(missing) == 0 {
		fmt.Println("Nothing to migrate")
		return nil
	}

	for _, migration := range missing {
		sm := SchemaMigration{
			Version: migration[0],
			File:    migration[1],
		}

		fmt.Println("\nRunning Migration: ", migration[0])
		if err := executeSQL(db, migration[1]); err != nil {
			return err
		}

		if err := db.Create(&sm).Error; err != nil {
			return errors.WrapGeneric(err)
		}
		fmt.Println("Migrated to: ", migration[0])
	}
	return nil
}

func MigrationSeed(v *viper.Viper, db *gorm.DB, args []string) error {
	seedFile := filepath.Join(dbFolder, "seed.go")

	if _, err := os.Stat(seedFile); os.IsNotExist(err) {
		return fmt.Errorf("no seed specified in %s", seedFile)

	}
	out, _ := exec.Command("go", "run", seedFile).Output()
	fmt.Println(string(out))
	return nil
}

func executeSQL(db *gorm.DB, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	defer file.Close()

	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	d := db.Begin()

	stmts := strings.Split(string(bytes), "-- endStatement")
	for _, stmt := range stmts {
		if stmt != "" && len(stmt) > 1 {
			st := strings.Replace(stmt, "-- beginStatement", "-- ", -1)
			if err := d.Exec(st).Error; err != nil {
				d.Rollback()
				return err
			}
		}
	}

	d.Commit()
	return nil
}

func formatSlug(str string) string {
	return strings.Replace(str, " ", "_", -1)
}

func missingMigrations(db *gorm.DB) [][]string {
	fmt.Print("HERE")

	if err := db.AutoMigrate(&SchemaMigration{}); err != nil {

		fmt.Println("Cannot create schema migrations table: ", err)
		return [][]string{}
	}

	missing := [][]string{}

	files, err := filepath.Glob(migrationPath + "*.up.sql")
	if err != nil {
		panic(err)
	}

	for _, file := range files {
		var sm SchemaMigration

		start := strings.LastIndex(file, "/")
		stop := strings.Index(file, "_")

		version := file[start+1 : stop]

		db.Model(&sm).Find(&sm, "version = ?", version)
		if sm.Version == "" {
			missing = append(missing, []string{version, file})
		}
	}

	return missing
}
