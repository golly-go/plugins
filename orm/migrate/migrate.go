package migrate

import (
	"fmt"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	orm "github.com/golly-go/plugins/orm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

// Commands migration commands to be imported into an application
// these commands allow for sql based migrations.
var commands = []*cobra.Command{
	{
		Use:   "init",
		Short: "Init Migration System",
		Run: func(cmd *cobra.Command, args []string) {
			boot(args, MigrationInit)
		},
	},
	{
		Use:   "generate [fname]",
		Short: "Generate migration up and down files",
		Run: func(cmd *cobra.Command, args []string) {
			boot(args, MigrationGenerate)
		},
	},
	{
		Use:   "migrate",
		Short: "Run all migration ups till db is upto date",
		Run: func(cmd *cobra.Command, args []string) {
			boot(args, MigrationPerform)
		},
	},
	{
		Use:   "down [version]",
		Short: "Run a single down for the given version",
		Run: func(cmd *cobra.Command, args []string) {
			boot(args, MigrationDown)
		},
	},
	{
		Use:   "version",
		Short: "returns the current db version",
		Run: func(cmd *cobra.Command, args []string) {
			boot(args, MigrationVersion)
		},
	},
}

func Command() *cobra.Command {
	cmd := &cobra.Command{
		Use:              "migration",
		TraverseChildren: true,
	}

	cmd.AddCommand(commands...)
	return cmd
}

func boot(args []string, fn func(*viper.Viper, *gorm.DB, []string) error) {
	err := golly.Boot(func(a golly.Application) error {
		db := orm.Connection()

		if db == nil {
			return errors.WrapFatal(fmt.Errorf("orm: no connection established, did you add the initializers?"))
		}
		return fn(a.Config, db, args)
	})

	if err != nil {
		panic(err)
	}
}
