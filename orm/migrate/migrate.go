package migrate

import (
	"github.com/slimloans/golly"
	orm "github.com/slimloans/golly/plugins/orm"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"gorm.io/gorm"
)

// Commands migration commands to be imported into an application
// these commands allow for sql based migrations.
var Commands = []*cobra.Command{
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

func boot(args []string, fn func(*viper.Viper, *gorm.DB, []string) error) {
	err := golly.Boot(func(a golly.Application) error { return fn(a.Config, orm.Connection(), args) })
	if err != nil {
		panic(err)
	}
}
