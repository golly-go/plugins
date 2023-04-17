package main

import (
	"context"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/mongo"
	"github.com/spf13/cobra"
)

var (
	commands = []*cobra.Command{
		{
			Use: "ping",
			Run: createCommand,
		},
		{
			Use: "create-test",
			Run: createCommand,
		},
	}
)

func main() {
	rootCMD := cobra.Command{}
	rootCMD.AddCommand(commands...)

	golly.RegisterInitializer(mongo.Initializer)

	if err := rootCMD.Execute(); err != nil {
		panic(err)
	}

}

func createCommand(cmd *cobra.Command, args []string) {
	golly.Run(func(app golly.Application) error {
		client := &mongo.Client{}

		ctx := app.NewContext(context.Background())

		client.Connect(ctx)

		if err := client.Ping(ctx); err != nil {
			return err
		}

		switch cmd.Name() {
		case "ping":
			app.Logger.Info("connected")
			return nil
		case "create-test":
			{
				test := mongo.Document{}

				err := client.Database("testing").Collection(ctx, test).Insert(&test)
				if err != nil {
					app.Logger.Errorf("error when inserting: %#v", err)
				}
			}

			{
				test := mongo.DocumentUUID{}

				err := client.Database("testing").Collection(ctx, test).Insert(&test)
				if err != nil {
					app.Logger.Errorf("error when inserting: %#v", err)
				}

			}
		}

		return nil
	})
}
