package main

import (
	"context"
	"fmt"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/mongo"
	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
		{
			Use:  "find-uuid [database] [collection] [id]",
			Args: cobra.MinimumNArgs(3),
			Run:  createCommand,
		},
		{
			Use:  "find-id [database] [collection] [id]",
			Args: cobra.MinimumNArgs(3),
			Run:  createCommand,
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

		// dbName := "testing"
		// if len(args) > 0 {
		// 	dbName = args[0]
		// }

		// fmt.Println(dbName)

		client.Connect(ctx)
		db := client.Database("testing")

		if err := client.Ping(ctx); err != nil {
			return err
		}

		switch cmd.Name() {
		case "ping":
			app.Logger.Info("connected")
			return nil
		case "find-uuid":
			id, _ := uuid.Parse(args[2])

			var result map[string]interface{}

			fmt.Printf("%s %#v\n", args[1], id.String())

			err := db.Collection(ctx, args[1]).FindByID(&result, id)

			if err != nil {
				app.Logger.Errorf("error when finding: %#v", err)
				return err
			}

			app.Logger.Infof("Found record %#v", result)
		case "find-id":
			id, _ := primitive.ObjectIDFromHex(args[2])
			var result map[string]interface{}

			err := db.Collection(ctx, args[1]).FindByID(&result, id)

			if err != nil {
				app.Logger.Errorf("error when finding: %#v", err)
				return err
			}

			app.Logger.Infof("Found record %#v", result)
		case "create-test":

			{
				test := mongo.Document{}

				err := db.Collection(ctx, test).Insert(&test)
				if err != nil {
					app.Logger.Errorf("error when inserting: %#v", err)
					return err
				}

			}

			{
				test := mongo.DocumentUUID{}

				collection := db.Collection(ctx, test)

				err := collection.Insert(&test)
				if err != nil {
					app.Logger.Errorf("error when inserting: %#v", err)
					return err
				}

				t := time.Now()
				test.DeletedAt = &t

				err = collection.UpdateOneDocument(test)

				fmt.Printf("%#v\n", err)

			}

		}

		return nil
	})
}
