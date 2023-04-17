package mongo

import (
	"fmt"
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"golang.org/x/net/context"
)

type Client struct {
	client   *mongo.Client
	database *mongo.Database
}

func (c *Client) Connect(ctx golly.Context) error {
	client, err := mongo.Connect(
		ctx.Context(),
		makeMongoOptions(ctx),
	)

	if err != nil {
		return err
	}

	c.client = client
	return nil
}

func (c *Client) Disconnect(gctx golly.Context) error {
	return c.client.Disconnect(contextWithDeadline(gctx))
}

func contextWithDeadline(gctx golly.Context, durs ...time.Duration) context.Context {
	c := gctx.Context()

	duration := 1 * time.Second
	if len(durs) > 0 {
		duration = durs[0]
	}

	if c.Err() != nil {
		c = context.Background()
	}

	ctx, _ := context.WithDeadline(c, time.Now().Add(duration))
	return ctx
}

func makeMongoOptions(ctx golly.Context) *options.ClientOptions {
	return options.Client().ApplyURI(ctx.Config().GetString("mongo.url")).
		SetRegistry(createCustomRegistry().Build())
}

func (c Client) IsConnected(gctx golly.Context) bool {
	return c.Ping(gctx) == nil
}

func (c Client) Ping(gctx golly.Context, timeout ...time.Duration) error {
	ctx := contextWithDeadline(gctx, timeout...)

	if err := c.client.Ping(ctx, readpref.Primary()); err != nil {
		return errors.WrapGeneric(err)
	}
	return nil
}

func (c Client) Database(name string) Client {
	c.database = c.client.Database(name)
	return c
}

func (c Client) Collection(gctx golly.Context, obj interface{}) Collection {
	s, err := collectionName(obj)

	if err != nil {
		panic(err)
	}

	fmt.Printf("FINDINGING: %s\n", s)

	collection := c.database.Collection(s)

	return Collection{
		Name: s,
		gctx: gctx,
		Col:  collection,
	}
}
