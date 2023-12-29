package mongo

import (
	"time"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"golang.org/x/net/context"
)

type Client interface {
	Connect(ctx golly.Context) error
	Disconnect(ctx golly.Context) error
	IsConnected(ctx golly.Context) bool
	Ping(ctx golly.Context, timeout ...time.Duration) error
	Database(ctx golly.Context, options DatabaseOptions) Client
	Collection(ctx golly.Context, obj interface{}) Collection
	Transaction(ctx golly.Context, fn func(ctx golly.Context) error) error
}

type MongoClient struct {
	*mongo.Client

	database *mongo.Database
}

type DatabaseOptions struct {
	Name           string
	NamingFunction func(golly.Context) string
}

func (c *MongoClient) Connect(ctx golly.Context) error {
	client, err := mongo.Connect(
		ctx.Context(),
		makeMongoOptions(ctx),
	)

	if err != nil {
		return err
	}

	c.Client = client
	return nil
}

func (c *MongoClient) Disconnect(gctx golly.Context) error {
	return c.Client.Disconnect(contextWithDeadline(gctx))
}

func contextWithDeadline(gctx golly.Context, durs ...time.Duration) context.Context {
	c := gctx.Context()

	duration := 5 * time.Second
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

	opts := options.Client().ApplyURI(ctx.Config().GetString("mongo.url")).
		SetRegistry(createCustomRegistry().Build())

	username := ctx.Config().GetString("mongo.user")
	if username != "" {
		opts.SetAuth(options.Credential{
			Username: username,
			Password: ctx.Config().GetString("mongo.pass"),
		})
	}

	return opts
}

func (c *MongoClient) IsConnected(gctx golly.Context) bool {
	return c.Ping(gctx) == nil
}

func (c *MongoClient) Ping(gctx golly.Context, timeout ...time.Duration) error {
	ctx := contextWithDeadline(gctx, timeout...)

	if err := c.Client.Ping(ctx, readpref.Primary()); err != nil {
		return errors.WrapGeneric(err)
	}
	return nil
}

func (c *MongoClient) Database(gctx golly.Context, options DatabaseOptions) Client {
	if c.database == nil {
		dbName := options.Name

		if options.NamingFunction != nil {
			dbName = options.NamingFunction(gctx)
		}

		c.database = c.Client.Database(dbName)
	}
	return c
}

func (c *MongoClient) Collection(gctx golly.Context, obj interface{}) Collection {
	s, err := collectionName(obj)

	if err != nil {
		panic(err)
	}

	return MongoCollection{
		Name:       s,
		gctx:       gctx,
		Collection: c.database.Collection(s),
	}
}

func (c *MongoClient) Transaction(ctx golly.Context, fn func(ctx golly.Context) error) error {
	// 1. Start a new session
	session, err := c.Client.StartSession()
	if err != nil {
		return err
	}

	defer session.EndSession(context.Background())

	// 2. Define transaction options, if necessary (e.g. read and write concerns)

	txnOptions := options.Transaction().
		SetReadConcern(readconcern.Local()).
		SetWriteConcern(writeconcern.New(writeconcern.WMajority()))

	// Begin the transaction
	err = session.StartTransaction(txnOptions)
	if err != nil {
		return err
	}

	// 3. Execute the provided function
	err = fn(ctx)
	if err != nil {
		_ = session.AbortTransaction(context.Background()) // Abort transaction on error
		return err
	}

	// 4. Commit the transaction
	err = session.CommitTransaction(context.Background())
	if err != nil {
		return err
	}

	return nil
}
