package mongo

import (
	"fmt"
	"time"

	"github.com/golly-go/golly"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
)

var (
	ErrorNotConnected = fmt.Errorf("client not connected")

	client *Client = &Client{}
)

type MongoDB interface {
	Collection(golly.Context, interface{}) Collection
	Connect(ctx golly.Context) error
	Disconnect(ctx golly.Context) error
	IsConnected(ctx golly.Context) bool
	Database(ctx golly.Context, options DatabaseOptions) Client
	Transaction(ctx golly.Context, fn func(ctx golly.Context) error) error
	Ping(ctx golly.Context, timeout ...time.Duration) error
}

func defaultOptions(app golly.Application) {
	app.Config.SetDefault("mongo", map[string]interface{}{
		"url": "mongodb://localhost:27017",
	})
}

func Connection(ctx golly.Context) MongoDB {
	return client
}

func Initializer(app golly.Application) error {
	defaultOptions(app)

	if err := client.Connect(app.NewContext(app.GoContext())); err != nil {
		return err
	}

	golly.Events().Add(golly.EventAppShutdown, func(gctx golly.Context, e golly.Event) error {
		return client.Disconnect(gctx)
	})

	return nil
}

func createCustomRegistry() *bsoncodec.RegistryBuilder {
	var primitiveCodecs bson.PrimitiveCodecs

	rb := bsoncodec.NewRegistryBuilder()

	bsoncodec.DefaultValueEncoders{}.RegisterDefaultEncoders(rb)
	bsoncodec.DefaultValueDecoders{}.RegisterDefaultDecoders(rb)

	rb.RegisterTypeEncoder(tUUID, bsoncodec.ValueEncoderFunc(uuidEncodeValue))
	rb.RegisterTypeDecoder(tUUID, bsoncodec.ValueDecoderFunc(uuidDecodeValue))

	primitiveCodecs.RegisterPrimitiveCodecs(rb)

	return rb
}
