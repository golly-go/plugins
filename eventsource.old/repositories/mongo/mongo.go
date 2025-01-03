package mongo

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/mongo"
)

func Connection(ctx golly.Context) mongo.Client {
	return mongo.Connection(ctx)
}

func Database(ctx golly.Context) mongo.Client {
	return Connection(ctx).Database(ctx, options)
}

func Collection(ctx golly.Context, object interface{}) mongo.Collection {
	return Database(ctx).Collection(ctx, object)
}
