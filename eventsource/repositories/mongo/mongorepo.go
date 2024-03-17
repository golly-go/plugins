package mongo

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/golly-go/plugins/mongo"
)

type MongoRepostory struct{}

var (
	options = mongo.DatabaseOptions{
		Name: "zephyrian",
	}
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

func (r MongoRepostory) Load(ctx golly.Context, object interface{}) error {
	return Collection(ctx, object).FindByID(object, mongo.IDField(object))
}

func (r MongoRepostory) Save(ctx golly.Context, object interface{}) error {
	collection := Collection(ctx, object)

	if r.IsNewRecord(object) {
		return collection.Insert(object)
	}
	return collection.UpdateOneDocument(object)

}

func (MongoRepostory) IsNewRecord(obj interface{}) bool {
	return mongo.CreatedAtField(obj).IsZero()
}

func (r MongoRepostory) Transaction(ctx golly.Context, fn func(golly.Context, eventsource.Repository) error) error {
	return Connection(ctx).Transaction(ctx, func(ctx golly.Context) error {
		return fn(ctx, r)
	})
}
