package mongo

import (
	"github.com/golly-go/golly"
	"github.com/golly-go/plugins/eventsource"
	"github.com/golly-go/plugins/mongo"
)

type Repository struct{}

var (
	options = mongo.DatabaseOptions{
		Name: "zephyrian",
	}
)

func (r Repository) Load(ctx golly.Context, object interface{}) error {
	return Collection(ctx, object).FindByID(object, mongo.IDField(object))
}

func (r Repository) Save(ctx golly.Context, object interface{}) error {
	collection := Collection(ctx, object)

	if r.IsNewRecord(object) {
		return collection.Insert(object)
	}
	return collection.UpdateOneDocument(object)

}

func (Repository) IsNewRecord(obj interface{}) bool {
	return mongo.CreatedAtField(obj).IsZero()
}

func (r Repository) Transaction(ctx golly.Context, fn func(golly.Context, eventsource.Repository) error) error {
	return Connection(ctx).Transaction(ctx, func(ctx golly.Context) error {
		return fn(ctx, r)
	})
}
