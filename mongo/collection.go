package mongo

import (
	"reflect"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
)

type Collection struct {
	gctx golly.Context

	Name string
	Col  *mongo.Collection
}

func (c Collection) logger() *logrus.Entry {
	return c.gctx.Logger().WithFields(logrus.Fields{
		"collection": c.Name,
		"operation":  "insert",
	})
}

func (c Collection) FindOne(out interface{}, filter interface{}) error {

	res := c.Col.FindOne(c.gctx.Context(), filter)

	if err := res.Err(); err != nil {
		return err
	}

	if err := res.Decode(out); err != nil {
		return err
	}

	return nil
}

func (c Collection) UpdateOneDocument(out interface{}) error {
	_, err := c.Col.UpdateByID(c.gctx.Context(), IDField(out), bson.M{"$set": out})
	return err
}

func (c Collection) UpdateOne(out interface{}, updateDocument interface{}) error {
	_, err := c.Col.UpdateByID(c.gctx.Context(), IDField(out), updateDocument)
	return err
}

func (c Collection) FindByID(out interface{}, id interface{}) error {
	filter := bson.M{"_id": id}

	switch idT := id.(type) {
	case primitive.ObjectID, uuid.UUID:
		filter = bson.M{"_id": idT}
	}

	return c.FindOne(out, filter)
}

func (c Collection) Insert(out interface{}) (err error) {
	recordCnt := 0

	t := time.Now()

	defer func(start time.Time) {
		duration := time.Since(start)
		l := c.logger().WithFields(logrus.Fields{
			"documents": recordCnt,
			"duration":  duration,
			"elapsed":   duration.String(),
		})

		if err != nil {
			l.Errorf("error with insert on %s %#v\n", c.Name, err)
			return
		}

		l.Infof("inserted in to %s %s", c.Name, duration.String())
	}(t)

	s := reflect.ValueOf(out)
	if s.IsNil() {
		return nil
	}

	switch s.Kind() {
	case reflect.Slice:
		// Keep the distinction between nil and empty slice input
		ret := make([]interface{}, s.Len())
		recordCnt = s.Len()

		for i := 0; i < recordCnt; i++ {
			out := s.Index(i).Interface()

			setID(out, t)
			timestamps(out, t)

			ret[i] = out
		}

		_, err = c.Col.InsertMany(c.gctx.Context(), ret)
		return
	default:
		timestamps(out, t)
		setID(out, t)

		recordCnt = 1

		_, err = c.Col.InsertOne(c.gctx.Context(), out)
		return
	}
}
