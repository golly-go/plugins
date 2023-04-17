package mongo

import (
	"reflect"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
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
