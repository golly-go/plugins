package neo4j

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/golly-go/golly"
	"github.com/golly-go/golly/utils"
	"github.com/iancoleman/strcase"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/config"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/sirupsen/logrus"
)

var (
	Connection neo4j.DriverWithContext
)

type Neo4JObject interface {
	Neo4jAttributes() map[string]any
}

func setDefaults(app golly.Application) {
	app.Config.SetDefault("neo4j", map[string]interface{}{
		"username": "neo4j",
		"password": "letmein!",
		"url":      "neo4j://localhost",
	})
}

type Logger struct {
	logger *logrus.Entry
}

func (l Logger) Error(name, id string, err error) {
	l.logger.WithFields(logrus.Fields{"name": name, "id": id}).Errorf("error: %#v", err)
}

func (l Logger) Warnf(name, id, msg string, args ...any) {
	l.logger.WithFields(logrus.Fields{"name": name, "id": id}).Warnf(msg, args...)
}

func (l Logger) Infof(name, id, msg string, args ...any) {
	l.logger.WithFields(logrus.Fields{"name": name, "id": id}).Infof(msg, args...)
}

func (l Logger) Debugf(name, id, msg string, args ...any) {
	l.logger.WithFields(logrus.Fields{"name": name, "id": id}).Debugf(msg, args...)
}

func Initializer(app golly.Application) error {
	setDefaults(app)

	d, err := neo4j.NewDriverWithContext(app.Config.GetString("neo4j.url"),
		neo4j.BasicAuth(
			app.Config.GetString("neo4j.username"),
			app.Config.GetString("neo4j.password"),
			"",
		),
		func(config *config.Config) {
			config.Log = Logger{logger: golly.NewLogger().WithField("driver", "neo4j")}
		},
	)

	if err != nil {
		return err
	}

	if err := d.VerifyConnectivity(context.Background()); err != nil {
		return err
	}

	Connection = d

	golly.Events().Add(golly.EventAppShutdown, func(ctx golly.Context, e golly.Event) error {
		Connection.Close(ctx.Context())
		return nil
	})

	return nil
}

func DriveValues(i interface{}, prefix string) map[string]interface{} {
	tpe := reflect.ValueOf(i)

	if tpe.Kind() == reflect.Ptr {
		tpe = tpe.Elem()
	}

	if tpe.Kind() == reflect.Slice {
		tpe = reflect.New(tpe.Type().Elem()).Elem()
	}

	ret := map[string]interface{}{}

	for pos := tpe.NumField() - 1; pos >= 0; pos-- {
		field := tpe.Type().Field(pos)
		val := tpe.Field(pos)

		name := fieldName(prefix, strings.ToLower(field.Name))

		if n := field.Tag.Get("neo4j"); n == "-" {
			continue
		}

		switch val.Kind() {
		case reflect.Struct:
			k := field.Tag.Get("neo4j")
			if k == "-" {
				continue
			}

			if k != "" {
				name = fieldName(prefix, k)
			}

			for k, v := range DriveValues(val.Interface(), name) {
				ret[k] = v
			}
		case reflect.Slice:
			k := field.Tag.Get("neo4j")
			if k == "-" {
				continue
			}

			if k != "" {
				name = fieldName(prefix, k)
			}

			t := val.Type().Elem()
			if t.Kind() == reflect.Struct {
				for k, v := range DriveValues(reflect.New(t).Elem().Interface(), name) {
					ret[k] = v
				}
			}
		default:
			k := field.Tag.Get("neo4j")
			if k == "-" {
				continue
			}

			if k != "" {
				name = fieldName(prefix, k)
			}

			ret[name] = val.Interface()
		}
	}
	return ret
}

func fieldName(prefix, value string) string {
	if prefix == "" {
		return value
	}

	return strcase.ToLowerCamel(fmt.Sprintf("%s_%s", prefix, value))
}

func CreateNode(ctx golly.Context, obj Neo4JObject) (string, error) {
	ret := []string{}

	values := obj.Neo4jAttributes()

	for k := range values {
		ret = append(ret, fmt.Sprintf("%s: $%s", k, k))
	}

	res, err := neo4j.ExecuteQuery(ctx.Context(),
		Connection,
		fmt.Sprintf("CREATE (n:%s { %s }) RETURN n", utils.GetType(obj), strings.Join(ret, ", ")),
		values,
		neo4j.EagerResultTransformer)

	if err != nil {
		return "", err
	}

	return res.Records[0].Values[0].(dbtype.Node).ElementId, nil
}
