package mongo

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/gertd/go-pluralize"
	"github.com/golly-go/golly/utils"
)

var (
	matchFirstCap = regexp.MustCompile("(.)([A-Z][a-z]+)")
	matchAllCap   = regexp.MustCompile("([a-z0-9])([A-Z])")

	ErrUnsupportedDataType = errors.New("unsupported data type")
)

type CollectionNamer interface {
	CollectionName() string
}

func collectionName(doc interface{}) (string, error) {
	t, e := ResolveToType(doc)

	if e == nil && !t.IsNil() {
		if namer, ok := t.Interface().(CollectionNamer); ok {
			return namer.CollectionName(), nil
		}

		s := snakeCase(utils.GetType(t.Interface()))

		return pluralize.NewClient().Pluralize(s, 2, false), nil
	}
	return "", e
}

func snakeCase(str string) string {
	snake := matchFirstCap.ReplaceAllString(str, "${1}_${2}")
	snake = matchAllCap.ReplaceAllString(snake, "${1}_${2}")
	return strings.ToLower(snake)
}

func ResolveToType(toResolve interface{}) (reflect.Value, error) {
	value := reflect.ValueOf(toResolve)

	if value.Kind() == reflect.Ptr && value.IsNil() {
		value = reflect.New(value.Type().Elem())
	}

	modelType := reflect.Indirect(value).Type()

	if modelType.Kind() == reflect.Interface {
		modelType = reflect.Indirect(reflect.ValueOf(toResolve)).Elem().Type()
	}

	for modelType.Kind() == reflect.Slice || modelType.Kind() == reflect.Array || modelType.Kind() == reflect.Ptr {
		modelType = modelType.Elem()
	}

	if modelType.Kind() != reflect.Struct {
		if modelType.PkgPath() == "" {
			return value, fmt.Errorf("%w: %+v", ErrUnsupportedDataType, toResolve)
		}
		return value, fmt.Errorf("%w: %s.%s", ErrUnsupportedDataType, modelType.PkgPath(), modelType.Name())
	}

	return reflect.New(modelType), nil
}
