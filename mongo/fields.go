package mongo

import (
	"fmt"
	"reflect"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

func setField(field reflect.Value, value interface{}) {
	v := reflect.ValueOf(value)

	if field.Kind() == reflect.Ptr {
		if v.CanAddr() {
			field.SetPointer(v.Addr().UnsafePointer())
			return
		}

		if v.Kind() == reflect.Ptr {
			field.Set(v)
			return
		}

		field.Set(toPointer(v))
		return
	}

	if v.Kind() == reflect.Ptr {
		if v.CanAddr() {
			field.Set(v.Addr().Elem())
		}

		field.Set(v.Elem())
		return
	}

	fmt.Printf("%#v\n", v)
	field.Set(v)
}

func toPointer(value reflect.Value) reflect.Value {
	vp := reflect.New(value.Type())
	vp.Elem().Set(value)
	return vp
}

func setID(out interface{}, t time.Time) {
	value := reflect.ValueOf(out)

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}

	if v := value.FieldByName("ID"); v.IsValid() {
		switch id := v.Interface().(type) {
		case uuid.UUID:
			if id == uuid.Nil {
				setField(v, uuid.New())
			}
		case primitive.ObjectID:
			if id.IsZero() {
				setField(v, primitive.NewObjectID())
			}
		default:
			fmt.Printf("%#v\n", id)
		}
	}
}

func IDField(model interface{}) interface{} {
	value := valueOf(model)

	if v := value.FieldByName("ID"); v.IsValid() {
		switch id := v.Interface().(type) {
		case uuid.UUID, primitive.ObjectID:
			return id
		case map[string]interface{}:
			return id["_id"]
		default:
			return id
		}
	}
	return ""
}

func CreatedAtField(model interface{}) time.Time {
	value := valueOf(model)

	if v := value.FieldByName("CreatedAt"); v.IsValid() {
		switch cAt := v.Interface().(type) {
		case time.Time:
			return cAt
		case map[string]interface{}:
			if cAt, ok := cAt["created_at"].(time.Time); ok {
				return cAt
			}
		}
	}
	return time.Time{}
}

func timestamps(out interface{}, t time.Time) {
	value := valueOf(out)

	if v := value.FieldByName("CreatedAt"); v.IsValid() {

		switch v.Kind() {
		case reflect.Ptr:
			switch valueTime := v.Interface().(type) {
			case *time.Time:
				if valueTime.IsZero() {
					setField(v, t)
				}
			case time.Time:
				if valueTime.IsZero() {
					setField(v, &t)
				}
			}
		default:
			setField(v, t)
		}
	}

	if v := value.FieldByName("UpdatedAt"); v.IsValid() {
		setField(v, t)
	}
}

func valueOf(obj interface{}) reflect.Value {
	value := reflect.ValueOf(obj)

	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	return value
}
