package mongo

import (
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

type fieldsTestDocumentUUID struct {
	DocumentUUID
}

func TestSetField(t *testing.T) {
	t.Run("it should set the field if the types match", func(t *testing.T) {
		ti := time.Now()

		var examples = []struct {
			field string
			value interface{}
		}{
			{"CreatedAt", ti},
			{"UpdatedAt", ti},
			{"DeletedAt", ti},
			{"ID", uuid.New()},
			{"DeletedAt", &ti},
			{"CreatedAt", &ti},
			{"UpdatedAt", &ti},
		}
		for _, example := range examples {
			testPtr := &fieldsTestDocumentUUID{}
			test := *testPtr

			field := valueOf(&test).FieldByName(example.field)
			assert.True(t, field.IsValid())

			setField(field, example.value)

			setValue := valueOf(test).FieldByName(example.field).Interface()

			exampleValue := reflect.ValueOf(example.value)

			if field.Kind() == reflect.Ptr {
				if exampleValue.Kind() != reflect.Ptr {
					exampleValue = toPointer(exampleValue)
				}
			} else {
				if exampleValue.Kind() == reflect.Ptr {
					exampleValue = exampleValue.Elem()
				}
			}

			assert.Equal(t, exampleValue.Interface(), setValue)
		}
	})

}
