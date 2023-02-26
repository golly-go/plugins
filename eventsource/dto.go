package eventsource

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/slimloans/golly/errors"
)

type AggregateReference struct {
	Name string `json:"name"`
	ID   string `json:"id"`
}

type CommandDTO struct {
	Command string `json:"command"`

	Aggregate AggregateReference `json:"aggregate"`

	Data     interface{} `json:"data"`
	Metadata Metadata    `json:"metadata"`
}

// FromCommandDTO create a new command and aggregate based on the looked up DTO
func FromCommandDTO(dto CommandDTO) (Command, Aggregate, error) {

	reg := FindRegistryByAggregateName(dto.Aggregate.Name)

	if reg == nil {
		return nil, nil, errors.WrapNotFound(fmt.Errorf("no such aggregate"))
	}

	cmd := reg.FindCommand(dto.Command)
	if cmd == nil {
		return nil, nil, errors.WrapNotFound(fmt.Errorf("no such command"))
	}

	b, _ := json.Marshal(dto.Data)

	dataValue := reflect.New(reflect.TypeOf(cmd))
	marshal := dataValue.Elem().Addr()

	json.Unmarshal(b, marshal.Interface())

	var ag interface{} = reg.Aggregate

	aggregateVal := reflect.ValueOf(reg.Aggregate)
	if aggregateVal.Kind() == reflect.Ptr {
		ag = aggregateVal.Elem().Interface()
	}

	aggregate := reflect.New(reflect.TypeOf(ag)).Interface().(Aggregate)

	if dto.Aggregate.ID != "" {
		aggregate.SetID(dto.Aggregate.ID)
	}

	return marshal.Elem().Interface().(Command),
		aggregate,
		nil
}
