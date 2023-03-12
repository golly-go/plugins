package eventsource

import (
	"reflect"
	"strings"

	"github.com/golly-go/golly/utils"
)

var (
	registry RegistryT = RegistryT{}
)

type RegistryT map[reflect.Type]RegistryItem

func (rt RegistryT) FindDefinition(aggregateType string) *RegistryItem {
	for _, ro := range rt {
		if ro.Aggregate.Type() == aggregateType {
			return &ro
		}
	}
	return nil
}

type RegistryOptions struct {
	Aggregate Aggregate

	Commands []Command
	Events   []interface{}
	Topics   []string
}

func (ro RegistryOptions) FindCommand(name string) Command {
	for _, cmd := range ro.Commands {
		if utils.GetTypeWithPackage(cmd) == name {
			return cmd
		}
	}
	return nil
}

type RegistryItem struct {
	Name string

	RegistryOptions
}

func FindRegistryByAggregateName(name string) *RegistryItem {
	for _, reg := range registry {
		if strings.EqualFold(reg.Name, name) {
			return &reg
		}
	}
	return nil
}

func FindRegistryItem(ag Aggregate) *RegistryItem {
	if ri, found := registry[reflect.TypeOf(ag)]; found {
		return &ri
	}
	return nil
}

func DefineAggregate(opts RegistryOptions) {
	registry[reflect.TypeOf(opts.Aggregate)] = RegistryItem{
		Name:            utils.GetTypeWithPackage(opts.Aggregate),
		RegistryOptions: opts,
	}
}

func Aggregates() (ret []Aggregate) {
	for _, ri := range registry {
		ret = append(ret, ri.Aggregate)
	}
	return
}

//  Register(RegistryOptions{
// 		Aggregate: Aggregate,
// 		Events: []Events{
// 			AggregateCreated{},
// 			AggregateUpdated{},
// 			AggregateDeleted{
// 		},
// 		Commands: []Commands{CreateAggregate{}},
// 		Topics: []string{}
// 	})
