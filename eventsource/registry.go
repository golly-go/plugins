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
	// Aggregate definition
	Aggregate Aggregate

	// Command regsitry is used for the DTO interface only
	// if you do not want a command to be dynamically invokable, leave
	// it off this definition
	Commands []Command
	// Events defines all potential events in the system, this is used for backwards mapping
	// in the event of playback or any remarshling of events
	Events []interface{}

	// What topics/channels are you publishing events too on this aggregate
	Topics []string
}

func (ro RegistryOptions) FindCommand(name string) Command {
	for _, cmd := range ro.Commands {
		cName := utils.GetTypeWithPackage(cmd)
		cNameShort := utils.GetType(cmd)

		if strings.EqualFold(cNameShort, name) {
			return cmd
		}

		if strings.EqualFold(cName, name) {
			return cmd
		}
	}
	return nil
}

type RegistryItem struct {
	Name      string
	ShortName string

	RegistryOptions
}

func FindRegistryByAggregateName(name string) *RegistryItem {
	for _, reg := range registry {

		if strings.EqualFold(reg.ShortName, name) {
			return &reg
		}

		if strings.EqualFold(reg.Name, name) {
			return &reg
		}

		if strings.EqualFold(reg.RegistryOptions.Aggregate.Type(), name) {
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
		ShortName:       utils.GetType(opts.Aggregate),
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
