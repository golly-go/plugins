package eventsource

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

var dtoAggregate = testAggregate{
	repo: &testRepostoryBase{},
}

type TestCommandData struct {
	Test bool `json:"test"`
}

func TestDTO(t *testing.T) {

	DefineAggregate(RegistryOptions{
		Aggregate: &dtoAggregate,
		Commands:  []Command{TestCommandEmpty{}},
	})

	t.Run("from the code", func(t *testing.T) {
		command, aggregate, err := FromCommandDTO(CommandDTO{
			Aggregate: AggregateReference{Name: "eventsource.testAggregate"},
			Command:   "eventsource.TestCommandEmpty",
			Data:      TestCommandEmpty{Test: true},
		})

		assert.NoError(t, err)
		assert.NotNil(t, aggregate)
		assert.NotNil(t, command)

		assert.True(t, command.(TestCommandEmpty).Test)

	})

	t.Run("from JSON", func(t *testing.T) {
		var dto = CommandDTO{}
		{
			err := json.Unmarshal([]byte(`
			{
				"command": "eventsource.TestCommandEmpty",
				"aggregate": { "name": "eventsource.testAggregate" },
				"data": { "test": true }
			}
		`), &dto)

			assert.NoError(t, err)
		}
		command, aggregate, err := FromCommandDTO(dto)

		assert.NoError(t, err)

		assert.NoError(t, err)
		assert.NotNil(t, aggregate)
		assert.NotNil(t, command)

		assert.True(t, command.(TestCommandEmpty).Test)

	})
}
