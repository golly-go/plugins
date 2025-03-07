package eventsource

import (
	"context"
	"errors"
	"testing"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
)

type TestCommand struct {
	name          string
	shouldSucceed bool
	executed      bool
	performErr    error
}

func (m *TestCommand) Perform(ctx context.Context, agg Aggregate) error {
	if m.performErr != nil {
		return m.performErr
	}

	agg.Record(testEvent{name: m.name})

	m.executed = true
	return nil
}

func TestExecute(t *testing.T) {
	ctx := golly.NewContext(context.Background())

	tests := []struct {
		name string
		agg  *TestAggregate

		cmd            *TestCommand
		expectErr      error
		expectExecuted bool
	}{
		{
			name:           "Success",
			agg:            &TestAggregate{ID: "123"},
			cmd:            &TestCommand{shouldSucceed: true},
			expectErr:      nil,
			expectExecuted: true,
		},

		{
			name:           "Command Fails",
			agg:            &TestAggregate{ID: "123"},
			cmd:            &TestCommand{performErr: errors.New("command failed")},
			expectErr:      errors.New("command failed"),
			expectExecuted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewEngine(&InMemoryStore{})

			repo.aggregates.Register(&TestAggregate{}, []any{})

			err := repo.Execute(ctx, tt.agg, tt.cmd)
			if tt.expectErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
			}

			assert.Equal(t, tt.expectExecuted, tt.cmd.executed)
		})
	}
}
