package orm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
)

func TestBeforeConnect(t *testing.T) {
	tests := []struct {
		name     string
		config   PostgresConfig
		expected string
		err      bool
	}{
		{name: "test1", config: PostgresConfig{
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
		}},

		{name: "before connect", config: PostgresConfig{
			Host:     "localhost",
			Port:     5432,
			User:     "postgres",
			Password: "postgres",
			Database: "postgres",
			BeforeConnect: func(ctx context.Context, config *pgx.ConnConfig) error {
				config.Password = "test"
				return nil
			},
		}},
		{
			name: "auth token",
			config: PostgresConfig{
				AuthToken: func() (string, error) {
					return "test", nil
				},
			},
		},
		{
			name: "auth token error",
			config: PostgresConfig{
				AuthToken: func() (string, error) {
					return "", errors.New("error")
				},
			},
			err: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := beforeConnectWrapper(test.config)(context.Background(), &pgx.ConnConfig{})
			if test.err {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
