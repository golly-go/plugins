package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMakeConnectionString(t *testing.T) {

	tests := []struct {
		config   SQLiteConfig
		expected string
	}{
		{SQLiteConfig{Database: "test"}, "db/test.sqlite"},
		{SQLiteConfig{Database: "test", Path: "xxxxx/"}, "xxxxx/test.sqlite"},
		{SQLiteConfig{Database: "test", Path: "test/db"}, "test/db/test.sqlite"},
		{SQLiteConfig{Database: "test", Path: "test/db", ConnectionString: "sqlite://test.sqlite"}, "sqlite://test.sqlite"},
	}

	for _, test := range tests {
		assert.Equal(t, test.expected, makeConnectionString(test.config))
	}
}
