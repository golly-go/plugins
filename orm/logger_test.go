package orm

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
)

func TestLogger_LogMode(t *testing.T) {
	var buf bytes.Buffer

	lg := golly.NewLogger()
	lg.SetOutput(&buf)
	lg.SetLevel(golly.LogLevelInfo)

	ormLogger := &Logger{
		logger: lg.WithField("driver", "test"),
	}

	ctx := context.Background()
	timestamp := time.Now()
	sql := "SELECT * FROM users"

	// trigger trace
	ormLogger.Trace(ctx, timestamp, func() (string, int64) {
		return sql, 1
	}, nil)

	// Since we are at Info level and Trace uses Debug, we expect no output
	assert.Empty(t, buf.String(), "Expected no log entry when logger is LogLevelInfo and Trace uses Debug")

	// Clear buffer
	buf.Reset()
}
