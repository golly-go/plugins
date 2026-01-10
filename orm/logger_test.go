package orm

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm/logger"
)

func TestLogger_LogMode(t *testing.T) {
	// Setup logrus hook to capture logs
	nullLogger, hook := test.NewNullLogger()
	nullLogger.SetLevel(logrus.InfoLevel) // Application running at Info level

	// Create our ORM logger, manually injecting the null logger entry
	// We can't use NewLogger easily because it depends on golly.Logger(),
	// so we construct it manually for this test.
	ormLogger := &Logger{
		logger: nullLogger.WithField("driver", "test"),
	}

	// Case 1: Default behavior (should be consistent with current bug)
	// LogMode(Info) is called, but currently it does nothing.
	// We expect NO log because implementation uses Debug(), and our logger is at InfoLevel.

	// Simulate db.Debug() calling LogMode(Info)
	debugLogger := ormLogger.LogMode(logger.Info)

	ctx := context.Background()
	timestamp := time.Now()
	sql := "SELECT * FROM users"

	// trigger trace
	debugLogger.Trace(ctx, timestamp, func() (string, int64) {
		return sql, 1
	}, nil)

	// In the BUGGY version, this should fail to log anything because:
	// 1. LogMode ignored the Info level.
	// 2. Trace logged at Debug.
	// 3. Logger is at Info.
	assert.Empty(t, hook.LastEntry(), "Expected no log entry in buggy version when logger is Info and Trace uses Debug")

	// Clear hook
	hook.Reset()
}
