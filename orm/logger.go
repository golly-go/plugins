package orm

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/golly-go/golly"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Logger struct {
	logger *golly.Entry
}

func NewLogger(driver string, disableLogger bool) *Logger {
	lg := golly.NewLogger()

	if disableLogger {
		lg.SetLevel(golly.LogLevelWarn)
	}

	return &Logger{
		logger: lg.WithField("driver", driver),
	}
}

func (l Logger) WithSourceFields() *golly.Entry {
	return l.logger.WithField("caller", FileWithLineNum())
}

// LogMode log mode
func (l *Logger) LogMode(level logger.LogLevel) logger.Interface {
	return l
}

// Info print info
func (l Logger) Info(ctx context.Context, msg string, data ...interface{}) {
	l.WithSourceFields().Infof(msg, data...)
}

// Warn print warn messages
func (l Logger) Warn(ctx context.Context, msg string, data ...interface{}) {
	l.WithSourceFields().Warnf(msg, data...)
}

// Error print error messages
func (l Logger) Error(ctx context.Context, msg string, data ...interface{}) {
	l.WithSourceFields().Errorf(msg, data...)
}

// Trace print sql message
func (l Logger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {

	elapsed := time.Since(begin)
	duration := float64(elapsed.Nanoseconds()) / 1e6

	logger := l.WithSourceFields().WithFields(golly.Fields{
		"elapsed":  duration,
		"duration": fmt.Sprintf("%v", duration),
	})

	sql, rows := fc()
	if rows != -1 {
		logger = logger.WithField("rows", rows)
	}

	switch {
	case err != nil && (!errors.Is(err, gorm.ErrRecordNotFound)):
		logger.Errorf("%s %s", err, sql)
	case elapsed >= time.Second:
		logger.Warnf("SLOW SQL >= %v (%s)", time.Second, sql)
	default:
		logger.Debug(sql)
	}
}

// FileWithLineNum returns the file name and line number of the most recent
// caller outside specific libraries or test files, optimized for production use.
func FileWithLineNum() string {
	for i := 2; i < 15; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}

		// Check exclusions as early as possible to avoid unnecessary allocations
		if !strings.Contains(file, "golly-go") &&
			!strings.Contains(file, "gorm") &&
			!strings.HasSuffix(file, "_test.go") {
			return fmt.Sprintf("%s:%d", file, line)
		}
	}

	return ""
}
