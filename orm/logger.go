package orm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/slimloans/golly"
	"github.com/slimloans/golly/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type Logger struct {
	logger *logrus.Entry
}

func newLogger(driver string) *Logger {
	return &Logger{
		logger: golly.NewLogger().WithField("driver", driver),
	}
}

func (l Logger) WithSourceFields() *logrus.Entry {
	return l.logger.WithField("caller", utils.FileWithLineNum())
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

	logger := l.WithSourceFields().WithFields(logrus.Fields{
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
