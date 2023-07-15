package orm

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var gollySourceDir string

func init() {
	_, file, _, _ := runtime.Caller(0)

	gollySourceDir = regexp.MustCompile(`logger\.source\.go`).ReplaceAllString(file, "")
}

type Logger struct {
	logger *logrus.Entry
}

func newLogger(driver string, disableLogger bool) *Logger {
	lg := golly.NewLogger()

	if !disableLogger {
		log := logrus.New()
		log.SetLevel(logrus.WarnLevel)
		lg = log.WithFields(lg.Data)
	}

	return &Logger{
		logger: lg.WithField("driver", driver),
	}
}

func (l Logger) WithSourceFields() *logrus.Entry {
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

// FileWithLineNum return the file name and line number of the current file
func FileWithLineNum() string {
	// the second caller usually from gorm internal, so set i start from 2
	for i := 2; i < 15; i++ {
		_, file, line, ok := runtime.Caller(i)

		if ok {
			notLibrary :=
				!strings.Contains(file, "golly-go") &&
					!strings.Contains(file, "gorm") &&
					!strings.HasSuffix(file, "_test.go")

			if notLibrary {
				return file + ":" + strconv.FormatInt(int64(line), 10)
			}
		}
	}
	return ""
}
