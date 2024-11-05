package kafka

import (
	"time"

	"github.com/golly-go/golly"
	"github.com/sirupsen/logrus"
)

var (
	publisher *KafkaPublisher
)

type Timeout struct {
	Connection time.Duration
	Idle       time.Duration
}

type Config struct {
	MinPool    int
	MaxPool    int
	JobsBuffer int

	StartOffset int64

	Brokers []string

	Partition int

	MinRead int
	MaxRead int

	Username string
	Password string

	Version string
	Retries int

	Timeouts Timeout
}

type errorLogger struct {
	l *logrus.Entry
}

func (l errorLogger) Printf(format string, args ...interface{}) {
	l.l.Errorf(format, args...)
}

func newKafkaLogger(baseLogger *logrus.Entry, source string) *logrus.Entry {
	l := golly.NewLogger().WithFields(baseLogger.Data)

	return l.WithField("source", source)
}
