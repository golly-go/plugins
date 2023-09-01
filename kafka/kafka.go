package kafka

import (
	"time"

	"github.com/golly-go/golly"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"

	"github.com/spf13/viper"
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

	GroupID string

	Username string
	Password string

	Version string

	BalanceStrategy string

	SASLAlgorythm string

	Retries int

	Timeouts Timeout

	UseErrorLogger bool
}

type errorLogger struct {
	l *logrus.Entry
}

func (l errorLogger) Printf(format string, args ...interface{}) {
	l.l.Errorf(format, args...)
}

func InitDefaultConfig(config *viper.Viper) {
	config.SetDefault("kafka", map[string]interface{}{
		"username": "",
		"password": "",
		"brokers":  []string{"localhost:9092"},
		"retries":  1,
		"timeouts": map[string]interface{}{
			"connection": 10,
			"idle":       45,
		},
		"consumer": map[string]interface{}{
			"workers": map[string]interface{}{
				"min":    1,
				"max":    25,
				"buffer": 50,
			},
			"bytes": map[string]int{
				"min": 10_000, // 10e3,
				"max": 10_000_000,
			},
			"start_offset":     kafka.FirstOffset,
			"partition":        0,
			"wait":             "50ms",
			"group_id":         "default-group",
			"balance_strategy": "",
		}})
}

func NewConfig(config *viper.Viper) Config {

	brokers := config.GetStringSlice("kafka.address")

	if len(brokers) < 1 {
		brokers = config.GetStringSlice("kafka.consumer.brokers")
	}

	if len(brokers) < 1 {
		brokers = config.GetStringSlice("kafka.brokers")
	}

	user := config.GetString("kafka.consumer.username")
	if user == "" {
		user = config.GetString("kafka.username")
	}

	password := config.GetString("kafka.consumer.password")
	if password == "" {
		password = config.GetString("kafka.password")
	}

	c := Config{
		// Pool Config
		MinPool:     config.GetInt("kafka.consumer.workers.min"),
		MaxPool:     config.GetInt("kafka.consumer.workers.max"),
		JobsBuffer:  config.GetInt("kafka.consumer.workers.buffer"),
		Retries:     config.GetInt("kafka.retries"),
		StartOffset: config.GetInt64("kafka.consumer.start_offset"),
		Username:    user,
		Password:    password,

		// Kafka Config
		Brokers:   brokers,
		Partition: config.GetInt("kafka.consumer.partition"),
		MinRead:   10e2, // 10e3, // 1KB
		MaxRead:   10e6, // 10MB

		GroupID: config.GetString("kafka.consumer.group_id"),

		Version: config.GetString("kafka.version"),

		Timeouts: Timeout{
			Connection: time.Duration(config.GetInt("kafka.timeouts.connection")) * time.Second,
			Idle:       time.Duration(config.GetInt("kafka.timeouts.idle")) * time.Second,
		},

		BalanceStrategy: config.GetString("kafka.consumer.balance_strategy"),
		UseErrorLogger:  config.GetBool("kafka.consumer.error_logger"),
	}

	return c
}

func newKafkaLogger(baseLogger *logrus.Entry, source string) *logrus.Entry {
	l := golly.NewLogger().WithFields(baseLogger.Data)

	return l.WithField("source", source)
}
