package redis

import (
	"context"
	"encoding/json"
	"fmt"

	redis "github.com/go-redis/redis/v8"
	"github.com/golly-go/golly"
)

type Event struct {
	Channel       string
	Payload       map[string]interface{}
	PayloadString string
}

type Redis struct {
	*redis.Client

	PubSub *redis.PubSub
}

// RedisService implements the golly.Service interface for Redis.
type RedisService struct {
	address  string
	password string
	db       int
	client   *redis.Client
	events   *golly.EventManager
	running  bool
	cancel   context.CancelFunc
}

// NewRedisService creates a new Redis service instance.
func NewRedisService(address, password string, db int) *RedisService {
	return &RedisService{
		address:  address,
		password: password,
		db:       db,
		events:   &golly.EventManager{},
	}
}

// Initialize sets up the Redis client and prepares the service.
func (s *RedisService) Initialize(app *golly.Application) error {
	app.Logger().Infof("Initializing Redis connection to %s", s.address)

	s.client = redis.NewClient(&redis.Options{
		Addr:     s.address,
		Password: s.password,
		DB:       s.db,
	})

	_, err := s.client.Ping(context.Background()).Result()
	if err != nil {
		return fmt.Errorf("failed to connect to Redis: %w", err)
	}

	app.Events().Register(golly.EventShutdown, func(ctx context.Context, event any) {
		_ = s.Stop()
	})

	return nil
}

// Start begins listening for pub/sub messages on Redis.
func (s *RedisService) Start() error {
	if s.running {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.running = true

	go func() {
		pubsub := s.client.PSubscribe(ctx, "*")
		defer pubsub.Close()

		for {
			select {
			case message := <-pubsub.Channel():
				event := Event{
					Channel:       message.Channel,
					PayloadString: message.Payload,
				}

				if err := json.Unmarshal([]byte(message.Payload), &event.Payload); err == nil {
					s.events.Dispatch(golly.NewContext(ctx), event)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Stop stops the Redis service.
func (s *RedisService) Stop() error {
	if !s.running {
		return nil
	}

	if s.cancel != nil {
		s.cancel()
	}

	s.running = false
	return nil
}

// IsRunning returns whether the Redis service is running.
func (s *RedisService) IsRunning() bool {
	return s.running
}

// Client returns the underlying Redis client.
func (s *RedisService) Client() *redis.Client {
	return s.client
}

// Subscribe registers event handlers for the given channels.
func (s *RedisService) Subscribe(handler golly.EventFunc, channels ...string) error {
	for _, channel := range channels {
		s.events.Register(channel, handler)
	}
	return nil
}

// Publish sends a message to a Redis channel.
func (s *RedisService) Publish(ctx *golly.Context, channel string, payload interface{}) error {
	if s.client == nil {
		return fmt.Errorf("Redis client is not initialized")
	}

	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return s.client.Publish(ctx, channel, data).Err()
}
