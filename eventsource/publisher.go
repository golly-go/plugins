package eventsource

type Publishable interface {
	Topic() string
}
