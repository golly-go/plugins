package eventsource

import "time"

// Ordered pool is a pool of
// workers that guarantee ordering against
// Definitions
// - Queue (a worker for a designated reason)
// - Worker (Job facilitator)
// - Event a piece of data required to be played in order recieved
type Queue struct {
	lastProcessedAt time.Time
}

type OrderedPool struct {
}
