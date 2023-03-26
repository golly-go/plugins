package kafka

import (
	"encoding/json"
	"time"

	"github.com/segmentio/kafka-go/protocol"
)

type Message struct {
	Topic   string
	Key     string
	Data    interface{}
	Time    time.Time
	Headers []protocol.Header
}

func (m Message) Bytes() []byte {
	if b, ok := m.Data.([]byte); ok {
		return b
	}
	return []byte{}
}

func (m Message) Marshal() []byte {
	b, _ := json.Marshal(m.Data)
	return b
}
