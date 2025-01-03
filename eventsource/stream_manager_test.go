package eventsource

import "testing"

// Test for getStreams
func TestGetStreams(t *testing.T) {
	sm := &StreamManager{streams: make(map[string]*Stream)}
	stream1 := sm.Register("stream1")
	stream2 := sm.Register("stream2")

	result := sm.getStreams()

	if len(result) != 2 {
		t.Errorf("expected 2 streams, got %d", len(result))
	}

	if result[0] != stream1 && result[1] != stream2 {
		t.Errorf("streams do not match registered streams")
	}
}
