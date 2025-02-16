package eventsource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test for getStreams
func TestGetStreams(t *testing.T) {
	sm := &StreamManager{streams: make(map[string]*Stream)}
	sm.RegisterStream(NewStream(StreamOptions{Name: "stream1", NumPartitions: 1}))
	sm.RegisterStream(NewStream(StreamOptions{Name: "stream2", NumPartitions: 1}))

	result := sm.getStreams()

	if len(result) != 2 {
		t.Errorf("expected 2 streams, got %d", len(result))
	}
}

func TestStreamManager_GetOrCreateStream(t *testing.T) {
	tests := []struct {
		name           string
		initialStreams []string // streams to pre-register
		lookupName     string   // name we call GetOrCreateStream with
		autoCreate     bool
		expectOk       bool // whether we expect the method to return ok == true
		expectExists   bool // whether the stream should exist after the call
	}{
		{
			name:           "StreamAlreadyExists",
			initialStreams: []string{"myStream"},
			lookupName:     "myStream",
			expectOk:       true, // it exists
			expectExists:   true, // remains in the manager
		},
		{
			name:           "StreamNotExist",
			initialStreams: []string{"otherStream"},
			lookupName:     "nonExistent",
			expectOk:       true, // won't create, so returns ok==false
			expectExists:   true, // still doesn't exist after call
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sm := NewStreamManager()

			for _, sName := range tc.initialStreams {
				sm.RegisterStream(NewStream(StreamOptions{Name: sName, NumPartitions: 1}))
			}

			s, err := sm.GetOrCreateStream(StreamOptions{Name: tc.lookupName, NumPartitions: 1})

			if tc.expectOk {
				require.NoError(t, err, "expected no error in test %s", tc.name)
			} else {
				require.Error(t, err, "expected error in test %s", tc.name)
			}

			if !tc.expectOk {
				// If we expected not ok, the stream returned should be nil
				assert.Nil(t, s, "expected nil stream when ok=false")
			} else {
				// We expected ok==true, so we should have a non-nil stream
				require.NotNil(t, s, "stream should not be nil when ok=true")
				// optional: confirm the name matches
				assert.Equal(t, tc.lookupName, s.Name(), "stream name mismatch")
			}

			_, existsInMap := sm.Get(tc.lookupName)
			assert.Equal(t, tc.expectExists, existsInMap, "stream existence mismatch")
		})
	}
}

func BenchmarkStreamManager_GetOrCreateStream(b *testing.B) {
	sm := NewStreamManager()
	sm.RegisterStream(NewStream(StreamOptions{Name: "existingStream", NumPartitions: 1}))

	for i := 0; i < b.N; i++ {
		_, _ = sm.GetOrCreateStream(StreamOptions{Name: "existingStream", NumPartitions: 1})
	}
}
