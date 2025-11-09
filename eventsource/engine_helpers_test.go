package eventsource

import (
	"testing"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
)

type sampleEvent struct {
	Name string
}

func Test_testEventToEvent_BuildsCorrectEvent(t *testing.T) {
	meta := map[string]any{"k": "v"}
	input := TestEventData{
		AggregateID:   "agg-1",
		AggregateType: "SampleAgg",
		Data:          sampleEvent{Name: "a"},
		Metadata:      meta,
	}

	out := testEventToEvent(input, 5)

	assert.Equal(t, "agg-1", out.AggregateID)
	assert.Equal(t, "SampleAgg", out.AggregateType)
	assert.Equal(t, meta, out.Metadata)
	// Type derived from the data's concrete type
	expectedType := golly.TypeNoPtr(input.Data).String()
	assert.Equal(t, expectedType, out.Type)
	assert.Equal(t, int64(5), out.GlobalVersion)
	assert.Equal(t, EventStateCompleted, out.State)
	// Data preserved
	se, ok := out.Data.(sampleEvent)
	assert.True(t, ok)
	assert.Equal(t, "a", se.Name)
}

func Test_buildInMemoryEvents_MixedInputs(t *testing.T) {
	meta := map[string]any{"m": "x"}
	// Pre-constructed event that should pass through unchanged
	pre := NewEvent(sampleEvent{Name: "pre"}, EventStateApplied, meta)
	pre.AggregateID = "agg-pre"
	pre.AggregateType = "PreAgg"
	pre.Type = golly.TypeNoPtr(sampleEvent{}).String()
	pre.GlobalVersion = 99

	inputs := []any{
		TestEventData{AggregateID: "agg-1", AggregateType: "Agg", Data: sampleEvent{Name: "one"}, Metadata: meta},
		TestEventData{AggregateID: "agg-2", AggregateType: "Agg", Data: sampleEvent{Name: "two"}, Metadata: meta},
		pre,
	}

	out := buildInMemoryEvents(inputs)
	assert.Len(t, out, 3)

	// First converted with version 1
	assert.Equal(t, "agg-1", out[0].AggregateID)
	assert.Equal(t, int64(1), out[0].GlobalVersion)
	assert.Equal(t, EventStateCompleted, out[0].State)

	// Second converted with version 2
	assert.Equal(t, "agg-2", out[1].AggregateID)
	assert.Equal(t, int64(2), out[1].GlobalVersion)
	assert.Equal(t, EventStateCompleted, out[1].State)

	// Third should remain as provided
	assert.Equal(t, pre.AggregateID, out[2].AggregateID)
	assert.Equal(t, pre.AggregateType, out[2].AggregateType)
	assert.Equal(t, pre.Type, out[2].Type)
	assert.Equal(t, int64(99), out[2].GlobalVersion)
	assert.Equal(t, EventStateApplied, out[2].State)
}
