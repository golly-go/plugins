package eventsource

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

// hcAgg is a local aggregate used to validate handler discovery
type hcAgg struct{}

// Valid handlers (correct signature)
func (a *hcAgg) AHandler(Event) {}
func (a *hcAgg) BHandler(Event) {}

// Invalid handlers (wrong signatures) that must be ignored by the indexer
func (a *hcAgg) BadHandlerString(string)       {}
func (a *hcAgg) BadHandlerTwo(Event, int)      {}
func (a *hcAgg) BadHandlerReturns(Event) error { return nil }

func TestHandlerCache_BuildsOnlyValidHandlers(t *testing.T) {
	handlerCache.Reset()

	m := handlerCache.getOrBuildHandlers(&hcAgg{})

	cases := []struct {
		name        string
		key         string
		wantPresent bool
	}{
		{name: "valid AHandler present", key: "AHandler", wantPresent: true},
		{name: "valid BHandler present", key: "BHandler", wantPresent: true},
		{name: "invalid string param excluded", key: "BadHandlerString", wantPresent: false},
		{name: "invalid two params excluded", key: "BadHandlerTwo", wantPresent: false},
		{name: "invalid returns excluded", key: "BadHandlerReturns", wantPresent: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, ok := m[tc.key]
			if tc.wantPresent {
				assert.True(t, ok, "expected %s to be indexed", tc.key)
			} else {
				assert.False(t, ok, "did not expect %s to be indexed", tc.key)
			}
		})
	}
}

func TestHandlerCache_PointerAndValueTypesShareIndex(t *testing.T) {
	handlerCache.Reset()

	// First call with pointer
	mp := handlerCache.getOrBuildHandlers(&hcAgg{})
	assert.NotEmpty(t, mp, "expected non-empty handler map for pointer receiver")

	// Second call with value type should return the same underlying map
	mv := handlerCache.getOrBuildHandlers(hcAgg{})
	assert.NotEmpty(t, mv, "expected non-empty handler map for value receiver")

	// Mutate one and ensure the other sees the change (proves same map instance)
	mp["ZZZHandler"] = 123
	_, ok := mv["ZZZHandler"]
	assert.True(t, ok, "expected cached map to be shared between pointer and value types")
}

func TestHandlerCache_ResetClearsEntries(t *testing.T) {
	handlerCache.Reset()

	_ = handlerCache.getOrBuildHandlers(&hcAgg{})
	// Ensure present
	if m, ok := handlerCache.loadIdxMap(reflect.TypeOf(&hcAgg{})); assert.True(t, ok) {
		assert.NotEmpty(t, m, "expected handler map to be present before reset")
	}

	handlerCache.Reset()

	_, ok := handlerCache.loadIdxMap(reflect.TypeOf(&hcAgg{}))
	assert.False(t, ok, "expected handler map to be cleared after reset")
}

// ------------------
// Benchmarks
// ------------------

func BenchmarkHandlerCache_GetOrBuild_Cold(b *testing.B) {
	for i := 0; i < b.N; i++ {
		handlerCache.Reset()
		_ = handlerCache.getOrBuildHandlers(&hcAgg{})
	}
}

func BenchmarkHandlerCache_GetOrBuild_Hot(b *testing.B) {
	handlerCache.Reset()
	_ = handlerCache.getOrBuildHandlers(&hcAgg{}) // seed

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = handlerCache.getOrBuildHandlers(&hcAgg{})
	}
}
