package eventsource

import (
	"reflect"
	"strings"
	"sync"
)

var handlerCache = NewHandlerCache()

type HandlerCache struct {
	mu sync.RWMutex
	m  map[reflect.Type]map[string]int
}

func NewHandlerCache() *HandlerCache {
	return &HandlerCache{m: make(map[reflect.Type]map[string]int)}
}

func (hc *HandlerCache) Reset() {
	hc.mu.Lock()
	hc.m = make(map[reflect.Type]map[string]int)
	hc.mu.Unlock()
}

// loadIdxMap returns the cached handler index map keyed by POINTER type.
func (hc *HandlerCache) loadIdxMap(t reflect.Type) (map[string]int, bool) {
	hc.mu.RLock()
	m, ok := hc.m[t]
	hc.mu.RUnlock()
	return m, ok
}

// buildHandlerIndex scans exported methods ending with "Handler" on the POINTER type
// and records their indexes. It validates the signature: func (agg *T) <Name>Handler(Event)
// with no return values.
func (hc *HandlerCache) buildHandlerIndex(t reflect.Type) map[string]int {
	// Ensure we index the POINTER type so pointer-receiver methods are included.
	// For a non-pointer type T, the method set does NOT include methods with receiver *T.
	if t.Kind() != reflect.Ptr {
		t = reflect.PointerTo(t)
	}

	// Pre-size to the number of exported methods; some will be skipped
	idx := make(map[string]int, t.NumMethod())
	for i := 0; i < t.NumMethod(); i++ {
		m := t.Method(i)
		name := m.Name
		if !strings.HasSuffix(name, "Handler") {
			continue
		}
		mt := m.Type
		// mt is: func(receiver, <params...>) <returns...>
		// we require exactly one param (Event) and no returns.
		if mt.NumIn() != 2 || mt.In(1) != eventRType || mt.NumOut() != 0 {
			// skip mismatched handlers instead of blowing up at runtime
			continue
		}
		idx[name] = i
	}
	return idx
}

// getOrBuildHandlers returns the per-type handler index map, building it once if needed.
// We normalize the key to the POINTER type (interfaces typically yield pointer types already).
func (hc *HandlerCache) getOrBuildHandlers(ag any) map[string]int {
	t := reflect.TypeOf(ag)
	// Build/load the cache keyed by POINTER type so indexes match reflect.Value.Method(i)
	if t.Kind() != reflect.Ptr {
		t = reflect.PointerTo(t)
	}

	if m, ok := hc.loadIdxMap(t); ok {
		return m
	}

	// Slow path: build then publish under write lock (double-checked)
	hc.mu.Lock()
	if m, ok := hc.m[t]; ok {
		hc.mu.Unlock()
		return m
	}
	m := hc.buildHandlerIndex(t)
	hc.m[t] = m
	hc.mu.Unlock()
	return m
}
