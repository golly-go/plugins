package eventsource

import (
	"reflect"
	"strings"
	"sync"
)

var (
	handlerCache = NewHandlerCache()

	topicerType = reflect.TypeFor[interface{ Topic() string }]()

	// eventBoxPool lends heap Events so reflect calls don't box one per call.
	eventBoxPool = sync.Pool{New: func() any { return new(Event) }}
)

const handlerSuffix = "Handler"

// handlerEntry is the immutable per-aggregate-type dispatch table.
type handlerEntry struct {
	byName  map[string]int  // "<X>Handler" -> method index
	byEvent map[string]int  // "<X>" and "<x>" -> method index (no string building on lookup)
	funcs   []reflect.Value // method index -> func(recv, Event)

	dynTopic bool   // aggregate implements Topic(); derive per instance
	topic    string // cached NameToTopicUnicode when !dynTopic
}

// topicFor returns the aggregate's topic, cached unless it supplies its own Topic().
func (e *handlerEntry) topicFor(ag any) string {
	if e.dynTopic {
		return NameToTopicUnicode(ag)
	}
	return e.topic
}

// HandlerCache is a write-once-per-type, lock-free-read cache of handler tables.
type HandlerCache struct {
	m sync.Map // reflect.Type (pointer) -> *handlerEntry
}

func NewHandlerCache() *HandlerCache { return &HandlerCache{} }

func (hc *HandlerCache) Reset() { hc.m.Clear() }

// loadIdxMap returns the cached handler index map keyed by POINTER type.
func (hc *HandlerCache) loadIdxMap(t reflect.Type) (map[string]int, bool) {
	v, ok := hc.m.Load(t)
	if !ok {
		return nil, false
	}
	return v.(*handlerEntry).byName, true
}

// buildHandlerIndex scans exported methods ending with "Handler" on the POINTER type
// and records their indexes. It validates the signature: func (agg *T) <Name>Handler(Event)
// with no return values.
func (hc *HandlerCache) buildHandlerIndex(t reflect.Type) *handlerEntry {
	// Ensure we index the POINTER type so pointer-receiver methods are included.
	// For a non-pointer type T, the method set does NOT include methods with receiver *T.
	if t.Kind() != reflect.Ptr {
		t = reflect.PointerTo(t)
	}

	n := t.NumMethod()
	ent := &handlerEntry{
		byName:   make(map[string]int, n),
		byEvent:  make(map[string]int, 2*n),
		funcs:    make([]reflect.Value, n),
		dynTopic: t.Implements(topicerType),
	}

	for i := 0; i < n; i++ {
		m := t.Method(i)
		name := m.Name
		if !strings.HasSuffix(name, handlerSuffix) {
			continue
		}
		mt := m.Type
		// mt is: func(receiver, <params...>) <returns...>
		// we require exactly one param (Event) and no returns.
		if mt.NumIn() != 2 || mt.In(1) != eventRType || mt.NumOut() != 0 {
			// skip mismatched handlers instead of blowing up at runtime
			continue
		}

		ent.byName[name] = i
		ent.funcs[i] = m.Func

		base := name[:len(name)-len(handlerSuffix)]
		ent.byEvent[base] = i
		if base != "" && base[0] >= 'A' && base[0] <= 'Z' {
			ent.byEvent[string(base[0]+('a'-'A'))+base[1:]] = i
		}
	}

	return ent
}

// entry returns the per-type handler table, building it once if needed.
// The key is normalized to the POINTER type (interfaces typically yield pointer types already).
func (hc *HandlerCache) entry(ag any) *handlerEntry {
	t := reflect.TypeOf(ag)
	if t.Kind() != reflect.Ptr {
		t = reflect.PointerTo(t)
	}

	if v, ok := hc.m.Load(t); ok {
		return v.(*handlerEntry)
	}

	ent := hc.buildHandlerIndex(t)
	ent.topic = ""
	if !ent.dynTopic {
		ent.topic = NameToTopicUnicode(ag)
	}

	v, _ := hc.m.LoadOrStore(t, ent)
	return v.(*handlerEntry)
}

// getOrBuildHandlers returns the per-type handler index map, building it once if needed.
func (hc *HandlerCache) getOrBuildHandlers(ag any) map[string]int {
	return hc.entry(ag).byName
}
