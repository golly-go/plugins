package eventsource

import (
	"context"
	"hash/fnv"
	"strconv"
	"sync"
	"testing"

	"github.com/golly-go/golly"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type perfCtxKey struct{}

type perfCaptureStream struct {
	mu   sync.Mutex
	vals []any
}

func (s *perfCaptureStream) Publish(ctx context.Context, _ string, _ any) error {
	s.mu.Lock()
	s.vals = append(s.vals, ctx.Value(perfCtxKey{}))
	s.mu.Unlock()
	return nil
}

func TestAggregateWorkerIndex_MatchesFNV32a(t *testing.T) {
	ids := []string{"", "a", "0", "7f9c24e8-3f6a-4a5e-9b0d-0123456789ab", "tenant/agg/üñí"}
	for _, id := range ids {
		for _, n := range []int{2, 3, 8, 13} {
			h := fnv.New32a()
			h.Write([]byte(id))
			assert.Equal(t, int(h.Sum32())%n, aggregateWorkerIndex(id, n), "id=%q n=%d", id, n)
		}
	}
	assert.Equal(t, 0, aggregateWorkerIndex("anything", 1))
}

func TestNameToTopicUnicode_Values(t *testing.T) {
	cases := map[string]string{
		"":                   "events.",
		"OrderCreated":       "events.ordercreated",
		"pkg/path.TestEvent": "events.pkg-path-testevent",
		"Ünï.Cödé":           "events.ünï-cödé",
		"A.B/C":              "events.a-b-c",
		"a-very-long-aggregate-name-over-the-stack-buffer-size-of-64-bytes-X": "events.a-very-long-aggregate-name-over-the-stack-buffer-size-of-64-bytes-x",
	}
	for in, want := range cases {
		assert.Equal(t, want, NameToTopicUnicode(in), in)
	}
}

func TestObjectPath_Value(t *testing.T) {
	assert.Equal(t, "github.com/golly-go/plugins/eventsource/benchAgg", ObjectPath(&benchAgg{}))
}

func TestApply_LowercaseEventTypeRoutesToHandler(t *testing.T) {
	agg := &benchAgg{}

	for _, typ := range []string{"pkg.benchBumped", "pkg/path.BenchBumped", "benchBumped"} {
		evt := Event{Type: typ, Data: benchBumped{By: 2}, Version: 1}
		require.NoError(t, apply(agg, evt))
	}
	assert.Equal(t, 6, agg.Count)

	require.NoError(t, apply(agg, Event{Type: "pkg.Unknown", Data: benchBumped{By: 9}, Version: 4}))
	assert.Equal(t, 6, agg.Count)
	assert.Equal(t, int64(4), agg.Version())
}

func TestApply_DerivesNameFromDataWhenTypeEmpty(t *testing.T) {
	agg := &benchAgg{}
	require.NoError(t, apply(agg, Event{Data: benchRenamed{Name: "zed"}, Version: 1}))
	assert.Equal(t, "zed", agg.Name)
}

func TestApply_ConcurrentHandlersSeeTheirOwnEvent(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			agg := &benchAgg{}
			for i := 1; i <= 200; i++ {
				require.NoError(t, apply(agg, Event{Type: "benchBumped", Data: benchBumped{By: g + 1}, Version: int64(i)}))
			}
			assert.Equal(t, 200*(g+1), agg.Count)
		}()
	}
	wg.Wait()
}

func TestProcessChanges_StampsEnvelopeAndTopic(t *testing.T) {
	SetUserInfoFunc(func(context.Context) UserInfo { return UserInfo{UserID: "u1", TenantID: "t1"} })
	t.Cleanup(func() { SetUserInfoFunc(nil) })

	agg := &benchAgg{ID: "agg-1"}
	agg.Record(benchRenamed{Name: "a"}, benchBumped{By: 3})
	require.NoError(t, agg.ProcessChanges(context.Background(), agg))

	ch := agg.Changes()
	require.Len(t, ch, 2)
	for i, e := range ch {
		assert.Equal(t, EventStateApplied, e.State)
		assert.Equal(t, "agg-1", e.AggregateID)
		assert.Equal(t, "eventsource.benchAgg", e.AggregateType)
		assert.Equal(t, "events.benchagg", e.Topic)
		assert.Equal(t, "u1", e.UserID)
		assert.Equal(t, "t1", e.TenantID)
		assert.Equal(t, int64(i+1), e.Version)
	}
	assert.Equal(t, 3, agg.Count)
}

type perfTopicAgg struct {
	AggregateBase
	ID  string
	top string
}

func (a *perfTopicAgg) GetID() string { return a.ID }
func (a *perfTopicAgg) Topic() string { return a.top }

func TestProcessChanges_DynamicTopicPerInstance(t *testing.T) {
	for _, top := range []string{"Alpha", "Beta.One"} {
		agg := &perfTopicAgg{ID: "x", top: top}
		agg.Record(benchBumped{By: 1})
		require.NoError(t, agg.ProcessChanges(context.Background(), agg))
		assert.Equal(t, NameToTopicUnicode(top), agg.Changes()[0].Topic)
	}
}

func TestUserInfoFunc_ConcurrentSetAndRead(t *testing.T) {
	t.Cleanup(func() { SetUserInfoFunc(nil) })

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			SetUserInfoFunc(func(context.Context) UserInfo { return UserInfo{UserID: "u"} })
			SetUserInfoFunc(nil)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			var e Event
			applyUserInfo(context.Background(), &e)
			assert.Contains(t, []string{"", "u"}, e.UserID)
		}
	}()
	wg.Wait()
}

func TestEventsByState_ExactCopy(t *testing.T) {
	evts := Events{{Version: 1, State: EventStateApplied}, {Version: 2, State: EventStateReady}, {Version: 3, State: EventStateApplied}}

	got := evts.Uncommitted()
	require.Len(t, got, 2)
	assert.Equal(t, 2, cap(got))
	assert.Equal(t, []int64{1, 3}, []int64{got[0].Version, got[1].Version})

	got[0].GlobalVersion = 99
	assert.Zero(t, evts[0].GlobalVersion, "result must not alias the source")

	none := evts.Completed()
	assert.NotNil(t, none)
	assert.Empty(t, none)
}

func TestSend_SharesDetachedContextAcrossProjectionsAndStreams(t *testing.T) {
	capture := &perfCaptureStream{}
	eng := benchEngine(WithStreams(capture))

	var mu sync.Mutex
	var projVals []any
	pm := eng.Projections()
	setEventHandlers(pm, "eventsource.benchBumped", func(ctx context.Context, _ Event) error {
		mu.Lock()
		projVals = append(projVals, ctx.Value(perfCtxKey{}))
		mu.Unlock()
		return nil
	})
	eng.Start()
	t.Cleanup(eng.Stop)

	ctx, cancel := context.WithCancel(golly.WithValue(context.Background(), perfCtxKey{}, "v"))
	evts := make([]Event, 3)
	for i := range evts {
		evts[i] = NewEvent(benchBumped{By: 1}, EventStateApplied)
		evts[i].AggregateID = "agg"
		evts[i].AggregateType = "eventsource.benchAgg"
		evts[i].Topic = "events.benchagg"
	}
	eng.Send(ctx, evts...)
	cancel() // detached contexts must survive the caller's cancellation
	pm.Wait()

	assert.Equal(t, []any{"v", "v", "v"}, capture.vals)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []any{"v", "v", "v"}, projVals)
}

func TestProjectionManager_RegisterConcurrentWithHandleEvent(t *testing.T) {
	pm := NewProjectionManager()
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			pm.Register(&benchProj{})
		}
	}()
	go func() {
		defer wg.Done()
		evt := Event{Type: "eventsource.benchBumped", Topic: "t"}
		for i := 0; i < 200; i++ {
			pm.handleEvent(context.Background(), evt)
		}
	}()
	wg.Wait()

	assert.Len(t, pm.index.Load().events[AllEvents], 200, "every concurrent Register must land")
}

func TestDefaultProjectionSettings_UnsetFallsBackToInitial(t *testing.T) {
	prevW, prevB := defaultProjectionWorkers.Load(), defaultProjectionBufferSize.Load()
	t.Cleanup(func() { defaultProjectionWorkers.Store(prevW); defaultProjectionBufferSize.Store(prevB) })

	defaultProjectionWorkers.Store(0)
	defaultProjectionBufferSize.Store(0)
	assert.Equal(t, initialProjectionWorkers, DefaultProjectionWorkers())
	assert.Equal(t, initialProjectionBufferSize, DefaultProjectionBufferSize())

	SetDefaultProjectionWorkers(3)
	assert.Equal(t, 3, DefaultProjectionWorkers())
}

func TestInMemoryStore_FilterLimitAndOrder(t *testing.T) {
	var data []Event
	for i := 5; i >= 1; i-- {
		data = append(data, Event{AggregateID: "a", AggregateType: "T", Version: int64(i), GlobalVersion: int64(10 - i)})
	}
	data = append(data, Event{AggregateID: "b", AggregateType: "T", Version: 1})
	s := NewInMemoryStore(data...)

	out, err := s.LoadEvents(context.Background(), EventFilter{AggregateID: "a", OrderByVersion: true})
	require.NoError(t, err)
	var vs []string
	for _, p := range out {
		vs = append(vs, strconv.FormatInt(p.(InMemoryEvent).Version, 10))
	}
	assert.Equal(t, []string{"1", "2", "3", "4", "5"}, vs)

	lim, err := s.LoadEvents(context.Background(), EventFilter{AggregateID: "a", Limit: 2})
	require.NoError(t, err)
	assert.Len(t, lim, 2)
}

func TestAllocs_HotPathBudgets(t *testing.T) {
	if raceEnabled {
		t.Skip("sync.Pool drops items and instrumentation allocates under -race")
	}

	agg := &benchAgg{ID: "agg"}
	evt := NewEvent(benchBumped{By: 1}, EventStateReady)
	assert.Zero(t, testing.AllocsPerRun(100, func() { _ = apply(agg, evt) }), "apply")

	pm := NewProjectionManager()
	pm.Register(&benchProj{})
	assert.Zero(t, testing.AllocsPerRun(100, func() { pm.handleEvent(context.Background(), evt) }), "handleEvent")

	assert.Zero(t, testing.AllocsPerRun(100, func() { _ = aggregateWorkerIndex("7f9c24e8-3f6a-4a5e-9b0d-0123456789ab", 8) }), "worker index")

	assert.LessOrEqual(t, testing.AllocsPerRun(100, func() { _ = NameToTopicUnicode("benchAgg") }), 1.0, "topic")

	assert.LessOrEqual(t, testing.AllocsPerRun(100, func() { _ = Events{evt}.Uncommitted() }), 1.0, "ByState")
}

func TestAggregateRegistry_ConcurrentRegisterAndRead(t *testing.T) {
	r := NewAggregateRegistry()
	r.Register(&benchAgg{}, []any{benchCreated{}})

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			r.Register(&benchAgg{}, []any{benchRenamed{}, benchBumped{}})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			assert.NotNil(t, r.GetEventCodec("eventsource.benchAgg", "eventsource.benchCreated"))
			_, ok := r.GetAggregate("eventsource.benchAgg")
			assert.True(t, ok)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			for range r.List() {
			}
		}
	}()
	wg.Wait()

	assert.NotNil(t, r.GetEventCodec("eventsource.benchAgg", "eventsource.benchBumped"))
	assert.NotNil(t, r.GetEventCodec("eventsource.benchAgg", "eventsource.benchCreated"), "earlier codecs survive re-register")
	r.Clear()
	assert.Nil(t, r.GetEventCodec("eventsource.benchAgg", "eventsource.benchCreated"))
}

func TestInternalStream_ConcurrentSubscribeAndHandle(t *testing.T) {
	s := NewInternalStream("c")
	var mu sync.Mutex
	calls := 0
	h := func(context.Context, Event) { mu.Lock(); calls++; mu.Unlock() }
	s.Subscribe("t", h)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			extra := func(context.Context, Event) {}
			s.Subscribe("t", extra)
			s.Unsubscribe("t", extra)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 300; i++ {
			s.handleEvent(context.Background(), Event{Topic: "t"})
		}
	}()
	wg.Wait()

	mu.Lock()
	assert.Equal(t, 300, calls)
	mu.Unlock()
	assert.Len(t, (*s.handlers.Load())["t"], 1)
}

func TestEngine_RunningFlagConcurrent(t *testing.T) {
	eng := benchEngine()
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		eng.Start() // engines are single-use: Stop closes the projection stop channel
		eng.Stop()
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			_ = eng.IsRunning()
		}
	}()
	wg.Wait()
	assert.False(t, eng.IsRunning())
}

func TestAllocs_EnginePathBudgets(t *testing.T) {
	if raceEnabled {
		t.Skip("instrumentation allocates under -race")
	}

	eng := benchEngine()
	ctx := context.Background()
	id := seedBenchAgg(t, eng, 10)

	// Replay of a 10-event aggregate: closure, filter, store batch + one boxed event each
	assert.LessOrEqual(t, testing.AllocsPerRun(50, func() {
		_ = eng.Load(ctx, &benchAgg{ID: id})
	}), 25.0, "Load/10")

	assert.LessOrEqual(t, testing.AllocsPerRun(50, func() {
		_ = eng.Execute(ctx, &benchAgg{}, benchCreateCmd{})
	}), 14.0, "Execute new")

	assert.Zero(t, testing.AllocsPerRun(100, func() {
		_ = eng.Aggregates().GetEventCodec("eventsource.benchAgg", "eventsource.benchBumped")
	}), "GetEventCodec")

	s := NewInternalStream("a")
	s.Subscribe("t", func(context.Context, Event) {})
	evt := Event{Topic: "t"}
	assert.Zero(t, testing.AllocsPerRun(100, func() { s.handleEvent(ctx, evt) }), "stream handleEvent")

	agg := &benchAgg{ID: "agg"}
	assert.LessOrEqual(t, testing.AllocsPerRun(100, func() {
		agg.ClearChanges()
		agg.Record(benchRenamed{Name: "x"}, benchBumped{By: 1}, benchBumped{By: 2})
		_ = agg.ProcessChanges(ctx, agg)
	}), 6.0, "Record+ProcessChanges")
}
