package eventsource

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golly-go/golly"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	typCreated = "eventsource.benchCreated"
	typRenamed = "eventsource.benchRenamed"
	typBumped  = "eventsource.benchBumped"
)

// spyStore counts store round trips and can inject Save failures.
type spyStore struct {
	*InMemoryStore
	batchCalls atomic.Int64
	loaded     atomic.Int64
	saveCalls  atomic.Int64
	failSaves  atomic.Int64 // remaining Saves to fail with ErrVersionConflict
}

func newSpyStore() *spyStore { return &spyStore{InMemoryStore: NewInMemoryStore()} }

func (s *spyStore) LoadEventsInBatches(ctx context.Context, n int, h func([]PersistedEvent) error, f ...EventFilter) error {
	s.batchCalls.Add(1)
	return s.InMemoryStore.LoadEventsInBatches(ctx, n, func(p []PersistedEvent) error {
		s.loaded.Add(int64(len(p)))
		return h(p)
	}, f...)
}

func (s *spyStore) Save(ctx context.Context, ev ...*Event) error {
	s.saveCalls.Add(1)
	if s.failSaves.Add(-1) >= 0 {
		return ErrVersionConflict
	}
	return s.InMemoryStore.Save(ctx, ev...)
}

func spyEngine(t testing.TB, opts ...Option) (*Engine, *spyStore) {
	st := newSpyStore()
	eng := NewEngine(append([]Option{WithStore(st)}, opts...)...)
	require.NoError(t, eng.RegisterAggregate(&benchAgg{}, []any{benchCreated{}, benchRenamed{}, benchBumped{}}))
	return eng, st
}

func storedEvents(t testing.TB, eng *Engine, id string) []Event {
	t.Helper()
	p, err := eng.Store().LoadEvents(context.Background(), EventFilter{AggregateID: id, OrderByVersion: true})
	require.NoError(t, err)
	out := make([]Event, len(p))
	for i := range p {
		e, err := p[i].Hydrate(eng)
		require.NoError(t, err)
		out[i] = e
	}
	return out
}

type logBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *logBuf) Write(p []byte) (int, error) { l.mu.Lock(); defer l.mu.Unlock(); return l.b.Write(p) }
func (l *logBuf) String() string              { l.mu.Lock(); defer l.mu.Unlock(); return l.b.String() }

func captureLog(t testing.TB) *logBuf {
	lb := &logBuf{}
	golly.DefaultLogger().SetOutput(lb)
	t.Cleanup(func() { golly.DefaultLogger().SetOutput(io.Discard) })
	return lb
}

// ---- Execute: order, versions, stamping ----

func TestExecute_PersistsOrderedStampedEvents(t *testing.T) {
	SetUserInfoFunc(func(context.Context) UserInfo { return UserInfo{UserID: "u1", TenantID: "t1"} })
	t.Cleanup(func() { SetUserInfoFunc(nil) })

	eng, _ := spyEngine(t)
	ctx := context.Background()

	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))
	b := &benchAgg{ID: a.ID}
	require.NoError(t, eng.Execute(ctx, b, benchCmd{n: 5}))

	evs := storedEvents(t, eng, a.ID)
	require.Len(t, evs, 4)

	wantTypes := []string{typCreated, typBumped, typRenamed, typBumped}
	for i, e := range evs {
		assert.Equal(t, wantTypes[i], e.Type, "type[%d]", i)
		assert.Equal(t, int64(i+1), e.Version, "version[%d]", i)
		assert.Equal(t, int64(i+1), e.GlobalVersion, "global[%d]", i)
		assert.Equal(t, a.ID, e.AggregateID)
		assert.Equal(t, "eventsource.benchAgg", e.AggregateType)
		assert.Equal(t, "events.benchagg", e.Topic)
		assert.Equal(t, "u1", e.UserID)
		assert.Equal(t, "t1", e.TenantID)
		assert.Equal(t, EventKindEvent, e.Kind)
		assert.Empty(t, e.State, "state is transient")
	}
	assert.Equal(t, benchBumped{By: 5}, evs[3].Data)

	assert.Equal(t, 6, b.Count)
	assert.Equal(t, "x", b.Name)
	assert.Equal(t, int64(4), b.Version())
	assert.Empty(t, b.Changes().Uncommitted(), "nothing left uncommitted")
}

func TestExecute_WithoutUserInfoFuncLeavesEnvelopeEmpty(t *testing.T) {
	SetUserInfoFunc(nil)
	eng, _ := spyEngine(t)
	a := &benchAgg{}
	require.NoError(t, eng.Execute(context.Background(), a, benchCreateCmd{}))
	for _, e := range storedEvents(t, eng, a.ID) {
		assert.Empty(t, e.UserID)
		assert.Empty(t, e.TenantID)
	}
}

func TestExecute_CommandErrorPersistsNothing(t *testing.T) {
	eng, st := spyEngine(t)
	boom := errors.New("boom")
	a := &benchAgg{ID: "x"}
	err := eng.Execute(context.Background(), a, failingCmd{err: boom})
	require.ErrorIs(t, err, boom)
	assert.Zero(t, st.saveCalls.Load())
	for _, e := range a.Changes() {
		assert.Equal(t, EventStateFailed, e.State)
	}
}

type failingCmd struct{ err error }

func (c failingCmd) Perform(_ context.Context, agg Aggregate) error {
	agg.Record(benchBumped{By: 1})
	return c.err
}

type countingCmd struct{ performs atomic.Int64 }

func (c *countingCmd) Perform(_ context.Context, agg Aggregate) error {
	c.performs.Add(1)
	agg.Record(benchRenamed{Name: "r"}, benchBumped{By: 7})
	return nil
}

func TestExecute_ConflictRetryRunsPerformOnceAndKeepsEventIDs(t *testing.T) {
	eng, st := spyEngine(t)
	ctx := context.Background()
	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))

	st.saveCalls.Store(0)
	st.failSaves.Store(2) // two conflicts, third save lands
	cmd := &countingCmd{}
	b := &benchAgg{ID: a.ID}
	require.NoError(t, eng.Execute(ctx, b, cmd))

	assert.EqualValues(t, 1, cmd.performs.Load(), "Perform must not re-run on conflict")
	assert.EqualValues(t, 3, st.saveCalls.Load())

	evs := storedEvents(t, eng, a.ID)
	require.Len(t, evs, 4, "conflicted saves persist nothing")
	assert.Equal(t, typRenamed, evs[2].Type)
	assert.Equal(t, typBumped, evs[3].Type)
	assert.Less(t, evs[1].Version, evs[2].Version)
	assert.Equal(t, evs[2].Version+1, evs[3].Version, "pending events stay consecutive")
	assert.Equal(t, 8, b.Count)
}

func TestExecute_ConcurrentSameAggregateLosesNothing(t *testing.T) {
	eng, _ := spyEngine(t)
	ctx := context.Background()
	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))

	const g = 16
	var wg sync.WaitGroup
	errs := make(chan error, g)
	for i := 0; i < g; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- eng.Execute(ctx, &benchAgg{ID: a.ID}, benchCmd{n: 1})
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	evs := storedEvents(t, eng, a.ID)
	assert.Len(t, evs, 2+2*g)

	seen := map[int64]bool{}
	ids := map[uuid.UUID]bool{}
	for _, e := range evs {
		assert.False(t, seen[e.GlobalVersion], "global version %d reused", e.GlobalVersion)
		seen[e.GlobalVersion] = true
		ids[e.ID] = true
	}
	assert.Len(t, ids, 2+2*g, "event ids unique")
}

// ---- Load / replay ----

func TestLoad_ReplaysManyEventsThroughSnapshots(t *testing.T) {
	eng, st := spyEngine(t)
	ctx := context.Background()

	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))
	for i := 0; i < 124; i++ {
		require.NoError(t, eng.Execute(ctx, a, benchCmd{n: 1}))
	}
	require.Equal(t, int64(250), a.Version())

	var snaps []int64
	all, err := eng.Store().LoadEvents(ctx)
	require.NoError(t, err)
	for _, p := range all {
		if e := p.(InMemoryEvent).Event; e.Kind == EventKindSnapshot {
			snaps = append(snaps, e.Version)
		}
	}
	assert.Equal(t, []int64{100, 200}, snaps)

	st.loaded.Store(0)
	fresh := &benchAgg{ID: a.ID}
	require.NoError(t, eng.Load(ctx, fresh))

	assert.Equal(t, 125, fresh.Count)
	assert.Equal(t, "x", fresh.Name)
	assert.Equal(t, int64(250), fresh.Version())
	assert.EqualValues(t, 50, st.loaded.Load(), "only events after the v200 snapshot are read")
	assert.Len(t, fresh.Changes(), 50)
	assert.Empty(t, fresh.Changes().Uncommitted(), "replayed events are completed, not re-saveable")
	for i, e := range fresh.Changes() {
		assert.Equal(t, int64(201+i), e.Version)
		assert.Equal(t, EventStateCompleted, e.State)
	}
}

func TestLoad_ReplayWithoutSnapshotAppliesInVersionOrder(t *testing.T) {
	eng, _ := spyEngine(t)
	ctx := context.Background()
	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))
	for i := 1; i <= 7; i++ {
		require.NoError(t, eng.Execute(ctx, a, benchCmd{n: i}))
	}

	fresh := &benchAgg{ID: a.ID}
	require.NoError(t, eng.Load(ctx, fresh))
	assert.Equal(t, 1+28, fresh.Count)
	assert.Equal(t, int64(16), fresh.Version())
	require.Len(t, fresh.Changes(), 16)
	for i, e := range fresh.Changes() {
		assert.Equal(t, int64(i+1), e.Version)
	}
}

func TestReplay_EmptyAndInvalidIDsDoNotTouchTheStore(t *testing.T) {
	eng, st := spyEngine(t)
	ctx := context.Background()

	for _, id := range []string{"", uuid.Nil.String(), "0"} {
		a := &benchAgg{ID: id}
		require.NoError(t, eng.Replay(ctx, a))
		assert.Zero(t, a.Version())
		assert.Empty(t, a.Changes())
	}
	assert.Zero(t, st.batchCalls.Load())

	unknown := &benchAgg{ID: "does-not-exist"}
	require.NoError(t, eng.Replay(ctx, unknown))
	assert.Zero(t, unknown.Version())
	assert.Empty(t, unknown.Name)
	assert.Empty(t, unknown.Changes())
}

type benchChecked struct{ benchAgg }

func (b *benchChecked) IsNewRecord() bool { return b.Version() == 0 }

func TestLoad_NewRecordCheckerReportsNoEvents(t *testing.T) {
	eng, _ := spyEngine(t)
	require.NoError(t, eng.RegisterAggregate(&benchChecked{}, nil))
	err := eng.Load(context.Background(), &benchChecked{benchAgg{ID: "ghost"}})
	assert.ErrorIs(t, err, ErrorNoEventsFound)
}

func TestReplay_PendingChangesShortCircuit(t *testing.T) {
	eng, st := spyEngine(t)
	ctx := context.Background()
	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))

	st.batchCalls.Store(0)
	p := &benchAgg{ID: a.ID}
	p.Record(benchBumped{By: 9})
	require.NoError(t, eng.Replay(ctx, p))
	assert.Zero(t, st.batchCalls.Load())
	assert.Zero(t, p.Count, "replay must not apply store events over pending changes")
	assert.Len(t, p.Changes(), 1)
}

func TestReplay_HydrateErrorSurfaces(t *testing.T) {
	st := NewInMemoryStore(Event{ID: uuid.New(), Type: "eventsource.unregistered", AggregateType: "eventsource.benchAgg", AggregateID: "z", Version: 1, GlobalVersion: 1, Kind: EventKindEvent})
	eng := NewEngine(WithStore(&hydratingStore{st}))
	require.NoError(t, eng.RegisterAggregate(&benchAgg{}, []any{benchCreated{}}))
	err := eng.Replay(context.Background(), &benchAgg{ID: "z"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no codec found")
}

// hydratingStore serves events through the codec path instead of the in-memory passthrough.
type hydratingStore struct{ *InMemoryStore }

type codecEvent struct{ Event }

func (c codecEvent) Hydrate(eng *Engine) (Event, error) {
	e := c.Event
	return e, e.Hydrate(eng, []byte(`{}`), nil)
}

func (s *hydratingStore) LoadEventsInBatches(ctx context.Context, n int, h func([]PersistedEvent) error, f ...EventFilter) error {
	return s.InMemoryStore.LoadEventsInBatches(ctx, n, func(p []PersistedEvent) error {
		out := make([]PersistedEvent, len(p))
		for i := range p {
			out[i] = codecEvent{p[i].(InMemoryEvent).Event}
		}
		return h(out)
	}, f...)
}

// ---- handler dispatch ----

type multiAgg struct {
	AggregateBase
	ID   string
	Log  []string
	Skip int
}

func (a *multiAgg) GetID() string           { return a.ID }
func (a *multiAgg) AlphaHandler(Event)      { a.Log = append(a.Log, "alpha") }
func (a *multiAgg) BetaHandler(Event)       { a.Log = append(a.Log, "beta") }
func (a *multiAgg) GammaHandler(e Event)    { a.Log = append(a.Log, fmt.Sprintf("gamma:%d", e.Version)) }
func (a *multiAgg) BadHandler(Event) error  { a.Skip++; return nil }
func (a *multiAgg) NotAHandler(Event)       { a.Skip++ }
func (a *multiAgg) WrongArgsHandler(string) {}
func (a *multiAgg) unexportedHandler(Event) {}

type Alpha struct{}
type Beta struct{}
type Gamma struct{}
type Delta struct{}
type Bad struct{}

func TestApply_RoutesEachTypeToItsOwnHandler(t *testing.T) {
	a := &multiAgg{ID: "m"}
	evs := []Event{
		{Type: "pkg.Beta", Data: Beta{}, Version: 1},
		{Type: "pkg/p.Alpha", Data: Alpha{}, Version: 2},
		{Type: "Gamma", Data: Gamma{}, Version: 3},
		{Type: "pkg.alpha", Data: Alpha{}, Version: 4},
		{Type: "pkg.Delta", Data: Delta{}, Version: 5}, // no handler
		{Type: "pkg.Bad", Data: Bad{}, Version: 6},     // wrong return signature: not indexed
		{Data: Gamma{}, Version: 7},                    // type derived from Data
	}
	for _, e := range evs {
		require.NoError(t, apply(a, e))
	}
	assert.Equal(t, []string{"beta", "alpha", "gamma:3", "alpha", "gamma:7"}, a.Log)
	assert.Zero(t, a.Skip)
	assert.Equal(t, int64(7), a.Version())
}

func TestApply_NilDataIsAnError(t *testing.T) {
	require.Error(t, apply(&multiAgg{}, Event{Type: "Alpha"}))
}

type applierAgg struct {
	AggregateBase
	got []string
}

func (a *applierAgg) GetID() string      { return "ap" }
func (a *applierAgg) Apply(e Event)      { a.got = append(a.got, e.Type) }
func (a *applierAgg) AlphaHandler(Event) { a.got = append(a.got, "HANDLER-SHOULD-NOT-RUN") }

func TestApply_CustomApplierTakesPrecedence(t *testing.T) {
	a := &applierAgg{}
	require.NoError(t, apply(a, Event{Type: "pkg.Alpha", Data: Alpha{}, Version: 3}))
	assert.Equal(t, []string{"pkg.Alpha"}, a.got)
	assert.Equal(t, int64(3), a.Version())
}

func TestApply_HandlerReceivesFullEventCopy(t *testing.T) {
	var got Event
	a := &captureAgg{sink: &got}
	want := Event{ID: uuid.New(), Type: "pkg.Alpha", Data: Alpha{}, Version: 9, AggregateID: "agg", TenantID: "t", UserID: "u", Topic: "tp", GlobalVersion: 4, Kind: EventKindEvent, State: EventStateReady}
	for i := 0; i < 3; i++ { // reuse of the pooled event box must not leak state between calls
		require.NoError(t, apply(a, want))
		assert.Equal(t, want, got)
		got = Event{}
		other := Event{Type: "pkg.Alpha", Data: Alpha{}, Version: 1}
		require.NoError(t, apply(a, other))
		assert.Equal(t, other, got)
	}
}

type captureAgg struct {
	AggregateBase
	sink *Event
}

func (a *captureAgg) GetID() string        { return "c" }
func (a *captureAgg) AlphaHandler(e Event) { *a.sink = e }

func TestHandlerEntry_PerTypeTopicCaching(t *testing.T) {
	e := handlerCache.entry(&benchAgg{})
	assert.False(t, e.dynTopic)
	assert.Equal(t, "events.benchagg", e.topicFor(&benchAgg{}))

	d := handlerCache.entry(&perfTopicAgg{})
	assert.True(t, d.dynTopic)
	assert.Equal(t, "events.one", d.topicFor(&perfTopicAgg{top: "One"}))
	assert.Equal(t, "events.two-x", d.topicFor(&perfTopicAgg{top: "Two.X"}), "dynamic topics are never cached per type")
}

// ---- Record / ProcessChanges ----

func TestRecord_BatchesAppendAfterExistingChangesWithConsecutiveVersions(t *testing.T) {
	a := &benchAgg{ID: "r"}
	a.SetVersion(10)
	a.Record(benchRenamed{Name: "a"})
	a.Record(benchBumped{By: 1}, benchBumped{By: 2}, benchBumped{By: 3})
	a.Record()
	a.Record(benchRenamed{Name: "b"})

	ch := a.Changes()
	require.Len(t, ch, 5)
	wantTypes := []string{typRenamed, typBumped, typBumped, typBumped, typRenamed}
	for i, e := range ch {
		assert.Equal(t, int64(11+i), e.Version)
		assert.Equal(t, wantTypes[i], e.Type)
		assert.Equal(t, EventStateReady, e.State)
	}
	assert.Equal(t, benchBumped{By: 3}, ch[3].Data)
}

func TestProcessChanges_AppliesInOrderAndSkipsAlreadyApplied(t *testing.T) {
	a := &multiAgg{ID: "p"}
	a.Record(Beta{}, Alpha{})
	require.NoError(t, a.ProcessChanges(context.Background(), a))
	require.Equal(t, []string{"beta", "alpha"}, a.Log)

	a.Record(Gamma{})
	require.NoError(t, a.ProcessChanges(context.Background(), a))
	assert.Equal(t, []string{"beta", "alpha", "gamma:3"}, a.Log, "applied events are not re-applied")
	for _, e := range a.Changes() {
		assert.Equal(t, EventStateApplied, e.State)
		assert.Equal(t, "events.multiagg", e.Topic)
	}
}

func TestProcessChanges_SnapshotAppendedOnBoundary(t *testing.T) {
	a := &benchAgg{ID: "s"}
	a.SetVersion(99)
	a.Record(benchBumped{By: 1})
	require.NoError(t, a.ProcessChanges(context.Background(), a))
	ch := a.Changes()
	require.Len(t, ch, 2)
	assert.Equal(t, EventKindSnapshot, ch[1].Kind)
	assert.Equal(t, int64(100), ch[1].Version)
	assert.Equal(t, "s", ch[1].AggregateID)
}

// ---- Events helpers ----

func TestEventsByState_PreservesOrderAndMixedStates(t *testing.T) {
	evts := Events{
		{Version: 1, State: EventStateReady}, {Version: 2, State: EventStateApplied},
		{Version: 3, State: EventStateReady}, {Version: 4, State: EventStateCompleted},
		{Version: 5, State: EventStateReady},
	}
	ready := evts.Ready()
	require.Len(t, ready, 3)
	assert.Equal(t, 3, cap(ready))
	assert.Equal(t, []int64{1, 3, 5}, []int64{ready[0].Version, ready[1].Version, ready[2].Version})
	assert.Len(t, evts.Completed(), 1)
	assert.Empty(t, Events(nil).Ready())
}

// ---- userinfo ----

func TestUserInfoFunc_SwapIsNeverTorn(t *testing.T) {
	pair := func(n string) func(context.Context) UserInfo {
		return func(context.Context) UserInfo { return UserInfo{UserID: "u-" + n, TenantID: "t-" + n} }
	}
	SetUserInfoFunc(pair("a"))
	t.Cleanup(func() { SetUserInfoFunc(nil) })

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				var e Event
				applyUserInfo(context.Background(), &e)
				if e.UserID == "" {
					continue // momentarily nil
				}
				if e.UserID[2:] != e.TenantID[2:] {
					t.Errorf("torn read: %s/%s", e.UserID, e.TenantID)
					return
				}
			}
		}()
	}
	for i := 0; i < 2000; i++ {
		SetUserInfoFunc(pair([]string{"a", "b", "c"}[i%3]))
		if i%7 == 0 {
			SetUserInfoFunc(nil)
		}
	}
	close(stop)
	wg.Wait()

	SetUserInfoFunc(pair("final"))
	var e Event
	applyUserInfo(context.Background(), &e)
	assert.Equal(t, "u-final", e.UserID)
	assert.Equal(t, "t-final", e.TenantID)
	SetUserInfoFunc(nil)
	e = Event{UserID: "keep"}
	applyUserInfo(context.Background(), &e)
	assert.Equal(t, "keep", e.UserID, "nil func leaves event untouched")
}

// ---- engine flag ----

func TestEngine_RunningFlagTransitions(t *testing.T) {
	eng := NewEngine(WithStore(NewInMemoryStore()))
	assert.False(t, eng.IsRunning())

	eng.Start()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); assert.True(t, eng.IsRunning()) }()
	}
	wg.Wait()

	eng.Stop()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); assert.False(t, eng.IsRunning()) }()
	}
	wg.Wait()
}

// ---- registry: copy-on-write ----

type regAggA struct{ AggregateBase }
type regAggB struct{ AggregateBase }
type regAggC struct{ AggregateBase }
type regAggD struct{ AggregateBase }

func (regAggA) GetID() string { return "" }
func (regAggB) GetID() string { return "" }
func (regAggC) GetID() string { return "" }
func (regAggD) GetID() string { return "" }

type regE0 struct{ V int }
type regE1 struct{ V int }
type regE2 struct{ V int }
type regE3 struct{ V int }
type regE4 struct{ V int }
type regE5 struct{ V int }

func TestAggregateRegistry_ConcurrentRegistersLoseNoUpdates(t *testing.T) {
	aggs := []Aggregate{&regAggA{}, &regAggB{}, &regAggC{}, &regAggD{}}
	evs := []any{regE0{}, regE1{}, regE2{}, regE3{}, regE4{}, regE5{}}

	r := NewAggregateRegistry()
	var wg sync.WaitGroup
	start := make(chan struct{})
	for g := 0; g < 48; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r.Register(aggs[g%4], []any{evs[g%6]})
		}()
	}
	// concurrent readers must always see a coherent item (codec present or not, never a panic)
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 500; i++ {
				_ = r.GetEventCodec("eventsource.regAggA", "eventsource.regE0")
				for item := range r.List() {
					_ = len(item.Events)
				}
			}
		}()
	}
	close(start)
	wg.Wait()

	want := map[string]map[string]bool{}
	for g := 0; g < 48; g++ {
		an := fmt.Sprintf("eventsource.regAgg%c", 'A'+g%4)
		if want[an] == nil {
			want[an] = map[string]bool{}
		}
		want[an][fmt.Sprintf("eventsource.regE%d", g%6)] = true
	}
	for an, es := range want {
		for en := range es {
			c := r.GetEventCodec(an, en)
			require.NotNil(t, c, "lost update: %s/%s", an, en)
			v, err := c.UnmarshalFn([]byte(`{"V":42}`))
			require.NoError(t, err)
			assert.NotNil(t, v)
		}
		_, ok := r.GetAggregate(an)
		assert.True(t, ok, an)
	}
	n := 0
	for item := range r.List() {
		n += len(item.Events)
	}
	total := 0
	for _, es := range want {
		total += len(es)
	}
	assert.Equal(t, total, n)
}

func TestAggregateRegistry_ReRegisterKeepsOldCodecsAndOldSnapshotsStayImmutable(t *testing.T) {
	r := NewAggregateRegistry()
	r.Register(&regAggA{}, []any{regE0{}})

	var before *AggregateRegistryItem
	for it := range r.List() {
		before = it
	}
	r.Register(&regAggA{}, []any{regE1{}})

	assert.Len(t, before.Events, 1, "published item must not be mutated by a later Register")
	assert.NotNil(t, r.GetEventCodec("eventsource.regAggA", "eventsource.regE0"))
	assert.NotNil(t, r.GetEventCodec("eventsource.regAggA", "eventsource.regE1"))

	v, err := r.GetEventCodec("eventsource.regAggA", "eventsource.regE1").UnmarshalFn(`{"V":7}`)
	require.NoError(t, err)
	assert.Equal(t, regE1{V: 7}, v)
}

// ---- projection manager ----

type idProj struct {
	ProjectionBase
	id    string
	calls *atomic.Int64
}

func (p *idProj) ID() string { return p.id }
func (p *idProj) HandleEvent(context.Context, Event) error {
	p.calls.Add(1)
	return nil
}

func TestProjectionManager_ConcurrentRegisterAndDispatchLoseNothing(t *testing.T) {
	pm := NewProjectionManager()
	var calls atomic.Int64
	const n = 64

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			pm.Register(&idProj{id: fmt.Sprintf("p%d", i), calls: &calls})
		}()
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 300; i++ {
				pm.handleEvent(context.Background(), Event{Type: "x", Topic: "t"})
			}
		}()
	}
	close(start)
	wg.Wait()

	assert.Len(t, pm.index.Load().events[AllEvents], n)
	for i := 0; i < n; i++ {
		_, err := pm.Get(fmt.Sprintf("p%d", i))
		require.NoError(t, err)
	}

	calls.Store(0)
	pm.handleEvent(context.Background(), Event{Type: "x", Topic: "t"})
	assert.EqualValues(t, n, calls.Load(), "every registered projection receives the event exactly once")
}

type typedProj struct {
	ProjectionBase
	mu   sync.Mutex
	seen []string
}

func (p *typedProj) EventTypes() []any { return []any{benchBumped{}} }
func (p *typedProj) HandleEvent(_ context.Context, e Event) error {
	p.mu.Lock()
	p.seen = append(p.seen, fmt.Sprintf("%s@%d", e.Type, e.Version))
	p.mu.Unlock()
	return nil
}

func TestProjection_FiltersByEventTypeAndKeepsPerAggregateOrder(t *testing.T) {
	eng, _ := spyEngine(t)
	eng.Start()
	t.Cleanup(eng.Stop)

	all := &recProj{}
	typed := &typedProj{}
	require.NoError(t, eng.RegisterProjection(all))
	require.NoError(t, eng.RegisterProjection(typed))

	ctx := context.Background()
	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))
	for i := 0; i < 10; i++ {
		require.NoError(t, eng.Execute(ctx, a, benchCmd{n: i}))
	}
	eng.Projections().Wait()

	var wantAll, wantTyped []string
	wantAll = append(wantAll, typCreated+"@1", typBumped+"@2")
	wantTyped = append(wantTyped, typBumped+"@2")
	for i := 0; i < 10; i++ {
		v := 3 + 2*i
		wantAll = append(wantAll, fmt.Sprintf("%s@%d", typRenamed, v), fmt.Sprintf("%s@%d", typBumped, v+1))
		wantTyped = append(wantTyped, fmt.Sprintf("%s@%d", typBumped, v+1))
	}
	assert.Equal(t, wantAll, all.snapshot())
	typed.mu.Lock()
	assert.Equal(t, wantTyped, typed.seen)
	typed.mu.Unlock()
	assert.EqualValues(t, 22, all.Position(ctx), "position is the last global version")
}

type recProj struct {
	ProjectionBase
	mu   sync.Mutex
	seen []string
	// failAt, when >0, fails the HandleEvent call with that 1-based index
	failAt int
	n      int
}

var errProj = errors.New("projection boom")

func (p *recProj) HandleEvent(_ context.Context, e Event) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n++
	if p.failAt > 0 && p.n == p.failAt {
		return errProj
	}
	p.seen = append(p.seen, fmt.Sprintf("%s@%d", e.Type, e.Version))
	return nil
}

func (p *recProj) snapshot() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.seen...)
}

func TestProjection_ErrorMidBatchStopsAndKeepsPosition(t *testing.T) {
	eng, _ := spyEngine(t)
	ctx := context.Background()
	a := &benchAgg{}
	require.NoError(t, eng.Execute(ctx, a, benchCreateCmd{}))
	require.NoError(t, eng.Execute(ctx, a, benchCmd{n: 1}))
	require.NoError(t, eng.Execute(ctx, a, benchCmd{n: 2})) // 6 events

	p := &recProj{failAt: 3}
	require.NoError(t, eng.RegisterProjection(p))

	err := eng.RunProjectionToEnd(ctx, p)
	require.ErrorIs(t, err, errProj)
	assert.Equal(t, []string{typCreated + "@1", typBumped + "@2"}, p.snapshot(), "events before the failure were handled, none after")
	assert.Zero(t, p.Position(ctx), "position must not advance past a failed batch")

	p.failAt = 0
	require.NoError(t, eng.RunProjectionToEnd(ctx, p))
	assert.Len(t, p.snapshot(), 2+6, "re-run resumes from the unchanged position")
	assert.EqualValues(t, 6, p.Position(ctx))
}

func TestProjection_QueueFullDropsAndLogs(t *testing.T) {
	lb := captureLog(t)
	pm := NewProjectionManagerWithConfig(1, 1) // not started: nothing drains the 128-slot queue
	pm.running.Store(true)
	t.Cleanup(func() { pm.running.Store(false) })

	for i := 0; i < minProjectionWorkerBuffer+3; i++ {
		pm.dispatch(context.Background(), Event{AggregateID: "a"})
	}
	assert.Len(t, pm.workers[0], minProjectionWorkerBuffer)
	assert.Equal(t, 3, bytes.Count([]byte(lb.String()), []byte("projection queue full")))
}

func TestSend_NotRunningLogsOneWarningPerEventAndEnqueuesNothing(t *testing.T) {
	lb := captureLog(t)
	eng, _ := spyEngine(t)
	eng.Send(context.Background(), Event{AggregateID: "a"}, Event{AggregateID: "b"})
	assert.Equal(t, 2, bytes.Count([]byte(lb.String()), []byte("projection manager not running")))
	for _, w := range eng.projections.workers {
		assert.Zero(t, len(w))
	}
}

type failStream struct{ err error }

func (s failStream) Publish(context.Context, string, any) error { return s.err }

type orderStream struct {
	mu  sync.Mutex
	got []string
}

func (s *orderStream) Publish(_ context.Context, topic string, ev any) error {
	e := ev.(Event)
	s.mu.Lock()
	s.got = append(s.got, fmt.Sprintf("%s|%s|%d", topic, e.AggregateID, e.Version))
	s.mu.Unlock()
	return nil
}

func TestSend_PublishesEveryEventToEveryStreamInOrderAndLogsFailures(t *testing.T) {
	lb := captureLog(t)
	boom := errors.New("kafka down")
	o1, o2 := &orderStream{}, &orderStream{}
	eng, _ := spyEngine(t, WithStreams(failStream{boom}, o1, o2))

	evs := []Event{
		{AggregateType: "eventsource.benchAgg", AggregateID: "a", Version: 1, Type: "T1"},
		{Topic: "custom", AggregateID: "a", Version: 2, Type: "T2"},
		{AggregateID: "no-topic", Version: 3}, // no topic and no aggregate type: skipped
		{AggregateType: "eventsource.benchAgg", AggregateID: "b", Version: 4, Type: "T4"},
	}
	eng.Send(context.Background(), evs...)

	want := []string{"events.eventsource-benchagg|a|1", "custom|a|2", "events.eventsource-benchagg|b|4"}
	assert.Equal(t, want, o1.got)
	assert.Equal(t, want, o2.got)
	assert.Equal(t, 3, bytes.Count([]byte(lb.String()), []byte("stream publish failed")))
	assert.Contains(t, lb.String(), "kafka down")
	assert.Contains(t, lb.String(), "eventType=T2")
}

func TestStreamManager_PublishFansOutStreamMajorInOrder(t *testing.T) {
	sm := NewStreamManager()
	o := &orderStream{}
	sm.Add(o, o)
	sm.Publish(context.Background(), "tp", Event{AggregateID: "z", Version: 5}, Event{AggregateID: "z", Version: 6})
	assert.Equal(t, []string{"tp|z|5", "tp|z|6", "tp|z|5", "tp|z|6"}, o.got, "stream-major, as before")
}

func TestSend_ZeroEventsIsANoOp(t *testing.T) {
	lb := captureLog(t)
	eng, _ := spyEngine(t)
	eng.Send(context.Background())
	assert.Empty(t, lb.String())
}

// ---- internal stream: copy-on-write handlers ----

func TestInternalStream_ConcurrentSubscribeUnsubscribeExactSet(t *testing.T) {
	s := NewInternalStream("cow")
	var counts [8]atomic.Int64
	// distinct literals: Unsubscribe matches handlers by code pointer, so closures from one literal are indistinguishable
	handlers := []StreamHandler{
		func(context.Context, Event) { counts[0].Add(1) },
		func(context.Context, Event) { counts[1].Add(1) },
		func(context.Context, Event) { counts[2].Add(1) },
		func(context.Context, Event) { counts[3].Add(1) },
		func(context.Context, Event) { counts[4].Add(1) },
		func(context.Context, Event) { counts[5].Add(1) },
		func(context.Context, Event) { counts[6].Add(1) },
		func(context.Context, Event) { counts[7].Add(1) },
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range handlers {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; s.Subscribe("t", handlers[i]) }()
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 300; i++ {
				s.handleEvent(context.Background(), Event{Topic: "t"})
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Len(t, (*s.handlers.Load())["t"], len(handlers), "no Subscribe lost")

	for i := range counts {
		counts[i].Store(0)
	}
	var wg2 sync.WaitGroup
	for i := 0; i < len(handlers); i += 2 {
		wg2.Add(1)
		go func() { defer wg2.Done(); s.Unsubscribe("t", handlers[i]) }()
	}
	wg2.Wait()

	s.handleEvent(context.Background(), Event{Topic: "t"})
	for i := range counts {
		want := int64(1)
		if i%2 == 0 {
			want = 0
		}
		assert.Equal(t, want, counts[i].Load(), "handler %d", i)
	}
}

func TestInternalStream_TopicThenWildcardOrderAndPanicIsolated(t *testing.T) {
	lb := captureLog(t)
	s := NewInternalStream("o")
	var got []string
	s.Subscribe(AllEvents, func(context.Context, Event) { got = append(got, "all") })
	s.Subscribe("t", func(context.Context, Event) { got = append(got, "t1") })
	s.Subscribe("t", func(context.Context, Event) { panic("kaboom") })
	s.Subscribe("t", func(context.Context, Event) { got = append(got, "t3-never") })

	s.handleEvent(context.Background(), Event{Topic: "t"})
	assert.Equal(t, []string{"t1"}, got, "panic aborts the remaining handlers of this event, as before")
	assert.Contains(t, lb.String(), "kaboom")

	got = nil
	s2 := NewInternalStream("o2")
	s2.Subscribe(AllEvents, func(context.Context, Event) { got = append(got, "all") })
	s2.Subscribe("t", func(context.Context, Event) { got = append(got, "topic") })
	s2.handleEvent(context.Background(), Event{Topic: "t"})
	assert.Equal(t, []string{"topic", "all"}, got)
}

func TestInternalStream_StopIsIdempotentAndDrains(t *testing.T) {
	s := NewInternalStream("d")
	var n atomic.Int64
	s.Subscribe("t", func(context.Context, Event) { n.Add(1) })
	for i := 0; i < 10; i++ {
		require.NoError(t, s.Publish(context.Background(), "t", Event{Topic: "t"}))
	}
	s.Start()
	done := make(chan struct{})
	go func() { s.Stop(); s.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	assert.EqualValues(t, 10, n.Load())
}

// ---- in-memory store filters ----

func TestInMemoryFilters_EachDimension(t *testing.T) {
	t0 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(aid, at, typ, topic string, v, gv int64, k EventKind, dt time.Duration) Event {
		return Event{AggregateID: aid, AggregateType: at, Type: typ, Topic: topic, Version: v, GlobalVersion: gv, Kind: k, CreatedAt: t0.Add(dt)}
	}
	data := []Event{
		mk("a", "A", "E1", "ta", 1, 1, EventKindEvent, 0),
		mk("a", "A", "E2", "ta", 2, 2, EventKindEvent, time.Hour),
		mk("a", "A", "E2", "ta", 2, 0, EventKindSnapshot, 2*time.Hour),
		mk("b", "B", "E1", "tb", 1, 3, EventKindEvent, 3*time.Hour),
		mk("c", "C", "E3", "tc", 5, 4, EventKindEvent, 4*time.Hour),
	}
	ids := func(f EventFilter) []string {
		out := applyInMemoryFilters(data, f)
		r := make([]string, len(out))
		for i, e := range out {
			r[i] = fmt.Sprintf("%s%d/%d", e.AggregateID, e.Version, e.GlobalVersion)
		}
		return r
	}

	cases := []struct {
		name string
		f    EventFilter
		want []string
	}{
		{"none", EventFilter{}, []string{"a1/1", "a2/2", "a2/0", "b1/3", "c5/4"}},
		{"agg id", EventFilter{AggregateID: "a"}, []string{"a1/1", "a2/2", "a2/0"}},
		{"agg type", EventFilter{AggregateType: "B"}, []string{"b1/3"}},
		{"agg types", EventFilter{AggregateTypes: []string{"B", "C"}}, []string{"b1/3", "c5/4"}},
		{"event types", EventFilter{EventType: []string{"E2", "E3"}}, []string{"a2/2", "a2/0", "c5/4"}},
		{"topics", EventFilter{Topics: []string{"tb", "tc"}}, []string{"b1/3", "c5/4"}},
		{"from version", EventFilter{FromVersion: 2}, []string{"a2/2", "a2/0", "c5/4"}},
		{"to version", EventFilter{ToVersion: 1}, []string{"a1/1", "b1/3"}},
		{"from global", EventFilter{FromGlobalVersion: 3}, []string{"b1/3", "c5/4"}},
		{"to global", EventFilter{ToGlobalVersion: 2}, []string{"a1/1", "a2/2", "a2/0"}},
		{"from time", EventFilter{FromTime: t0.Add(3 * time.Hour)}, []string{"b1/3", "c5/4"}},
		{"to time", EventFilter{ToTime: t0.Add(30 * time.Minute)}, []string{"a1/1"}},
		{"exclude snapshot", EventFilter{ExcludeKinds: []EventKind{EventKindSnapshot}}, []string{"a1/1", "a2/2", "b1/3", "c5/4"}},
		{"limit", EventFilter{Limit: 2}, []string{"a1/1", "a2/2"}},
		{"limit after filter", EventFilter{AggregateID: "a", ExcludeKinds: []EventKind{EventKindSnapshot}, Limit: 1}, []string{"a1/1"}},
		{"order by version then global", EventFilter{AggregateID: "a", OrderByVersion: true}, []string{"a1/1", "a2/0", "a2/2"}},
		{"no match", EventFilter{AggregateID: "zzz"}, []string{}},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, ids(c.f), c.name)
	}
}

func TestInMemoryStore_LoadSnapshotMissingAndHighestWins(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	p, err := s.LoadSnapshot(ctx, "A", "a")
	assert.Nil(t, p)
	require.Error(t, err)
	assert.Equal(t, "record not found", err.Error())

	for _, v := range []int64{100, 300, 200} {
		require.NoError(t, s.SaveSnapshot(ctx, Event{Kind: EventKindSnapshot, AggregateType: "A", AggregateID: "a", Version: v}))
	}
	p, err = s.LoadSnapshot(ctx, "A", "a")
	require.NoError(t, err)
	assert.EqualValues(t, 300, p.(InMemoryEvent).Version)
}

// ---- projection defaults ----

func TestProjectionWorkerIndex_StableAndInRange(t *testing.T) {
	seen := map[int]bool{}
	for i := 0; i < 2000; i++ {
		id := uuid.NewString()
		idx := aggregateWorkerIndex(id, 8)
		require.GreaterOrEqual(t, idx, 0)
		require.Less(t, idx, 8)
		assert.Equal(t, idx, aggregateWorkerIndex(id, 8))
		seen[idx] = true
	}
	assert.Len(t, seen, 8, "hash spreads across all workers")
}
