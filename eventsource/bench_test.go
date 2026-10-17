package eventsource

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"
)

type benchCreated struct{ Name string }
type benchRenamed struct{ Name string }
type benchBumped struct{ By int }

type benchAgg struct {
	AggregateBase
	ID    string
	Name  string
	Count int
}

func (a *benchAgg) GetID() string { return a.ID }

func (a *benchAgg) BenchCreatedHandler(e Event) {
	d := e.Data.(benchCreated)
	a.Name = d.Name
}

func (a *benchAgg) BenchRenamedHandler(e Event) {
	d := e.Data.(benchRenamed)
	a.Name = d.Name
}

func (a *benchAgg) BenchBumpedHandler(e Event) {
	d := e.Data.(benchBumped)
	a.Count += d.By
}

type benchCmd struct{ n int }

func (c benchCmd) Validate(context.Context, Aggregate) error { return nil }

func (c benchCmd) Perform(_ context.Context, agg Aggregate) error {
	agg.Record(benchRenamed{Name: "x"}, benchBumped{By: c.n})
	return nil
}

type benchCreateCmd struct{}

func (benchCreateCmd) Perform(_ context.Context, agg Aggregate) error {
	a := agg.(*benchAgg)
	a.ID = uuid.NewString()
	agg.Record(benchCreated{Name: "n"}, benchBumped{By: 1})
	return nil
}

type benchProj struct {
	ProjectionBase
	n int
}

func (p *benchProj) HandleEvent(context.Context, Event) error { p.n++; return nil }

type benchStream struct{ n int }

func (s *benchStream) Publish(context.Context, string, any) error { s.n++; return nil }

func benchEngine(opts ...Option) *Engine {
	eng := NewEngine(append([]Option{WithStore(NewInMemoryStore())}, opts...)...)
	_ = eng.RegisterAggregate(&benchAgg{}, []any{benchCreated{}, benchRenamed{}, benchBumped{}})
	return eng
}

// startedBenchEngine runs the projection workers so Send doesn't take the not-running warn path.
func startedBenchEngine(b testing.TB, opts ...Option) *Engine {
	eng := benchEngine(opts...)
	eng.Start()
	b.Cleanup(eng.Stop)
	return eng
}

func seedBenchAgg(b testing.TB, eng *Engine, events int) string {
	b.Helper()
	ctx := context.Background()
	agg := &benchAgg{}
	if err := eng.Execute(ctx, agg, benchCreateCmd{}); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < events/2; i++ {
		if err := eng.Execute(ctx, agg, benchCmd{n: i}); err != nil {
			b.Fatal(err)
		}
	}
	return agg.ID
}

func BenchmarkExecuteNew(b *testing.B) {
	eng := startedBenchEngine(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := &benchAgg{}
		if err := eng.Execute(ctx, agg, benchCreateCmd{}); err != nil {
			b.Fatal(err)
		}
		if i%32 == 31 {
			b.StopTimer()
			eng.Projections().Wait()
			b.StartTimer()
		}
	}
}

func BenchmarkExecuteExisting(b *testing.B) {
	ctx := context.Background()
	eng := startedBenchEngine(b)
	id := seedBenchAgg(b, eng, 10)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%8 == 7 { // keep replay length and store size bounded
			b.StopTimer()
			eng.Projections().Wait()
			eng.Stop()
			eng = startedBenchEngine(b)
			id = seedBenchAgg(b, eng, 10)
			b.StartTimer()
		}
		agg := &benchAgg{ID: id}
		if err := eng.Execute(ctx, agg, benchCmd{n: i}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkExecuteWithUserInfoAndStream(b *testing.B) {
	SetUserInfoFunc(func(context.Context) UserInfo { return UserInfo{UserID: "u", TenantID: "t"} })
	b.Cleanup(func() { SetUserInfoFunc(nil) })

	eng := benchEngine(WithStreams(&benchStream{}))
	eng.Start()
	b.Cleanup(eng.Stop)
	if err := eng.RegisterProjection(&benchProj{}); err != nil {
		b.Fatal(err)
	}

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		agg := &benchAgg{}
		if err := eng.Execute(ctx, agg, benchCreateCmd{}); err != nil {
			b.Fatal(err)
		}
		if i%32 == 31 {
			b.StopTimer()
			eng.Projections().Wait()
			b.StartTimer()
		}
	}
}

func BenchmarkRecordAndProcess(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		agg := &benchAgg{ID: "agg"}
		agg.Record(benchRenamed{Name: "x"}, benchBumped{By: 1}, benchBumped{By: 2})
		if err := agg.ProcessChanges(ctx, agg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLoad(b *testing.B) {
	for _, n := range []int{10, 50, 100} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			eng := benchEngine()
			ctx := context.Background()
			id := seedBenchAgg(b, eng, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				agg := &benchAgg{ID: id}
				if err := eng.Load(ctx, agg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkApply(b *testing.B) {
	agg := &benchAgg{ID: "agg"}
	evt := NewEvent(benchBumped{By: 1}, EventStateReady)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := apply(agg, evt); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkProjectionDispatch(b *testing.B) {
	eng := benchEngine()
	eng.Start()
	b.Cleanup(eng.Stop)
	if err := eng.RegisterProjection(&benchProj{}); err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	evt := NewEvent(benchBumped{By: 1}, EventStateApplied)
	evt.AggregateID = "agg"
	evt.AggregateType = "eventsource.benchAgg"
	evt.Topic = "events.benchagg"

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		eng.projections.dispatch(ctx, evt)
		if i%64 == 63 {
			b.StopTimer()
			eng.Projections().Wait()
			b.StartTimer()
		}
	}
}

func BenchmarkProjectionHandleEvent(b *testing.B) {
	pm := NewProjectionManager()
	pm.Register(&benchProj{})
	ctx := context.Background()
	evt := NewEvent(benchBumped{By: 1}, EventStateApplied)
	evt.Topic = "events.benchagg"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pm.handleEvent(ctx, evt)
	}
}

func BenchmarkStreamPublish(b *testing.B) {
	sm := NewStreamManager()
	sm.Add(&benchStream{})
	ctx := context.Background()
	evt := NewEvent(benchBumped{By: 1}, EventStateApplied)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sm.Publish(ctx, "events.benchagg", evt)
	}
}

func BenchmarkInternalStreamHandleEvent(b *testing.B) {
	s := NewInternalStream("bench")
	s.Subscribe("events.benchagg", func(context.Context, Event) {})
	ctx := context.Background()
	evt := NewEvent(benchBumped{By: 1}, EventStateApplied)
	evt.Topic = "events.benchagg"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.handleEvent(ctx, evt)
	}
}
