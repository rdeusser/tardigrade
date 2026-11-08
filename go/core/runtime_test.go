package core_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/reactive"
)

type Incremented struct{}
type Notified struct {
	Value int `json:"value"`
}
type Poisoned struct{}

func (Incremented) EventType() string { return "test.Incremented" }
func (Notified) EventType() string    { return "test.Notified" }
func (Poisoned) EventType() string    { return "test.Poisoned" }

type counterState struct {
	N  int   `json:"n"`
	At int64 `json:"at"`
}

var count = core.DurableAtom("test.count", counterState{},
	core.On(func(s counterState, _ Incremented, m core.Meta) counterState {
		return counterState{N: s.N + 1, At: m.Position}
	}),
	core.On(func(s counterState, _ Poisoned, _ core.Meta) counterState { panic("poisoned event") }),
)

var notified = core.DurableAtom("test.notified", []int(nil),
	core.On(func(s []int, e Notified, _ core.Meta) []int { return append(slices.Clone(s), e.Value) }),
)

var Notify = core.NewAct[int, string]("test.notify")

var root = core.EffectAtom(func(g reactive.Getter) core.Output[int] {
	state := core.Get(g, count)
	out := core.Output[int]{View: state.N}
	if state.N > 0 && state.N%2 == 0 && !slices.Contains(core.Get(g, notified), state.N) {
		n := state.N
		out.Acts = map[string]core.Proposal{"notify": Notify.Request(core.Request[int, string]{
			Origin: core.At(state.At), Input: n,
			OnSettled: func(o core.Outcome[string], _ core.Ref) []core.Event {
				if o.Reason != nil {
					return []core.Event{Notified{Value: -n}}
				}
				return []core.Event{Notified{Value: n}}
			},
		})}
	}
	return out
})

var counter = core.DefineActor("counter", root, map[string]core.Method{
	"increment": core.NewMethod(core.MethodSpec[struct{}, int]{
		OnReceive: func(struct{}, core.MethodContext) core.Event { return Incremented{} },
		Result: func(_ struct{}, g reactive.Getter, _ core.MethodContext) *core.MethodResult[int] {
			return core.Completed(core.Get(g, count).N)
		},
	}),
})

var external = core.Sender{Kind: "external", ID: "test"}

func open(t *testing.T, journal core.Journal, impl core.Implementation, policy core.Policy) *core.Runtime {
	t.Helper()
	rt, err := core.Open(core.Options{Actor: counter, Journal: journal, Acts: []core.Implementation{impl}, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

func wait(t *testing.T, rt *core.Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, rt *core.Runtime, check func() bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for !check() {
		select {
		case <-rt.Changed():
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("condition not reached")
		}
	}
}

func increment(t *testing.T, rt *core.Runtime, id string) {
	t.Helper()
	if err := rt.Invoke("increment", id, external, []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func types(t *testing.T, journal core.Journal) []string {
	t.Helper()
	length, _ := journal.Length()
	records, err := journal.Read(0, length)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(records))
	for i, r := range records {
		names[i] = r.Event.EventType()
	}
	return names
}

var immediate = Notify.Implement(func(_ *core.Execution, n int) (core.Result[string], error) {
	return core.Value(fmt.Sprint(n)), nil
}, nil)

func TestCounterNotifiesThroughAct(t *testing.T) {
	journal := core.NewMemoryJournal()
	rt := open(t, journal, immediate, core.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	if got := core.Read(rt, notified); !slices.Equal(got, []int{2}) {
		t.Fatalf("notified = %v", got)
	}
	state, err := rt.Call("increment", "b")
	if err != nil || state.Status != core.StatusCompleted || string(state.Output) != "2" {
		t.Fatalf("call state = %+v, %v", state, err)
	}
	want := []string{"test.Incremented", "test.Incremented", "EffectRequested", "EffectSettled", "test.Notified"}
	if got := types(t, journal); !slices.Equal(got, want) {
		t.Fatalf("journal = %v", got)
	}
}

func TestInvokeIsIdempotentPerID(t *testing.T) {
	journal := core.NewMemoryJournal()
	rt := open(t, journal, immediate, core.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "a")
	if n := core.Read(rt, count).N; n != 1 {
		t.Fatalf("count = %d", n)
	}
	if err := rt.Invoke("increment", "a", core.Sender{Kind: "external", ID: "other"}, []byte("{}")); !errors.Is(err, core.ErrMessageConflict) {
		t.Fatalf("conflict = %v", err)
	}
}

func TestReplayResumesPendingWorkOnce(t *testing.T) {
	journal := core.NewMemoryJournal()
	started := make(chan struct{})
	blocking := Notify.Implement(func(x *core.Execution, n int) (core.Result[string], error) {
		close(started)
		<-x.Context().Done()
		return core.Result[string]{}, x.Context().Err()
	}, nil)
	rt, err := core.Open(core.Options{Actor: counter, Journal: journal, Acts: []core.Implementation{blocking}})
	if err != nil {
		t.Fatal(err)
	}
	increment(t, rt, "a")
	increment(t, rt, "b")
	<-started
	_ = rt.Close()
	reopened := open(t, journal, immediate, core.Policy{})
	wait(t, reopened)
	if got := core.Read(reopened, notified); !slices.Equal(got, []int{2}) {
		t.Fatalf("notified = %v", got)
	}
	requested := 0
	for _, name := range types(t, journal) {
		if name == "EffectRequested" {
			requested++
		}
	}
	if requested != 1 {
		t.Fatalf("EffectRequested recorded %d times", requested)
	}
}

func TestRetryJournalsAttempts(t *testing.T) {
	journal := core.NewMemoryJournal()
	var attempts atomic.Int32
	flaky := Notify.Implement(func(x *core.Execution, n int) (core.Result[string], error) {
		value, err := core.Retry(x, core.RetryPolicy{Decide: func(err error, attempt int) *core.RetryDecision {
			return &core.RetryDecision{Delay: time.Millisecond, Reason: err.Error()}
		}}, func(context.Context) (string, error) {
			if attempts.Add(1) < 3 {
				return "", errors.New("transient")
			}
			return "ok", nil
		})
		return core.Value(value), err
	}, nil)
	rt := open(t, journal, flaky, core.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	retries := 0
	for _, name := range types(t, journal) {
		if name == "RetryScheduled" {
			retries++
		}
	}
	if retries != 2 || !slices.Equal(core.Read(rt, notified), []int{2}) {
		t.Fatalf("retries = %d notified = %v", retries, core.Read(rt, notified))
	}
}

func TestForkSettlesPromise(t *testing.T) {
	journal := core.NewMemoryJournal()
	forked := Notify.Implement(func(x *core.Execution, n int) (core.Result[string], error) {
		return core.Fork(x, core.PromiseOptions{}, func(context.Context) (string, error) { return "later", nil }), nil
	}, nil)
	rt := open(t, journal, forked, core.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	eventually(t, rt, func() bool { return len(core.Read(rt, notified)) == 1 })
	want := []string{"test.Incremented", "test.Incremented", "EffectRequested", "EffectSettled", "PromiseSettled", "test.Notified"}
	if got := types(t, journal); !slices.Equal(got, want) {
		t.Fatalf("journal = %v", got)
	}
}

func TestPromiseDeadlineRejects(t *testing.T) {
	journal := core.NewMemoryJournal()
	never := Notify.Implement(func(x *core.Execution, n int) (core.Result[string], error) {
		return core.Defer[string](core.Handle{Executor: "external", ID: "job"}, core.PromiseOptions{Timeout: 20 * time.Millisecond}), nil
	}, nil)
	rt, err := core.Open(core.Options{Actor: counter, Journal: journal, Acts: []core.Implementation{never}, Promises: ignoringPromises{}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	increment(t, rt, "a")
	increment(t, rt, "b")
	eventually(t, rt, func() bool { return len(core.Read(rt, notified)) == 1 })
	if got := core.Read(rt, notified); got[0] != -2 {
		t.Fatalf("notified = %v, want rejection", got)
	}
}

type ignoringPromises struct{}

func (ignoringPromises) Watch(core.PromiseWatch) error  { return nil }
func (ignoringPromises) Cancel(core.PromiseWatch) error { return nil }

func TestCancellationRunsCleanup(t *testing.T) {
	journal := core.NewMemoryJournal()
	started := make(chan struct{})
	var cleaned atomic.Bool
	blocking := Notify.Implement(func(x *core.Execution, n int) (core.Result[string], error) {
		close(started)
		<-x.Context().Done()
		return core.Result[string]{}, x.Context().Err()
	}, func(c core.Cleanup, n int) error {
		cleaned.Store(true)
		return nil
	})
	rt := open(t, journal, blocking, core.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	<-started
	if err := rt.Cancel(core.Ref{Seq: 2, Atom: "counter", Act: Notify.Name()}, "stop"); err != nil {
		t.Fatal(err)
	}
	wait(t, rt)
	if !cleaned.Load() {
		t.Fatal("cleanup did not run")
	}
	want := []string{"test.Incremented", "test.Incremented", "EffectRequested", "EffectCancelled", "test.Notified", "CancellationSettled"}
	if got := types(t, journal); !slices.Equal(got, want) {
		t.Fatalf("journal = %v", got)
	}
}

func TestPanicStopsActor(t *testing.T) {
	journal := core.NewMemoryJournal()
	broken := Notify.Implement(func(*core.Execution, int) (core.Result[string], error) { panic("boom") }, nil)
	rt := open(t, journal, broken, core.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	var stopped *core.StoppedError
	if err := rt.Invoke("increment", "c", external, []byte("{}")); !errors.As(err, &stopped) {
		t.Fatalf("invoke after defect = %v", err)
	}
	state, _ := rt.Call("increment", "b")
	if state.Status != core.StatusCompleted {
		t.Fatalf("settled call state = %+v", state)
	}
}

func TestRejectedEventLeavesStateUnchanged(t *testing.T) {
	journal := core.NewMemoryJournal()
	rt := open(t, journal, immediate, core.Policy{})
	increment(t, rt, "a")
	if err := rt.Send(Poisoned{}); err == nil {
		t.Fatal("poisoned event accepted")
	}
	if length, _ := journal.Length(); length != 1 || core.Read(rt, count).N != 1 {
		t.Fatalf("length = %d count = %d", length, core.Read(rt, count).N)
	}
	increment(t, rt, "b")
	wait(t, rt)
	if core.Read(rt, count).N != 2 {
		t.Fatal("runtime unusable after rejection")
	}
}

type countingJournal struct {
	*core.MemoryJournal
	reads atomic.Int64
}

func (j *countingJournal) Read(from, to int64) ([]core.Record, error) {
	j.reads.Add(to - from)
	return j.MemoryJournal.Read(from, to)
}

func TestCheckpointLimitsReplay(t *testing.T) {
	journal := &countingJournal{MemoryJournal: core.NewMemoryJournal()}
	rt := open(t, journal, immediate, core.Policy{CheckpointEvery: 2})
	for i := range 5 {
		increment(t, rt, fmt.Sprint(i))
		wait(t, rt)
	}
	_ = rt.Close()
	checkpoint, _ := journal.Checkpoint()
	if checkpoint == nil {
		t.Fatal("no checkpoint saved")
	}
	journal.reads.Store(0)
	reopened := open(t, journal, immediate, core.Policy{CheckpointEvery: 2})
	length, _ := journal.Length()
	if got := core.Read(reopened, count).N; got != 5 {
		t.Fatalf("count after checkpoint = %d", got)
	}
	if reads := journal.reads.Load(); reads >= length {
		t.Fatalf("replay read %d of %d records", reads, length)
	}
}

func TestAttemptTimeoutRejects(t *testing.T) {
	journal := core.NewMemoryJournal()
	slow := Notify.Implement(func(x *core.Execution, n int) (core.Result[string], error) {
		<-x.Context().Done()
		return core.Result[string]{}, x.Context().Err()
	}, nil)
	rt := open(t, journal, slow, core.Policy{AttemptTimeout: 20 * time.Millisecond})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	if got := core.Read(rt, notified); !slices.Equal(got, []int{-2}) {
		t.Fatalf("notified = %v", got)
	}
}
