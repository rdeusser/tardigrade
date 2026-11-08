package tardigrade_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rdeusser/tardigrade/go/reactive"
	"github.com/rdeusser/tardigrade/go/tardigrade"
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

var count = tardigrade.DurableAtom("test.count", counterState{},
	tardigrade.On(func(s counterState, _ Incremented, m tardigrade.Meta) counterState {
		return counterState{N: s.N + 1, At: m.Position}
	}),
	tardigrade.On(func(s counterState, _ Poisoned, _ tardigrade.Meta) counterState { panic("poisoned event") }),
)

var notified = tardigrade.DurableAtom("test.notified", []int(nil),
	tardigrade.On(func(s []int, e Notified, _ tardigrade.Meta) []int { return append(slices.Clone(s), e.Value) }),
)

var Notify = tardigrade.NewAct[int, string]("test.notify")

var root = tardigrade.EffectAtom(func(g reactive.Getter) tardigrade.Output[int] {
	state := tardigrade.Get(g, count)
	out := tardigrade.Output[int]{View: state.N}
	if state.N > 0 && state.N%2 == 0 && !slices.Contains(tardigrade.Get(g, notified), state.N) {
		n := state.N
		out.Acts = map[string]tardigrade.Proposal{"notify": Notify.Request(tardigrade.Request[int, string]{
			Origin: new(state.At), Input: n,
			OnFinished: func(o tardigrade.Outcome[string], _ tardigrade.Ref) []tardigrade.Event {
				if o.Err != nil {
					return []tardigrade.Event{Notified{Value: -n}}
				}
				return []tardigrade.Event{Notified{Value: n}}
			},
		})}
	}
	return out
})

var counter = tardigrade.DefineActor("counter", root, map[string]tardigrade.Method{
	"increment": tardigrade.NewMethod(tardigrade.MethodSpec[struct{}, int]{
		OnReceive: func(struct{}, tardigrade.MethodContext) tardigrade.Event { return Incremented{} },
		Result: func(_ struct{}, g reactive.Getter, _ tardigrade.MethodContext) *tardigrade.MethodResult[int] {
			return tardigrade.Completed(tardigrade.Get(g, count).N)
		},
	}),
})

var external = tardigrade.Sender{Kind: "external", ID: "test"}

func open(t *testing.T, journal tardigrade.Journal, impl tardigrade.Handler, policy tardigrade.Policy) *tardigrade.Runtime {
	t.Helper()
	rt, err := tardigrade.Open(tardigrade.Options{Actor: counter, Journal: journal, Handlers: []tardigrade.Handler{impl}, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return rt
}

func wait(t *testing.T, rt *tardigrade.Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rt.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func eventually(t *testing.T, rt *tardigrade.Runtime, check func() bool) {
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

func increment(t *testing.T, rt *tardigrade.Runtime, id string) {
	t.Helper()
	if err := rt.Invoke("increment", id, external, []byte("{}")); err != nil {
		t.Fatal(err)
	}
}

func types(t *testing.T, journal tardigrade.Journal) []string {
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

var immediate = Notify.Handle(func(_ *tardigrade.Execution, n int) (tardigrade.Result[string], error) {
	return tardigrade.Value(fmt.Sprint(n)), nil
}, nil)

func TestCounterNotifiesThroughAct(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	rt := open(t, journal, immediate, tardigrade.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	if got := tardigrade.Read(rt, notified); !slices.Equal(got, []int{2}) {
		t.Fatalf("notified = %v", got)
	}
	state, err := rt.Call("increment", "b")
	if err != nil || state.Status != tardigrade.StatusCompleted || string(state.Output) != "2" {
		t.Fatalf("call state = %+v, %v", state, err)
	}
	want := []string{"test.Incremented", "test.Incremented", "EffectRequested", "EffectFinished", "test.Notified"}
	if got := types(t, journal); !slices.Equal(got, want) {
		t.Fatalf("journal = %v", got)
	}
}

func TestInvokeIsIdempotentPerID(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	rt := open(t, journal, immediate, tardigrade.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "a")
	if n := tardigrade.Read(rt, count).N; n != 1 {
		t.Fatalf("count = %d", n)
	}
	if err := rt.Invoke("increment", "a", tardigrade.Sender{Kind: "external", ID: "other"}, []byte("{}")); !errors.Is(err, tardigrade.ErrMessageConflict) {
		t.Fatalf("conflict = %v", err)
	}
}

func TestReplayResumesPendingWorkOnce(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	started := make(chan struct{})
	blocking := Notify.Handle(func(x *tardigrade.Execution, n int) (tardigrade.Result[string], error) {
		close(started)
		<-x.Context().Done()
		return tardigrade.Result[string]{}, x.Context().Err()
	}, nil)
	rt, err := tardigrade.Open(tardigrade.Options{Actor: counter, Journal: journal, Handlers: []tardigrade.Handler{blocking}})
	if err != nil {
		t.Fatal(err)
	}
	increment(t, rt, "a")
	increment(t, rt, "b")
	<-started
	_ = rt.Close()
	reopened := open(t, journal, immediate, tardigrade.Policy{})
	wait(t, reopened)
	if got := tardigrade.Read(reopened, notified); !slices.Equal(got, []int{2}) {
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
	journal := tardigrade.NewMemoryJournal()
	var attempts atomic.Int32
	flaky := Notify.Handle(func(x *tardigrade.Execution, n int) (tardigrade.Result[string], error) {
		value, err := tardigrade.Retry(x, tardigrade.RetryPolicy{Decide: func(err error, attempt int) *tardigrade.RetryDecision {
			return &tardigrade.RetryDecision{Delay: time.Millisecond, Reason: err.Error()}
		}}, func(context.Context) (string, error) {
			if attempts.Add(1) < 3 {
				return "", errors.New("transient")
			}
			return "ok", nil
		})
		return tardigrade.Value(value), err
	}, nil)
	rt := open(t, journal, flaky, tardigrade.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	retries := 0
	for _, name := range types(t, journal) {
		if name == "RetryScheduled" {
			retries++
		}
	}
	if retries != 2 || !slices.Equal(tardigrade.Read(rt, notified), []int{2}) {
		t.Fatalf("retries = %d notified = %v", retries, tardigrade.Read(rt, notified))
	}
}

func TestForkSettlesPromise(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	forked := Notify.Handle(func(x *tardigrade.Execution, n int) (tardigrade.Result[string], error) {
		return tardigrade.Fork(x, tardigrade.PromiseOptions{}, func(context.Context) (string, error) { return "later", nil }), nil
	}, nil)
	rt := open(t, journal, forked, tardigrade.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	eventually(t, rt, func() bool { return len(tardigrade.Read(rt, notified)) == 1 })
	want := []string{"test.Incremented", "test.Incremented", "EffectRequested", "EffectFinished", "PromiseFinished", "test.Notified"}
	if got := types(t, journal); !slices.Equal(got, want) {
		t.Fatalf("journal = %v", got)
	}
}

func TestPromiseDeadlineRejects(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	never := Notify.Handle(func(x *tardigrade.Execution, n int) (tardigrade.Result[string], error) {
		return tardigrade.Defer[string](tardigrade.Handle{Executor: "external", ID: "job"}, tardigrade.PromiseOptions{Timeout: 20 * time.Millisecond}), nil
	}, nil)
	rt, err := tardigrade.Open(tardigrade.Options{Actor: counter, Journal: journal, Handlers: []tardigrade.Handler{never}, Promises: ignoringPromises{}})
	if err != nil {
		t.Fatal(err)
	}
	defer rt.Close()
	increment(t, rt, "a")
	increment(t, rt, "b")
	eventually(t, rt, func() bool { return len(tardigrade.Read(rt, notified)) == 1 })
	if got := tardigrade.Read(rt, notified); got[0] != -2 {
		t.Fatalf("notified = %v, want rejection", got)
	}
}

type ignoringPromises struct{}

func (ignoringPromises) Watch(tardigrade.PromiseWatch) error  { return nil }
func (ignoringPromises) Cancel(tardigrade.PromiseWatch) error { return nil }

func TestCancellationRunsCleanup(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	started := make(chan struct{})
	var cleaned atomic.Bool
	blocking := Notify.Handle(func(x *tardigrade.Execution, n int) (tardigrade.Result[string], error) {
		close(started)
		<-x.Context().Done()
		return tardigrade.Result[string]{}, x.Context().Err()
	}, func(c tardigrade.Cleanup, n int) error {
		cleaned.Store(true)
		return nil
	})
	rt := open(t, journal, blocking, tardigrade.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	<-started
	if err := rt.Cancel(tardigrade.Ref{Seq: 2, Atom: "counter", Act: Notify.Name()}, "stop"); err != nil {
		t.Fatal(err)
	}
	wait(t, rt)
	if !cleaned.Load() {
		t.Fatal("cleanup did not run")
	}
	want := []string{"test.Incremented", "test.Incremented", "EffectRequested", "EffectCancelled", "test.Notified", "CleanupFinished"}
	if got := types(t, journal); !slices.Equal(got, want) {
		t.Fatalf("journal = %v", got)
	}
}

func TestPanicStopsActor(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	broken := Notify.Handle(func(*tardigrade.Execution, int) (tardigrade.Result[string], error) { panic("boom") }, nil)
	rt := open(t, journal, broken, tardigrade.Policy{})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	var stopped *tardigrade.StoppedError
	if err := rt.Invoke("increment", "c", external, []byte("{}")); !errors.As(err, &stopped) {
		t.Fatalf("invoke after defect = %v", err)
	}
	state, _ := rt.Call("increment", "b")
	if state.Status != tardigrade.StatusCompleted {
		t.Fatalf("settled call state = %+v", state)
	}
}

func TestRejectedEventLeavesStateUnchanged(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	rt := open(t, journal, immediate, tardigrade.Policy{})
	increment(t, rt, "a")
	if err := rt.Send(Poisoned{}); err == nil {
		t.Fatal("poisoned event accepted")
	}
	if length, _ := journal.Length(); length != 1 || tardigrade.Read(rt, count).N != 1 {
		t.Fatalf("length = %d count = %d", length, tardigrade.Read(rt, count).N)
	}
	increment(t, rt, "b")
	wait(t, rt)
	if tardigrade.Read(rt, count).N != 2 {
		t.Fatal("runtime unusable after rejection")
	}
}

type countingJournal struct {
	*tardigrade.MemoryJournal
	reads atomic.Int64
}

func (j *countingJournal) Read(from, to int64) ([]tardigrade.Record, error) {
	j.reads.Add(to - from)
	return j.MemoryJournal.Read(from, to)
}

func TestCheckpointLimitsReplay(t *testing.T) {
	journal := &countingJournal{MemoryJournal: tardigrade.NewMemoryJournal()}
	rt := open(t, journal, immediate, tardigrade.Policy{CheckpointEvery: 2})
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
	reopened := open(t, journal, immediate, tardigrade.Policy{CheckpointEvery: 2})
	length, _ := journal.Length()
	if got := tardigrade.Read(reopened, count).N; got != 5 {
		t.Fatalf("count after checkpoint = %d", got)
	}
	if reads := journal.reads.Load(); reads >= length {
		t.Fatalf("replay read %d of %d records", reads, length)
	}
}

func TestAttemptTimeoutRejects(t *testing.T) {
	journal := tardigrade.NewMemoryJournal()
	slow := Notify.Handle(func(x *tardigrade.Execution, n int) (tardigrade.Result[string], error) {
		<-x.Context().Done()
		return tardigrade.Result[string]{}, x.Context().Err()
	}, nil)
	rt := open(t, journal, slow, tardigrade.Policy{AttemptTimeout: 20 * time.Millisecond})
	increment(t, rt, "a")
	increment(t, rt, "b")
	wait(t, rt)
	if got := tardigrade.Read(rt, notified); !slices.Equal(got, []int{-2}) {
		t.Fatalf("notified = %v", got)
	}
}
