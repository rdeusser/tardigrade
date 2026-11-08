package host_test

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/rdeusser/tardigrade/go/client"
	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/host"
	"github.com/rdeusser/tardigrade/go/reactive"
	"github.com/rdeusser/tardigrade/go/sqlite"
)

type Asked struct {
	ID    string `json:"id"`
	Depth int    `json:"depth"`
}
type Answered struct {
	ID     string `json:"id"`
	Answer string `json:"answer"`
}

func (Asked) EventType() string    { return "relay.Asked" }
func (Answered) EventType() string { return "relay.Answered" }

type question struct {
	ID    string `json:"id"`
	Depth int    `json:"depth"`
	At    int64  `json:"at"`
}

type relayState struct {
	Open    []question        `json:"open"`
	Answers map[string]string `json:"answers"`
}

var relay = core.DurableAtom("relay.state", relayState{},
	core.On(func(s relayState, e Asked, m core.Meta) relayState {
		return relayState{Open: append(slices.Clone(s.Open), question{ID: e.ID, Depth: e.Depth, At: m.Position}), Answers: s.Answers}
	}),
	core.On(func(s relayState, e Answered, _ core.Meta) relayState {
		answers := map[string]string{e.ID: e.Answer}
		for k, v := range s.Answers {
			answers[k] = v
		}
		open := slices.DeleteFunc(slices.Clone(s.Open), func(q question) bool { return q.ID == e.ID })
		return relayState{Open: open, Answers: answers}
	}),
)

type askInput struct {
	Depth int `json:"depth"`
}

var (
	Delegate = core.NewAct[askInput, string]("relay.delegate")
	Answer   = core.NewAct[askInput, string]("relay.answer")
)

var relayRoot = core.EffectAtom(func(g reactive.Getter) core.Output[int] {
	state := core.Get(g, relay)
	acts := map[string]core.Proposal{}
	for _, q := range state.Open {
		act := Answer
		if q.Depth > 0 {
			act = Delegate
		}
		id := q.ID
		acts[q.ID] = act.Request(core.Request[askInput, string]{Origin: core.At(q.At), Input: askInput{Depth: q.Depth},
			OnSettled: func(o core.Outcome[string], _ core.Ref) []core.Event {
				if o.Reason != nil {
					return []core.Event{Answered{ID: id, Answer: "error: " + o.Reason.Message}}
				}
				return []core.Event{Answered{ID: id, Answer: o.Value}}
			}})
	}
	return core.Output[int]{View: len(state.Open), Acts: acts}
})

var relayActor = core.DefineActor("relay", relayRoot, map[string]core.Method{
	"ask": core.NewMethod(core.MethodSpec[askInput, string]{
		OnReceive: func(in askInput, c core.MethodContext) core.Event { return Asked{ID: c.ID, Depth: in.Depth} },
		Result: func(_ askInput, g reactive.Getter, c core.MethodContext) *core.MethodResult[string] {
			if answer, ok := core.Get(g, relay).Answers[c.ID]; ok {
				return core.Completed(answer)
			}
			return nil
		},
	}),
})

func relayActs(release <-chan struct{}) func(h *host.Host, address core.ThreadCoordinate) []core.Implementation {
	return func(h *host.Host, address core.ThreadCoordinate) []core.Implementation {
		return []core.Implementation{
			Delegate.Implement(func(x *core.Execution, in askInput) (core.Result[string], error) {
				handle, err := h.Delegate(x.Address(), x.Ref(), "ask", askInput{Depth: in.Depth - 1})
				if err != nil {
					return core.Result[string]{}, err
				}
				return core.Defer[string](handle, core.PromiseOptions{Timeout: core.NoDeadline}), nil
			}, nil),
			Answer.Implement(func(x *core.Execution, in askInput) (core.Result[string], error) {
				if release != nil {
					select {
					case <-release:
					case <-x.Context().Done():
						return core.Result[string]{}, x.Context().Err()
					}
				}
				return core.Value(fmt.Sprintf("answered at %s", address.Thread)), nil
			}, nil),
		}
	}
}

func start(t *testing.T, path string, release <-chan struct{}) (*host.Host, *client.Client) {
	t.Helper()
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := host.New(host.Options{Actor: relayActor, Store: store, Acts: relayActs(release)})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h.Handler(host.HTTPOptions{Token: "secret"}))
	t.Cleanup(func() {
		server.Close()
		h.Close()
		store.Close()
	})
	return h, client.New(server.URL, "secret")
}

func TestDelegationSettlesThroughChildThreads(t *testing.T) {
	_, c := start(t, filepath.Join(t.TempDir(), "host.sqlite"), nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if metadata, err := c.Metadata(ctx); err != nil || metadata.Name != "relay" {
		t.Fatalf("metadata = %+v, %v", metadata, err)
	}
	thread, err := c.CreateThread(ctx, "main", "root", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Invoke(ctx, thread, "ask", "q1", askInput{Depth: 2}); err != nil {
		t.Fatal(err)
	}
	state, err := c.Await(ctx, thread, "ask", "q1", 10*time.Millisecond)
	if err != nil || state.Status != core.StatusCompleted || string(state.Output) != `"answered at root.2.2"` {
		t.Fatalf("state = %+v, %v", state, err)
	}
	threads, err := c.Threads(ctx, "main")
	if err != nil || len(threads) != 3 || threads[2].Depth != 2 {
		t.Fatalf("threads = %+v, %v", threads, err)
	}
	rows, err := c.Events(ctx, thread, 0)
	if err != nil || len(rows) == 0 || rows[0].Seq != 1 {
		t.Fatalf("events = %v, %v", rows, err)
	}
	if err := c.Invoke(ctx, thread, "missing", "q2", askInput{}); err == nil {
		t.Fatal("unknown method accepted")
	}
	if _, err := client.New(c.BaseURL, "").Methods(ctx); err == nil {
		t.Fatal("missing token accepted")
	}
}

func TestRestartResumesActiveThreads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.sqlite")
	blocked := make(chan struct{})
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := host.New(host.Options{Actor: relayActor, Store: store, Acts: relayActs(blocked)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.CreateThread("main", "root", ""); err != nil {
		t.Fatal(err)
	}
	rt, _ := h.Thread("main", "root")
	if err := rt.Invoke("ask", "q1", core.Sender{Kind: "external", ID: "test"}, []byte(`{"depth":0}`)); err != nil {
		t.Fatal(err)
	}
	h.Close()
	store.Close()

	released := make(chan struct{})
	close(released)
	resumed, c := start(t, path, released)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	state, err := c.Await(ctx, core.ThreadCoordinate{Instance: "main", Thread: "root"}, "ask", "q1", 10*time.Millisecond)
	if err != nil || state.Status != core.StatusCompleted {
		t.Fatalf("state = %+v, %v", state, err)
	}
	if err := resumed.WaitIdle(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}
