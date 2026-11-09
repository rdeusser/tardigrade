package host_test

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	tardigradev1 "github.com/rdeusser/tardigrade/go/api/tardigrade/v1"
	"github.com/rdeusser/tardigrade/go/api/tardigrade/v1/tardigradev1connect"
	"github.com/rdeusser/tardigrade/go/host"
	"github.com/rdeusser/tardigrade/go/reactive"
	"github.com/rdeusser/tardigrade/go/sqlite"
	"github.com/rdeusser/tardigrade/go/tardigrade"
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

var relay = tardigrade.DurableAtom("relay.state", relayState{},
	tardigrade.On(func(s relayState, e Asked, m tardigrade.Meta) relayState {
		return relayState{Open: append(slices.Clone(s.Open), question{ID: e.ID, Depth: e.Depth, At: m.Position}), Answers: s.Answers}
	}),
	tardigrade.On(func(s relayState, e Answered, _ tardigrade.Meta) relayState {
		answers := map[string]string{e.ID: e.Answer}
		maps.Copy(answers, s.Answers)
		open := slices.DeleteFunc(slices.Clone(s.Open), func(q question) bool { return q.ID == e.ID })
		return relayState{Open: open, Answers: answers}
	}),
)

type askInput struct {
	Depth int `json:"depth"`
}

var (
	Delegate = tardigrade.NewAct[askInput, string]("relay.delegate")
	Answer   = tardigrade.NewAct[askInput, string]("relay.answer")
)

var relayRoot = tardigrade.EffectAtom(func(g reactive.Getter) tardigrade.Output[int] {
	state := tardigrade.Get(g, relay)
	acts := map[string]tardigrade.Proposal{}
	for _, q := range state.Open {
		act := Answer
		if q.Depth > 0 {
			act = Delegate
		}
		id := q.ID
		acts[q.ID] = act.Request(tardigrade.Request[askInput, string]{Origin: new(q.At), Input: askInput{Depth: q.Depth},
			OnFinished: func(o tardigrade.Outcome[string], _ tardigrade.Ref) []tardigrade.Event {
				if o.Err != nil {
					return []tardigrade.Event{Answered{ID: id, Answer: "error: " + o.Err.Message}}
				}
				return []tardigrade.Event{Answered{ID: id, Answer: o.Value}}
			}})
	}
	return tardigrade.Output[int]{View: len(state.Open), Acts: acts}
})

var relayActor = tardigrade.DefineActor("relay", relayRoot, map[string]tardigrade.Method{
	"ask": tardigrade.NewMethod(tardigrade.MethodSpec[askInput, string]{
		OnReceive: func(in askInput, c tardigrade.MethodContext) tardigrade.Event {
			return Asked{ID: c.ID, Depth: in.Depth}
		},
		Result: func(_ askInput, g reactive.Getter, c tardigrade.MethodContext) *tardigrade.MethodResult[string] {
			if answer, ok := tardigrade.Get(g, relay).Answers[c.ID]; ok {
				return tardigrade.Completed(answer)
			}
			return nil
		},
	}),
})

func relayActs(release <-chan struct{}) func(h *host.Host, address tardigrade.ThreadCoordinate) []tardigrade.Handler {
	return func(h *host.Host, address tardigrade.ThreadCoordinate) []tardigrade.Handler {
		return []tardigrade.Handler{
			Delegate.Handle(func(x *tardigrade.Execution, in askInput) (tardigrade.Result[string], error) {
				handle, err := h.Delegate(x.Address(), x.Ref(), "relay", "ask", askInput{Depth: in.Depth - 1})
				if err != nil {
					return tardigrade.Result[string]{}, err
				}
				return tardigrade.Defer[string](handle, tardigrade.PromiseOptions{Timeout: tardigrade.NoDeadline}), nil
			}, nil),
			Answer.Handle(func(x *tardigrade.Execution, in askInput) (tardigrade.Result[string], error) {
				if release != nil {
					select {
					case <-release:
					case <-x.Context().Done():
						return tardigrade.Result[string]{}, x.Context().Err()
					}
				}
				return tardigrade.Value(fmt.Sprintf("answered at %s", address.Thread)), nil
			}, nil),
		}
	}
}

type bearer struct {
	token string
	next  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(r)
}

func connectTo(server *httptest.Server, token string) tardigradev1connect.HostServiceClient {
	return tardigradev1connect.NewHostServiceClient(&http.Client{Transport: bearer{token, server.Client().Transport}}, server.URL)
}

func start(t *testing.T, path string, release <-chan struct{}) (*host.Host, *httptest.Server) {
	t.Helper()
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := host.New(host.Options{Actors: []*tardigrade.Actor{relayActor}, Store: store, Handlers: relayActs(release)})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h.Handler(host.HandlerOptions{Token: "secret"}))
	t.Cleanup(func() {
		server.Close()
		h.Close()
		store.Close()
	})
	return h, server
}

func await(ctx context.Context, t *testing.T, c tardigradev1connect.HostServiceClient, thread *tardigradev1.ThreadRef, id string) *tardigradev1.Call {
	t.Helper()
	stream, err := c.WatchCall(ctx, connect.NewRequest(&tardigradev1.WatchCallRequest{Thread: thread, CallId: id}))
	if err != nil {
		t.Fatal(err)
	}
	var last *tardigradev1.Call
	for stream.Receive() {
		last = stream.Msg().Call
	}
	if err := stream.Err(); err != nil {
		t.Fatal(err)
	}
	return last
}

func ask(ctx context.Context, c tardigradev1connect.HostServiceClient, thread *tardigradev1.ThreadRef, method, id string, depth int) error {
	input, _ := structpb.NewValue(map[string]any{"depth": depth})
	_, err := c.StartCall(ctx, connect.NewRequest(&tardigradev1.StartCallRequest{Thread: thread, Method: method, CallId: id, Input: input}))
	return err
}

func TestDelegationFinishesThroughChildThreads(t *testing.T) {
	_, server := start(t, filepath.Join(t.TempDir(), "host.sqlite"), nil)
	c := connectTo(server, "secret")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	actors, err := c.ListActors(ctx, connect.NewRequest(&tardigradev1.ListActorsRequest{}))
	if err != nil || len(actors.Msg.Actors) != 1 || actors.Msg.Actors[0].Name != "relay" || actors.Msg.Actors[0].Methods[0].InputSchema.Fields["type"].GetStringValue() != "object" {
		t.Fatalf("actors = %v, %v", actors, err)
	}
	created, err := c.CreateThread(ctx, connect.NewRequest(&tardigradev1.CreateThreadRequest{Actor: "relay", Instance: "main", Name: "root"}))
	if err != nil {
		t.Fatal(err)
	}
	thread := created.Msg.Thread.Ref
	if err := ask(ctx, c, thread, "ask", "q1", 2); err != nil {
		t.Fatal(err)
	}
	if call := await(ctx, t, c, thread, "q1"); call.Status != tardigradev1.CallStatus_CALL_STATUS_COMPLETED || call.GetOutput().GetStringValue() != "answered at root.2.2" || call.Method != "ask" {
		t.Fatalf("call = %v", call)
	}
	threads, err := c.ListThreads(ctx, connect.NewRequest(&tardigradev1.ListThreadsRequest{Actor: "relay", Instance: "main"}))
	if err != nil || len(threads.Msg.Threads) != 3 || threads.Msg.Threads[2].Depth != 2 || threads.Msg.Threads[1].Parent.GetThread() != "root" {
		t.Fatalf("threads = %v, %v", threads, err)
	}
	records, err := c.ListRecords(ctx, connect.NewRequest(&tardigradev1.ListRecordsRequest{Thread: thread, Limit: 2}))
	if err != nil || len(records.Msg.Records) != 2 || records.Msg.Records[0].Seq != 1 {
		t.Fatalf("records = %v, %v", records, err)
	}
	watch, err := c.WatchRecords(ctx, connect.NewRequest(&tardigradev1.WatchRecordsRequest{Thread: thread}))
	if err != nil {
		t.Fatal(err)
	}
	for watch.Receive() && watch.Msg().Record.MessageId != "q1" {
	}
	if record := watch.Msg().Record; record.MessageId != "q1" || record.Event.Fields["type"].GetStringValue() != record.Type {
		t.Fatalf("watched record = %v, %v", record, watch.Err())
	}
	watch.Close()
	codes := map[string]connect.Code{}
	codes["conflict"] = connect.CodeOf(ask(ctx, c, thread, "ask", "q1", 3))
	codes["method"] = connect.CodeOf(ask(ctx, c, thread, "missing", "q2", 0))
	codes["actor"] = connect.CodeOf(ask(ctx, c, &tardigradev1.ThreadRef{Actor: "nobody", Instance: "main", Thread: "root"}, "ask", "q3", 0))
	_, err = connectTo(server, "wrong").ListActors(ctx, connect.NewRequest(&tardigradev1.ListActorsRequest{}))
	codes["token"] = connect.CodeOf(err)
	want := map[string]connect.Code{"conflict": connect.CodeAborted, "method": connect.CodeNotFound, "actor": connect.CodeNotFound, "token": connect.CodeUnauthenticated}
	if !maps.Equal(codes, want) {
		t.Fatalf("codes = %v", codes)
	}
}

func TestRestartResumesActiveThreads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.sqlite")
	blocked := make(chan struct{})
	store, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := host.New(host.Options{Actors: []*tardigrade.Actor{relayActor}, Store: store, Handlers: relayActs(blocked)})
	if err != nil {
		t.Fatal(err)
	}
	address, err := h.CreateThread("relay", "main", "root", nil)
	if err != nil {
		t.Fatal(err)
	}
	rt, _ := h.Thread(address)
	if err := rt.Invoke("ask", "q1", tardigrade.Sender{Kind: "external", ID: "test"}, []byte(`{"depth":0}`)); err != nil {
		t.Fatal(err)
	}
	h.Close()
	store.Close()

	released := make(chan struct{})
	close(released)
	resumed, server := start(t, path, released)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if call := await(ctx, t, connectTo(server, "secret"), &tardigradev1.ThreadRef{Actor: "relay", Instance: "main", Thread: "root"}, "q1"); call.Status != tardigradev1.CallStatus_CALL_STATUS_COMPLETED {
		t.Fatalf("call = %v", call)
	}
	if err := resumed.WaitIdle(5 * time.Second); err != nil {
		t.Fatal(err)
	}
}
