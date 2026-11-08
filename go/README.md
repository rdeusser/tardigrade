# Tardigrade for Go

Tardigrade for Go builds durable, event-driven actors in Go. It follows the model of the TypeScript framework: every event is appended to a journal, durable atoms fold the journal into state, effect atoms propose work from that state, and the runtime executes the work and journals its outcome.

```text
{ view, effects } = f(event log)
```

```sh
go get github.com/rdeusser/tardigrade/go
```

The module needs Go 1.27 and has one dependency, the pure Go SQLite driver `modernc.org/sqlite`.

## Packages

| Package | Role |
| --- | --- |
| `reactive` | Dependency-tracked atom graph: sources, derived atoms, families, registries, snapshots |
| `schema` | JSON Schemas derived from Go types and strict decoding at trust boundaries |
| `tardigrade` | Events, durable and effect atoms, acts and their handlers, methods, actors, journals, and the runtime |
| `sqlite` | Thread journals and the thread directory in one SQLite database |
| `host` | Runs an actor's threads, resumes unfinished work, delegates to child threads, serves HTTP |
| `api`, `client` | Wire types of the HTTP method API and a Go client for it |
| `model` | Provider-neutral model calls over Anthropic Messages and OpenAI Chat Completions |
| `library` | Typed tool libraries: filesystem, fetch, notes, alarm, and child agents |
| `agent` | The agent actor: turns, tools, permissions, budgets, and compaction |
| `cmd/tdg` | The CLI: serve the agent, chat, call methods, read journals, scaffold projects |

## Counter actor

A durable atom folds journal events into state. Each `tardigrade.On` declares a transition: how the state changes when one event type arrives. It is typed by that event, registers it for decoding, and returns the next state without mutating the one it receives.

```go
type Incremented struct{}

func (Incremented) EventType() string { return "Incremented" }

type Count struct {
	N  int   `json:"n"`
	At int64 `json:"at"`
}

var count = tardigrade.DurableAtom("count", Count{},
	tardigrade.On(func(c Count, _ Incremented, m tardigrade.Meta) Count { return Count{N: c.N + 1, At: m.Position} }),
)
```

An act is a kind of durable work with a typed input and result, such as a model call, a tool run, or a notification. Actors never run side effects directly: an effect atom proposes an act from state, the runtime journals the acceptance, runs the act with a handler, and journals how it finished. Replaying the journal therefore never repeats work that already finished. The origin is the journal position of the event that made the invocation ready, which keeps invocations distinct across replays.

```go
var Notify = tardigrade.NewAct[int, struct{}]("notify")

var notify = tardigrade.EffectAtom(func(g reactive.Getter) tardigrade.Output[int] {
	c := tardigrade.Get(g, count)
	out := tardigrade.Output[int]{View: c.N}
	if c.N > 0 && c.N%2 == 0 {
		out.Acts = map[string]tardigrade.Proposal{"notify": Notify.Request(tardigrade.Request[int, struct{}]{Origin: new(c.At), Input: c.N})}
	}
	return out
})
```

Methods give callers typed inputs and outputs. An actor combines its root effect atom with methods.

```go
var counter = tardigrade.DefineActor("counter", notify, map[string]tardigrade.Method{
	"increment": tardigrade.NewMethod(tardigrade.MethodSpec[struct{}, int]{
		OnReceive: func(struct{}, tardigrade.MethodContext) tardigrade.Event { return Incremented{} },
		Result: func(_ struct{}, g reactive.Getter, _ tardigrade.MethodContext) *tardigrade.MethodResult[int] {
			return tardigrade.Completed(tardigrade.Get(g, count).N)
		},
	}),
})
```

A handler is the Go code that runs an act; `Act.Handle` builds one. The actor definition holds no handlers, so a host can supply different ones per thread, such as a workspace per thread, or fakes in tests.

```go
store, _ := sqlite.Open(".tardigrade/data/host.sqlite")
notify := Notify.Handle(func(_ *tardigrade.Execution, n int) (tardigrade.Result[struct{}], error) {
	log.Printf("%d is even", n)
	return tardigrade.Value(struct{}{}), nil
}, nil)
h, _ := host.New(host.Options{Actor: counter, Store: store, Handlers: func(*host.Host, tardigrade.ThreadCoordinate) []tardigrade.Handler {
	return []tardigrade.Handler{notify}
}})
http.ListenAndServe(":4242", h.Handler(host.HTTPOptions{}))
```

`tdg init counter` writes this project, and `tdg init meeseeks --template agent` writes an agent project.

## Agent actor

`agent.New` assembles the agent from its options. Its acts are the model call, the compaction summary, the tool run, and the permission and budget requests; `agent.Handlers` returns the handlers that run them on a host. Tool specs go to the actor and tool code goes to the handlers, so each thread can own a workspace.

```go
config, _ := model.ParseConfig([]byte(os.Getenv("TARDIGRADE_CONFIG")))
meeseeks := agent.New(agent.Options{
	Name: "meeseeks", System: "Help the user complete their task.",
	Model: config.Default, ContextWindowTokens: config.ContextWindow(config.Default),
	Tools: library.Specs(library.Filesystem(nil)),
})
router := &model.Router{Config: config}
h, _ := host.New(host.Options{Actor: meeseeks, Store: store, Handlers: func(_ *host.Host, t tardigrade.ThreadCoordinate) []tardigrade.Handler {
	workspace := library.LocalWorkspace{Dir: filepath.Join("workspace", t.Instance, t.Thread)}
	return agent.Handlers(agent.Services{Model: router, Tools: library.NewRegistry(library.Filesystem(workspace))})
}})
```

`agent.Options.Permissions` gates tool calls by policy and sends `ask` decisions to `agent.Services.Permission`. `agent.Options.Budget` limits counted tool calls per turn. `agent.Options.Compaction` bounds the context: above `TriggerRatio` of the context window the agent summarizes older messages and keeps a tail near `RetainRatio`.

## Configuration

The model configuration names a default model and connects providers. Providers named `anthropic`, `openai`, `openrouter`, and `google` inherit preset base URLs, protocols, and credential variables; other providers set `baseUrl`, `protocol` (`anthropic-messages` or `openai-chat-completions`), and `env`.

```json
{
  "default": { "provider": "openrouter", "model_id": "anthropic/claude-sonnet-4.6" },
  "providers": {
    "openrouter": { "models": { "anthropic/claude-sonnet-4.6": { "contextWindowTokens": 200000 } } }
  }
}
```

`models.<id>.options` holds provider request fields merged into each request body, such as `{"thinking": {"type": "adaptive"}}` for Anthropic or `{"reasoning_effort": "high"}` for chat completions.

## CLI

```sh
go install github.com/rdeusser/tardigrade/go/cmd/tdg@latest
TARDIGRADE_CONFIG="$(cat models.json)" tdg serve
tdg chat
tdg call t1 message '{"text":"audit the deploy"}' --wait
tdg events t1 --follow
```

`tdg serve` hosts the agent with filesystem, fetch, notes, alarm, and child-agent tools. It reads `PORT`, `TARDIGRADE_ACTOR_DATA`, `TARDIGRADE_WORKSPACE`, `TARDIGRADE_TOKEN`, and the model configuration from `TARDIGRADE_CONFIG` or the file `TARDIGRADE_CONFIG_PATH` names. When a model has no `contextWindowTokens`, serve looks it up in the models.dev catalog.

## HTTP API

| Route | |
| --- | --- |
| `GET /healthz` | Liveness, public |
| `GET /v1/metadata` | Actor name and API generation |
| `GET /v1/methods` | Methods with input and output JSON Schemas |
| `GET /v1/actors/{instance}/threads` | Thread directory |
| `POST /v1/actors/{instance}/threads` | Allocate a thread with an optional `name` and `parent` |
| `POST .../threads/{thread}/methods/{method}` | Call a method; `Idempotency-Key` names the call |
| `GET .../methods/{method}/calls/{id}` | Read a call: `pending`, `completed`, `failed`, or `cancelled` |
| `PUT .../methods/{method}/calls/{id}/cancellation` | Cancel a call with a `reason` |
| `GET .../threads/{thread}/events` | Journal rows after a one-based `after` cursor |
| `GET .../threads/{thread}/events/stream` | Journal rows as server-sent events, resuming from `Last-Event-ID` |
| `GET .../threads/{thread}/execution/stream` | Transient execution updates, such as streamed model text |

When `TARDIGRADE_TOKEN` is set, every route except `/healthz` requires `Authorization: Bearer`. Declared failures return `{"code", "message"}` with a 4xx status.

## Runtime guarantees

Each thread has one runtime. A commit appends an event and the events its act callbacks derive as one journal batch; when a transition rejects the batch or the append fails, the runtime restores its registry and operation table (`tardigrade/runtime_test.go`, `TestRejectedEventLeavesStateUnchanged`). Accepted work is journaled before it runs, so a reopened journal resumes unfinished work once and records its outcome (`TestReplayResumesPendingWorkOnce`). Retries journal each attempt and its absolute wake time (`TestRetryJournalsAttempts`). Cancellation journals the decision, interrupts the work, and runs the act's cleanup until it succeeds (`TestCancellationRunsCleanup`). A panic in an act records a terminal defect and stops the actor (`TestPanicStopsActor`). Checkpoints save durable atom states while no work is active, so replay folds only later records (`TestCheckpointLimitsReplay`). A host records which threads hold unfinished work and resumes them when it opens the store (`host/host_test.go`, `TestRestartResumesActiveThreads`).

Work that finishes later returns a handle, and its executor finishes it: `tardigrade.ExecutorLocal` for producers started with `tardigrade.Fork`, `tardigrade.ExecutorClock` for timers, `tardigrade.ExecutorThread` for method calls on child threads, and the host's `tardigrade.Promises` service for any other executor, which calls `Runtime.Finish`. Work finishes with a value or a `tardigrade.Failure`, and act callbacks receive that as a `tardigrade.Outcome` with `Value` and `Err`.

## Policies

Every policy value is exported and overridable where it applies.

| Default | Value | Override |
| --- | --- | --- |
| `tardigrade.DefaultAttemptTimeout` | 60s | `tardigrade.Policy.AttemptTimeout`, `tardigrade.RetryPolicy.AttemptTimeout` |
| `tardigrade.DefaultPromiseTimeout` | 60s | `tardigrade.Policy.PromiseTimeout`, `tardigrade.PromiseOptions.Timeout` |
| `tardigrade.DefaultCancellationRetry` | 5s | `tardigrade.Policy.CancellationRetry` |
| `tardigrade.DefaultCheckpointEvery` | 500 records | `tardigrade.Policy.CheckpointEvery` |
| `tardigrade.DefaultInputDigestMinBytes` | 2 KiB | `tardigrade.Policy.InputDigestMinBytes` |
| `host.DefaultMaxDepth` | 3 | `host.Options.MaxDepth` |
| `host.DefaultStreamBuffer` | 64 updates | `host.Options.StreamBuffer` |
| `model.DefaultMaxOutputTokens` | 32,768 | `models.<id>.maxOutputTokens` |
| `model.DefaultFirstChunk`, `model.DefaultIdle` | 60s | `model.Config.FirstChunk`, `model.Config.Idle` |
| `agent.DefaultModelTimeout` | 90s | `agent.Services.AttemptTimeout` |
| `agent.DefaultTriggerRatio`, `agent.DefaultRetainRatio` | 0.8, 0.5 | `agent.CompactionOptions` |
| `library.DefaultFilesystemPolicy` | 30s commands, 32,768 characters | `library.LocalWorkspace.Policy` |

`tardigrade.NoDeadline` disables a timeout.

## Scope

The Go module covers the actor runtime, the self-hosted SQLite host, the HTTP method API and its client, the model providers, the tool libraries, the agent, and the CLI. Cloudflare and Celld hosting stay in TypeScript because Durable Objects are JavaScript classes. Code mode stays in TypeScript because it runs model-written JavaScript; the Go agent calls tools directly. The Go runtime keeps act inputs at or above `tardigrade.DefaultInputDigestMinBytes` beside the journal until their work completes, and keeps file content inline in events.

Event types evolve through Go's `encoding/json`: a type that reads an older shape implements `UnmarshalJSON`. Journal records whose event type no process registered decode as `tardigrade.RawEvent`, which no transition accepts.
