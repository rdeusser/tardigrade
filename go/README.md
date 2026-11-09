# Tardigrade for Go

Tardigrade for Go builds durable, event-driven actors in Go. Every event is appended to a journal, durable atoms fold the journal into state, effect atoms propose work from that state, and the runtime executes the work and journals its outcome. A host serves its actors through HostService, a protobuf API defined in `proto/` with Go and TypeScript clients generated into `api/`.

```text
{ view, effects } = f(event log)
```

```sh
go get github.com/rdeusser/tardigrade/go
```

The module needs Go 1.27. Its dependencies are the pure Go SQLite driver `modernc.org/sqlite`, `google.golang.org/protobuf`, and `connectrpc.com/connect`.

## Packages

| Package | Role |
| --- | --- |
| `reactive` | Dependency-tracked atom graph: sources, derived atoms, families, registries, snapshots |
| `schema` | JSON Schemas derived from Go types and strict decoding at trust boundaries |
| `tardigrade` | Events, durable and effect atoms, acts and their handlers, methods, actors, journals, and the runtime |
| `sqlite` | Thread journals and the thread directory in one SQLite database |
| `host` | Runs actors' threads, resumes unfinished work, delegates to child threads of any served actor, serves HostService |
| `proto/tardigrade/v1` | The HostService definition |
| `api/tardigrade/v1` | Go messages, client, and handler interface generated from `proto/` |
| `api/typescript` | The generated TypeScript client package, `@tardigrade/api` |
| `model` | Model calls to any server implementing Anthropic Messages or OpenAI Chat Completions |
| `library` | Typed tool libraries: filesystem, fetch, notes, alarm, and child agents |
| `agent` | The agent actor: turns, tools, permissions, budgets, and compaction |
| `cmd/tdg` | The CLI: serve the agent, chat, call methods, read journals, scaffold projects |

`buf generate` regenerates `api/` from `proto/`; it runs the Go plugins as module tools and the TypeScript plugin from `api/typescript/node_modules`, so run `bun install` there first.

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

A handler is the Go code that runs an act; `Act.Handle` builds one. The actor definition holds no handlers, so a host can supply different ones per thread, such as a workspace per thread, or fakes in tests. A host serves any number of actors from one store; the address passed to `Handlers` names the thread's actor.

```go
store, _ := sqlite.Open(".tardigrade/data/host.sqlite")
notify := Notify.Handle(func(_ *tardigrade.Execution, n int) (tardigrade.Result[struct{}], error) {
	log.Printf("%d is even", n)
	return tardigrade.Value(struct{}{}), nil
}, nil)
h, _ := host.New(host.Options{Actors: []*tardigrade.Actor{counter}, Store: store, Handlers: func(*host.Host, tardigrade.ThreadCoordinate) []tardigrade.Handler {
	return []tardigrade.Handler{notify}
}})
http.ListenAndServe(":4242", h.Handler(host.HandlerOptions{}))
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
h, _ := host.New(host.Options{Actors: []*tardigrade.Actor{meeseeks}, Store: store, Handlers: func(_ *host.Host, t tardigrade.ThreadCoordinate) []tardigrade.Handler {
	workspace := library.LocalWorkspace{Dir: filepath.Join("workspace", t.Instance, t.Thread)}
	return agent.Handlers(agent.Services{Model: router, Tools: library.NewRegistry(library.Filesystem(workspace))})
}})
```

`library.Agents(h, "researcher", input)` lets an agent delegate to child threads of the `researcher` actor on the same host; the child's answer returns to the parent as a new turn. `agent.Options.Permissions` gates tool calls by policy and sends `ask` decisions to `agent.Services.Permission`. `agent.Options.Budget` limits counted tool calls per turn. `agent.Options.Compaction` bounds the context: above `TriggerRatio` of the context window the agent summarizes older messages and keeps a tail near `RetainRatio`.

## Configuration

The model configuration names a default model and connects providers. Each provider speaks `anthropic-messages` or `openai-chat-completions`, so any server implementing the Anthropic Messages API or the OpenAI Chat Completions API can serve it, including a model server on the same machine. A provider sets `baseUrl`, `protocol`, and `env`, the variables holding its API key. A provider named `openrouter` inherits OpenRouter's base URL, the chat completions protocol, and `OPENROUTER_API_KEY` for the fields it leaves empty.

```json
{
  "default": { "provider": "openrouter", "model_id": "anthropic/claude-sonnet-4.6" },
  "providers": {
    "openrouter": { "models": { "anthropic/claude-sonnet-4.6": { "contextWindowTokens": 200000 } } },
    "local": { "baseUrl": "http://127.0.0.1:8080/v1", "protocol": "openai-chat-completions", "models": { "qwen3": { "contextWindowTokens": 32768 } } }
  }
}
```

`models.<id>.options` holds provider request fields merged into each request body, such as `{"thinking": {"type": "adaptive"}}` for Anthropic Messages or `{"reasoning_effort": "high"}` for chat completions. `maxTokensField` names the chat completions output bound field for servers that expect `max_completion_tokens`.

## CLI

```sh
go install github.com/rdeusser/tardigrade/go/cmd/tdg@latest
TARDIGRADE_CONFIG="$(cat models.json)" tdg serve
tdg chat
tdg call t1 message '{"text":"audit the deploy"}' --wait
tdg events t1 --follow
```

`tdg serve` hosts the agent with filesystem, fetch, notes, alarm, and child-agent tools. It reads `PORT`, `TARDIGRADE_ACTOR_DATA`, `TARDIGRADE_WORKSPACE`, `TARDIGRADE_TOKEN`, and the model configuration from `TARDIGRADE_CONFIG` or the file `TARDIGRADE_CONFIG_PATH` names; the default model must set `contextWindowTokens`. Client commands address threads of `--actor`, which defaults to the host's only actor.

## HostService

`host.Host.Handler` serves HostService over the Connect, gRPC, and gRPC-Web protocols, plus a public `GET /healthz`. Connect clients work over HTTP/1.1; gRPC clients need HTTP/2, which `tdg serve` accepts without TLS.

| Procedure | |
| --- | --- |
| `ListActors` | Served actors with their methods and input and output JSON Schemas |
| `CreateThread` | Allocate a thread of an actor, optionally as a child of another thread |
| `ListThreads` | The thread directory, filtered by actor and instance |
| `StartCall` | Journal a method call under a caller-chosen call ID |
| `GetCall`, `WatchCall` | Read a call, or stream it until it finishes |
| `CancelCall` | Ask a cancellable call to stop |
| `ListRecords`, `WatchRecords` | Journal records after a one-based position, or stream them as they commit |
| `WatchUpdates` | Transient execution updates, such as streamed model text |

Method inputs and outputs travel as `google.protobuf.Value` and journal events as `google.protobuf.Struct`, so each actor keeps its own JSON shapes. `StartCall` absorbs a repeated call with the same ID and input, and an absent input is the empty object. When `TARDIGRADE_TOKEN` is set, every procedure requires `Authorization: Bearer`.

| Error | Code |
| --- | --- |
| Invalid input, or a name with a slash | `invalid_argument` |
| Unknown actor, thread, method, or call | `not_found` |
| A call ID reused with other input | `aborted` |
| A stopped actor, or a method without cancellation | `failed_precondition` |
| A closing host | `unavailable` |
| A missing or wrong token | `unauthenticated` |

The TypeScript client builds on `@connectrpc/connect`:

```ts
import { fromJson } from "@bufbuild/protobuf"
import { ValueSchema } from "@bufbuild/protobuf/wkt"
import { createHostClient } from "@tardigrade/api"

const client = createHostClient("http://localhost:4242", process.env.TARDIGRADE_TOKEN)
const thread = { actor: "counter", instance: "main", thread: "t1" }
await client.createThread({ actor: "counter", instance: "main", name: "t1" })
await client.startCall({ thread, method: "increment", callId: "c1", input: fromJson(ValueSchema, {}) })
for await (const { call } of client.watchCall({ thread, callId: "c1" })) console.log(call?.status)
```

## Runtime guarantees

Each thread has one runtime. A commit appends an event and the events its act callbacks derive as one journal batch; when a transition rejects the batch or the append fails, the runtime restores its registry and operation table (`tardigrade/runtime_test.go`, `TestRejectedEventLeavesStateUnchanged`). Accepted work is journaled before it runs, so a reopened journal resumes unfinished work once and records its outcome (`TestReplayResumesPendingWorkOnce`). Retries journal each attempt and its absolute wake time (`TestRetryJournalsAttempts`). Cancellation journals the decision, interrupts the work, and runs the act's cleanup until it succeeds (`TestCancellationRunsCleanup`). A panic in an act records a terminal defect and stops the actor (`TestPanicStopsActor`). Checkpoints save durable atom states while no work is active, so replay folds only later records (`TestCheckpointLimitsReplay`). A host records which threads hold unfinished work and resumes them when it opens the store (`host/host_test.go`, `TestRestartResumesActiveThreads`). Delegation to another actor's thread finishes the parent's promise with the child call's result (`agent/delegation_test.go`, `TestDelegationToAnotherActorReturnsAnswerAsTurn`).

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
| `host.DefaultReadMaxBytes` | 8 MiB | `host.HandlerOptions.ReadMaxBytes` |
| `model.DefaultMaxOutputTokens` | 32,768 | `models.<id>.maxOutputTokens` |
| `model.DefaultFirstChunk`, `model.DefaultIdle` | 60s | `model.Config.FirstChunk`, `model.Config.Idle` |
| `agent.DefaultModelTimeout` | 90s | `agent.Services.AttemptTimeout` |
| `agent.DefaultTriggerRatio`, `agent.DefaultRetainRatio` | 0.8, 0.5 | `agent.CompactionOptions` |
| `library.DefaultFilesystemPolicy` | 30s commands, 32,768 characters | `library.LocalWorkspace.Policy` |

`tardigrade.NoDeadline` disables a timeout.

## Scope

Tardigrade for Go is self-contained: its journals, hosts, and HostService are shared only by programs built on this module and clients generated from `proto/`. TypeScript programs reach a host through `@tardigrade/api` as clients. Hosts run on a workstation or a local cluster with SQLite storage, and the only network services they call are model providers; OpenRouter is the hosted provider the configuration knows by name. The agent calls tools directly. The runtime keeps act inputs at or above `tardigrade.DefaultInputDigestMinBytes` beside the journal until their work completes, and keeps file content inline in events.

Event types evolve through Go's `encoding/json`: a type that reads an older shape implements `UnmarshalJSON`. Journal records whose event type no process registered decode as `tardigrade.RawEvent`, which no transition accepts.
