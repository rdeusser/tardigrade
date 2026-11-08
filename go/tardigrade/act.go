package tardigrade

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rdeusser/tardigrade/go/reactive"
)

// Output is what an effect atom derives: a view for its parent and proposals for the runtime. The runtime collects proposals only from the actor's root atom, so parents merge their children's Events and Acts.
type Output[V any] struct {
	View V
	// Events proposes domain events keyed by source; a producer withdraws an event once its effect is visible.
	Events map[string]Event
	// Acts proposes act requests and cancellations keyed by source.
	Acts map[string]Proposal
}

func (o Output[V]) proposals() (map[string]Event, map[string]Proposal) { return o.Events, o.Acts }

type proposer interface {
	proposals() (map[string]Event, map[string]Proposal)
}

// EffectAtom defines an atom deriving an Output; it is Atom with the result type spelled out.
func EffectAtom[V any](read func(g reactive.Getter) Output[V]) reactive.Atom[Output[V]] {
	return reactive.New(read)
}

// Proposal is an act request or a cancellation.
type Proposal interface {
	isProposal()
}

type actProposal struct {
	act         string
	input       json.RawMessage
	origin      *int64
	onRequested func(Ref) []Event
	onDeferred  func(Handle, Ref) []Event
	onFinished  func(json.RawMessage, *Failure, *Handle, Ref) []Event
}

type cancelProposal struct {
	ref    Ref
	reason string
}

func (*actProposal) isProposal()   {}
func (cancelProposal) isProposal() {}

// Cancel proposes durable cancellation of accepted work.
func Cancel(ref Ref, reason string) Proposal {
	return cancelProposal{ref: ref, reason: reason}
}

// Act declares a kind of durable work with input I and result O, such as a model call or a tool run. An actor proposes acts from its state; the runtime journals each accepted invocation before running it with the Handler a host supplies, so a handler must tolerate redelivery with the same ref after a crash.
type Act[I, O any] struct {
	name string
}

// NewAct declares an act. The name identifies the act in journals and selects its handler.
func NewAct[I, O any](name string) *Act[I, O] {
	if name == "" {
		panic(fmt.Errorf("tardigrade: act name must not be empty"))
	}
	return &Act[I, O]{name: name}
}

// Name returns the act's name.
func (a *Act[I, O]) Name() string { return a.name }

// Request describes one invocation. Origin is the journal position of the event that made it ready; distinct invocations of one act by one actor need distinct origins, and a nil origin identifies a startup invocation. Callbacks derive domain events committed with each lifecycle step and must be pure.
type Request[I, O any] struct {
	Origin      *int64
	Input       I
	OnRequested func(ref Ref) []Event
	OnDeferred  func(handle Handle, ref Ref) []Event
	OnFinished  func(outcome Outcome[O], ref Ref) []Event
}

// Outcome is how an invocation finished: Value when Err is nil. Handle is set when the value arrived through a deferred promise.
type Outcome[O any] struct {
	Value  O
	Err    *Failure
	Handle *Handle
}

// Request proposes an invocation of the act.
func (a *Act[I, O]) Request(r Request[I, O]) Proposal {
	input, err := json.Marshal(r.Input)
	if err != nil {
		panic(fmt.Errorf("tardigrade: encode %s input: %w", a.name, err))
	}
	p := &actProposal{act: a.name, input: input, origin: r.Origin, onRequested: r.OnRequested, onDeferred: r.OnDeferred}
	if r.OnFinished != nil {
		p.onFinished = func(value json.RawMessage, failure *Failure, handle *Handle, ref Ref) []Event {
			var v O
			if failure == nil && len(value) > 0 {
				if err := json.Unmarshal(value, &v); err != nil {
					panic(fmt.Errorf("tardigrade: decode %s result: %w", a.name, err))
				}
			}
			return r.OnFinished(Outcome[O]{Value: v, Err: failure, Handle: handle}, ref)
		}
	}
	return p
}

// NoDeadline disables a timeout wherever a duration bounds waiting.
const NoDeadline time.Duration = -1

// PromiseOptions bound waiting for deferred work. A zero Timeout inherits the runtime's Policy.PromiseTimeout.
type PromiseOptions struct {
	Timeout time.Duration
}

// Result is an act's immediate value or a handle to an eventual one.
type Result[O any] struct {
	value   O
	handle  *Handle
	fork    func(context.Context) (json.RawMessage, error)
	options PromiseOptions
}

// Value returns an immediate result.
func Value[O any](v O) Result[O] {
	return Result[O]{value: v}
}

// Defer returns a handle whose executor finishes the work later. Clock handles finish at Handle.At with Handle.Value; other executors are watched by the host's Promises service.
func Defer[O any](handle Handle, options PromiseOptions) Result[O] {
	return Result[O]{handle: &handle, options: options}
}

// Fork returns a local promise that work finishes after the acceptance is recorded. After a crash the runtime runs the act again with the same ref, so the act calls Fork again to recreate the producer.
func Fork[O any](x *Execution, options PromiseOptions, work func(ctx context.Context) (O, error)) Result[O] {
	handle := Handle{Executor: ExecutorLocal, ID: x.ref.Key()}
	return Result[O]{handle: &handle, options: options, fork: func(ctx context.Context) (json.RawMessage, error) {
		value, err := work(ctx)
		if err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}}
}

type rawResult struct {
	value   json.RawMessage
	handle  *Handle
	fork    func(context.Context) (json.RawMessage, error)
	options PromiseOptions
}

// Cleanup describes accepted work whose cancellation was recorded. Handle is set when the work had submitted a promise.
type Cleanup struct {
	Context context.Context
	Ref     Ref
	Reason  string
	Handle  *Handle
}

// Handler runs invocations of one act.
type Handler struct {
	act    string
	run    func(x *Execution, input json.RawMessage) (rawResult, error)
	cancel func(c Cleanup, input json.RawMessage) error
}

// Handle returns the Handler that runs the act with run. cancel releases external resources after cancellation and may be nil; it must tolerate repeated calls after a crash.
func (a *Act[I, O]) Handle(run func(x *Execution, in I) (Result[O], error), cancel func(c Cleanup, in I) error) Handler {
	handler := Handler{act: a.name, run: func(x *Execution, input json.RawMessage) (rawResult, error) {
		var in I
		if err := json.Unmarshal(input, &in); err != nil {
			return rawResult{}, fmt.Errorf("decode %s input: %w", a.name, err)
		}
		result, err := run(x, in)
		if err != nil {
			return rawResult{}, err
		}
		if result.handle != nil {
			return rawResult{handle: result.handle, fork: result.fork, options: result.options}, nil
		}
		value, err := json.Marshal(result.value)
		if err != nil {
			return rawResult{}, fmt.Errorf("encode %s result: %w", a.name, err)
		}
		return rawResult{value: value}, nil
	}}
	if cancel != nil {
		handler.cancel = func(c Cleanup, input json.RawMessage) error {
			var in I
			if err := json.Unmarshal(input, &in); err != nil {
				return fmt.Errorf("decode %s input: %w", a.name, err)
			}
			return cancel(c, in)
		}
	}
	return handler
}

// Act returns the name of the act the handler runs.
func (i Handler) Act() string { return i.act }
