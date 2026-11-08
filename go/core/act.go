package core

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
	onSettled   func(json.RawMessage, *Reason, *Handle, Ref) []Event
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

// At returns a pointer to a journal position for Request.Origin.
func At(position int64) *int64 {
	return &position
}

// Act declares typed durable work whose implementation a host supplies. Implementations must tolerate redelivery with the same ref after a crash.
type Act[I, O any] struct {
	name string
}

// NewAct declares an act. The name identifies the act in journals and selects its implementation.
func NewAct[I, O any](name string) *Act[I, O] {
	if name == "" {
		panic(fmt.Errorf("core: act name must not be empty"))
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
	OnSettled   func(outcome Outcome[O], ref Ref) []Event
}

// Outcome is a settled invocation: Value when Reason is nil. Handle is set when the value arrived through a deferred promise.
type Outcome[O any] struct {
	Value  O
	Reason *Reason
	Handle *Handle
}

// Request proposes an invocation of the act.
func (a *Act[I, O]) Request(r Request[I, O]) Proposal {
	input, err := json.Marshal(r.Input)
	if err != nil {
		panic(fmt.Errorf("core: encode %s input: %w", a.name, err))
	}
	p := &actProposal{act: a.name, input: input, origin: r.Origin, onRequested: r.OnRequested, onDeferred: r.OnDeferred}
	if r.OnSettled != nil {
		p.onSettled = func(value json.RawMessage, reason *Reason, handle *Handle, ref Ref) []Event {
			var v O
			if reason == nil && len(value) > 0 {
				if err := json.Unmarshal(value, &v); err != nil {
					panic(fmt.Errorf("core: decode %s result: %w", a.name, err))
				}
			}
			return r.OnSettled(Outcome[O]{Value: v, Reason: reason, Handle: handle}, ref)
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

// Defer returns a handle whose executor settles the result later. Clock handles settle at Handle.At with Handle.Value; other executors are watched by the host's Promises service.
func Defer[O any](handle Handle, options PromiseOptions) Result[O] {
	return Result[O]{handle: &handle, options: options}
}

// Fork returns a local promise that work settles after the acceptance is recorded. After a crash the runtime runs the act again with the same ref, so the act calls Fork again to recreate the producer.
func Fork[O any](x *Execution, options PromiseOptions, work func(ctx context.Context) (O, error)) Result[O] {
	handle := Handle{Executor: "local", ID: x.ref.Key()}
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

// Implementation executes one act for a runtime.
type Implementation struct {
	act    string
	run    func(x *Execution, input json.RawMessage) (rawResult, error)
	cancel func(c Cleanup, input json.RawMessage) error
}

// Implement binds run to the act. cancel releases external resources after cancellation and may be nil; it must tolerate repeated calls after a crash.
func (a *Act[I, O]) Implement(run func(x *Execution, in I) (Result[O], error), cancel func(c Cleanup, in I) error) Implementation {
	impl := Implementation{act: a.name, run: func(x *Execution, input json.RawMessage) (rawResult, error) {
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
		impl.cancel = func(c Cleanup, input json.RawMessage) error {
			var in I
			if err := json.Unmarshal(input, &in); err != nil {
				return fmt.Errorf("decode %s input: %w", a.name, err)
			}
			return cancel(c, in)
		}
	}
	return impl
}

// Act returns the name of the act an implementation serves.
func (i Implementation) Act() string { return i.act }
