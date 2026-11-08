package tardigrade

import (
	"encoding/json"
	"fmt"

	"github.com/rdeusser/tardigrade/go/reactive"
)

// logView is an immutable view of a thread journal. Records before tailStart are read from the journal on demand.
type logView struct {
	source    *journalSource
	length    int64
	tailStart int64
	tail      []Record
	seeds     map[string]AtomState
}

type journalSource struct {
	journal Journal
}

func (v *logView) records(from, to int64) []Record {
	if from >= v.tailStart {
		return v.tail[from-v.tailStart : to-v.tailStart]
	}
	end := min(to, v.tailStart)
	prefix, err := v.source.journal.Read(from, end)
	if err != nil {
		panic(fmt.Errorf("read journal [%d, %d): %w", from, end, err))
	}
	if to <= v.tailStart {
		return prefix
	}
	return append(prefix, v.tail[:to-v.tailStart]...)
}

// logAtom holds the runtime's current journal view; it is nil outside a runtime.
var logAtom = reactive.Value[*logView](nil)

// Transition is how a durable atom's state changes when one event type arrives. On builds it; the atom routes each journal record to the transitions declared for its event type.
type Transition[S any] struct {
	event string
	apply func(S, Event, Meta) S
}

// On returns the transition that apply makes for events of type E and registers E for journal decoding. apply returns the next state and must not mutate the state it receives.
func On[S any, E Event](apply func(state S, event E, meta Meta) S) Transition[S] {
	return Transition[S]{event: RegisterEvent[E](), apply: func(s S, e Event, m Meta) S { return apply(s, e.(E), m) }}
}

type durableSpec[S any] struct {
	name        string
	initial     S
	from        int64
	transitions map[string][]func(S, Event, Meta) S
}

type durableFrame[S any] struct {
	name     string
	source   *journalSource
	position int64
	state    S
}

type checkpointer interface {
	checkpoint() (string, *journalSource, AtomState, error)
}

func (f *durableFrame[S]) checkpoint() (string, *journalSource, AtomState, error) {
	state, err := json.Marshal(f.state)
	return f.name, f.source, AtomState{Position: f.position, State: state}, err
}

func newSpec[S any](name string, initial S, from int64, transitions []Transition[S]) *durableSpec[S] {
	if name == "" {
		panic(fmt.Errorf("tardigrade: durable atom name must not be empty"))
	}
	spec := &durableSpec[S]{name: name, initial: initial, from: from, transitions: map[string][]func(S, Event, Meta) S{}}
	for _, t := range transitions {
		spec.transitions[t.event] = append(spec.transitions[t.event], t.apply)
	}
	return spec
}

func (spec *durableSpec[S]) fold(g reactive.Getter) *durableFrame[S] {
	view := reactive.Get(g, logAtom)
	if view == nil {
		return &durableFrame[S]{name: spec.name, state: spec.initial}
	}
	state, from := spec.initial, spec.from
	if prior, ok := reactive.Previous[*durableFrame[S]](g); ok && prior.source == view.source && prior.position <= view.length {
		state, from = prior.state, prior.position
	} else if seed, ok := view.seeds[spec.name]; ok && seed.Position <= view.length && seed.Position >= spec.from {
		var restored S
		if err := json.Unmarshal(seed.State, &restored); err != nil {
			panic(fmt.Errorf("restore durable atom %s: %w", spec.name, err))
		}
		state, from = restored, seed.Position
	}
	if from < view.length {
		for i, record := range view.records(from, view.length) {
			for _, apply := range spec.transitions[record.Event.EventType()] {
				state = apply(state, record.Event, Meta{Position: from + int64(i), RecordedAt: record.RecordedAt, Message: record.Message})
			}
		}
	}
	return &durableFrame[S]{name: spec.name, source: view.source, position: max(view.length, from), state: state}
}

// DurableAtom folds journal events into state by applying its transitions in journal order. Names identify checkpointed state and must be unique within an actor.
func DurableAtom[S any](name string, initial S, transitions ...Transition[S]) reactive.Atom[S] {
	spec := newSpec(name, initial, 0, transitions)
	frame := reactive.New(spec.fold)
	return reactive.New(func(g reactive.Getter) S { return reactive.Get(g, frame).state })
}

// DurableFamily defines durable atoms per key. Each member folds from the position from returns, so a member created late reads only the journal suffix that can affect it.
func DurableFamily[K comparable, S any](name func(K) string, initial S, from func(K) int64, transitions func(K) []Transition[S]) func(K) reactive.Atom[S] {
	specs := reactive.Family(func(key K, _ reactive.Getter) *durableSpec[S] {
		return newSpec(name(key), initial, from(key), transitions(key))
	})
	frames := reactive.Family(func(key K, g reactive.Getter) *durableFrame[S] {
		return reactive.Get(g, specs(key)).fold(g)
	})
	return reactive.Family(func(key K, g reactive.Getter) S { return reactive.Get(g, frames(key)).state })
}

// Atom defines a derived atom; it is reactive.New under the name used throughout actor definitions.
func Atom[T any](read func(g reactive.Getter) T) reactive.Atom[T] {
	return reactive.New(read)
}

// Get reads an atom inside an atom definition, method result, or act handler.
func Get[T any](g reactive.Getter, a reactive.Atom[T]) T {
	return reactive.Get(g, a)
}

// PromiseState is whether deferred work finished and how: Value on success, Err on failure.
type PromiseState struct {
	Finished bool            `json:"finished"`
	Value    json.RawMessage `json:"value,omitempty"`
	Err      *Failure        `json:"error,omitempty"`
}

// PromiseOf returns the durable state of the deferred work a ref identifies; cancelling unfinished work finishes it with a FailureCancelled error.
var PromiseOf = DurableFamily(
	func(ref Ref) string { return "promise:" + ref.Key() },
	PromiseState{},
	func(ref Ref) int64 { return ref.Seq },
	func(ref Ref) []Transition[PromiseState] {
		return []Transition[PromiseState]{
			On(func(s PromiseState, e PromiseFinished, _ Meta) PromiseState {
				if e.Ref != ref || s.Finished {
					return s
				}
				return PromiseState{Finished: true, Value: e.Value, Err: e.Err}
			}),
			On(func(s PromiseState, e EffectCancelled, _ Meta) PromiseState {
				if e.Ref != ref || s.Finished {
					return s
				}
				return PromiseState{Finished: true, Err: &Failure{Kind: FailureCancelled, Message: e.Reason}}
			}),
		}
	},
)
