package core

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

// Handler folds one event type into a durable atom's state.
type Handler[S any] struct {
	event  string
	reduce func(S, Event, Meta) S
}

// On returns a handler for events of type E and registers E for journal decoding.
func On[S any, E Event](reduce func(state S, event E, meta Meta) S) Handler[S] {
	return Handler[S]{event: RegisterEvent[E](), reduce: func(s S, e Event, m Meta) S { return reduce(s, e.(E), m) }}
}

type durableSpec[S any] struct {
	name     string
	initial  S
	from     int64
	handlers map[string][]func(S, Event, Meta) S
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

func newSpec[S any](name string, initial S, from int64, handlers []Handler[S]) *durableSpec[S] {
	if name == "" {
		panic(fmt.Errorf("core: durable atom name must not be empty"))
	}
	spec := &durableSpec[S]{name: name, initial: initial, from: from, handlers: map[string][]func(S, Event, Meta) S{}}
	for _, h := range handlers {
		spec.handlers[h.event] = append(spec.handlers[h.event], h.reduce)
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
			for _, reduce := range spec.handlers[record.Event.EventType()] {
				state = reduce(state, record.Event, Meta{Position: from + int64(i), RecordedAt: record.RecordedAt, Message: record.Message})
			}
		}
	}
	return &durableFrame[S]{name: spec.name, source: view.source, position: max(view.length, from), state: state}
}

// DurableAtom folds journal events into state with its handlers. Names identify checkpointed state and must be unique within an actor. Reducers must return new values instead of mutating their input.
func DurableAtom[S any](name string, initial S, handlers ...Handler[S]) reactive.Atom[S] {
	spec := newSpec(name, initial, 0, handlers)
	frame := reactive.New(spec.fold)
	return reactive.New(func(g reactive.Getter) S { return reactive.Get(g, frame).state })
}

// DurableFamily defines durable atoms per key. Each member folds from the position from returns, so a member created late reads only the journal suffix that can affect it.
func DurableFamily[K comparable, S any](name func(K) string, initial S, from func(K) int64, handlers func(K) []Handler[S]) func(K) reactive.Atom[S] {
	specs := reactive.Family(func(key K, _ reactive.Getter) *durableSpec[S] {
		return newSpec(name(key), initial, from(key), handlers(key))
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

// Get reads an atom inside an atom definition, method result, or act implementation.
func Get[T any](g reactive.Getter, a reactive.Atom[T]) T {
	return reactive.Get(g, a)
}

// PromiseStatus is the settlement state of deferred work.
type PromiseStatus struct {
	Status string          `json:"status"`
	Value  json.RawMessage `json:"value,omitempty"`
	Reason *Reason         `json:"reason,omitempty"`
}

// Promise status values.
const (
	PromisePending   = "pending"
	PromiseFulfilled = "fulfilled"
	PromiseRejected  = "rejected"
)

// PromiseOf returns the durable settlement state of the work a ref identifies; cancellation of pending work rejects it.
var PromiseOf = DurableFamily(
	func(ref Ref) string { return "promise:" + ref.Key() },
	PromiseStatus{Status: PromisePending},
	func(ref Ref) int64 { return ref.Seq },
	func(ref Ref) []Handler[PromiseStatus] {
		return []Handler[PromiseStatus]{
			On(func(s PromiseStatus, e PromiseSettled, _ Meta) PromiseStatus {
				if e.Ref != ref || s.Status != PromisePending {
					return s
				}
				if e.Reason != nil {
					return PromiseStatus{Status: PromiseRejected, Reason: e.Reason}
				}
				return PromiseStatus{Status: PromiseFulfilled, Value: e.Value}
			}),
			On(func(s PromiseStatus, e EffectCancelled, _ Meta) PromiseStatus {
				if e.Ref != ref || s.Status != PromisePending {
					return s
				}
				return PromiseStatus{Status: PromiseRejected, Reason: &Reason{Kind: ReasonCancelled, Message: e.Reason}}
			}),
		}
	},
)
