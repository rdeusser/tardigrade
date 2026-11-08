package tardigrade

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/rdeusser/tardigrade/go/reactive"
)

// Address returns the thread this runtime serves.
func (rt *Runtime) Address() ThreadCoordinate { return rt.address }

// Actor returns the actor definition.
func (rt *Runtime) Actor() *Actor { return rt.opts.Actor }

func canonical(input json.RawMessage) (json.RawMessage, error) {
	var value any
	if err := json.Unmarshal(input, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func (rt *Runtime) usable() error {
	if rt.closed {
		return ErrClosed
	}
	if rt.failure != nil {
		return rt.failure
	}
	if rt.defect != nil {
		return &StoppedError{Diagnostic: rt.defect.Diagnostic}
	}
	return nil
}

// Invoke records a method call under a caller-chosen ID. Repeating a call with the same ID, method, input, and sender is absorbed; reusing the ID otherwise fails with ErrMessageConflict.
func (rt *Runtime) Invoke(method, id string, from Sender, input json.RawMessage) error {
	m, ok := rt.opts.Actor.methods[method]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownMethod, method)
	}
	if id == "" {
		return &InvalidInputError{Err: errors.New("call ID must not be empty")}
	}
	normalized, err := canonical(input)
	if err != nil {
		return &InvalidInputError{Err: err}
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if err := rt.usable(); err != nil {
		return err
	}
	if prior, _, err := rt.opts.Journal.Message(id); err != nil {
		return err
	} else if prior != nil {
		same := prior.Message.Invocation != nil && prior.Message.Invocation.Method == method && bytes.Equal(prior.Message.Invocation.Input, normalized) && prior.Message.From == from
		if !same {
			return ErrMessageConflict
		}
		return nil
	}
	event, err := protectEvent(func() (Event, error) { return m.receive(normalized, MethodContext{ID: id, Method: method}) })
	if err != nil {
		return &InvalidInputError{Err: err}
	}
	if event == nil || isCore(event) {
		return &InvalidInputError{Err: fmt.Errorf("method %s must record a domain event", method)}
	}
	message := &Message{ID: id, From: from, Invocation: &Invocation{Method: method, Input: normalized}}
	if err := rt.commit([]Record{{Event: event, RecordedAt: rt.now(), Message: message}}); err != nil {
		return &InvalidInputError{Err: err}
	}
	rt.drive()
	return nil
}

func protectEvent(run func() (Event, error)) (event Event, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	return run()
}

// Call returns the state of a method call.
func (rt *Runtime) Call(method, id string) (MethodState, error) {
	m, ok := rt.opts.Actor.methods[method]
	if !ok {
		return MethodState{}, fmt.Errorf("%w: %s", ErrUnknownMethod, method)
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return MethodState{}, ErrClosed
	}
	record, _, err := rt.opts.Journal.Message(id)
	if err != nil {
		return MethodState{}, err
	}
	if record == nil || record.Message.Invocation == nil || record.Message.Invocation.Method != method {
		return MethodState{}, fmt.Errorf("%w: %s/%s", ErrUnknownCall, method, id)
	}
	state, err := func() (state *MethodState, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = panicError(r)
			}
		}()
		return m.result(record.Message.Invocation.Input, rt.registry, MethodContext{ID: id, Method: method})
	}()
	if err != nil {
		return MethodState{}, err
	}
	if state != nil {
		return *state, nil
	}
	if rt.defect != nil {
		return MethodState{Status: StatusFailed, Error: rt.defect.Diagnostic}, nil
	}
	return MethodState{Status: StatusPending}, nil
}

// CancelCall asks a cancellable method call to stop. The request is recorded under the message ID "cancel:" + id, so repeating it is absorbed.
func (rt *Runtime) CancelCall(method, id, reason string, from Sender) error {
	m, ok := rt.opts.Actor.methods[method]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknownMethod, method)
	}
	if m.cancel == nil {
		return ErrNotCancellable
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if err := rt.usable(); err != nil {
		return err
	}
	original, _, err := rt.opts.Journal.Message(id)
	if err != nil {
		return err
	}
	if original == nil || original.Message.Invocation == nil || original.Message.Invocation.Method != method {
		return fmt.Errorf("%w: %s/%s", ErrUnknownCall, method, id)
	}
	if original.Message.From != from {
		return &InvalidInputError{Err: errors.New("cancellation requires the invoking sender")}
	}
	cancelID := "cancel:" + id
	if prior, _, err := rt.opts.Journal.Message(cancelID); err != nil || prior != nil {
		return err
	}
	event, err := protectEvent(func() (Event, error) {
		return m.cancel(original.Message.Invocation.Input, MethodContext{ID: id, Method: method}, reason)
	})
	if err != nil {
		return &InvalidInputError{Err: err}
	}
	if err := rt.commit([]Record{{Event: event, RecordedAt: rt.now(), Message: &Message{ID: cancelID, From: from}}}); err != nil {
		return &InvalidInputError{Err: err}
	}
	rt.drive()
	return nil
}

// Send records domain events directly, as channels and interventions do.
func (rt *Runtime) Send(events ...Event) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if err := rt.usable(); err != nil {
		return err
	}
	entries := make([]Record, 0, len(events))
	for _, e := range events {
		if e == nil || isCore(e) {
			return &InvalidInputError{Err: errors.New("Send accepts domain events")}
		}
		entries = append(entries, Record{Event: e, RecordedAt: rt.now()})
	}
	if err := rt.commit(entries); err != nil {
		return &InvalidInputError{Err: err}
	}
	rt.drive()
	return nil
}

// Cancel records cancellation of accepted work that has not finished; finished or unknown work is left as is.
func (rt *Runtime) Cancel(ref Ref, reason string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return ErrClosed
	}
	op := rt.ops[ref.Key()]
	if op == nil || op.cancel != nil || op.executionDone() {
		return nil
	}
	if err := rt.commit([]Record{{Event: EffectCancelled{Ref: ref, Reason: reason}, RecordedAt: rt.now()}}); err != nil {
		return err
	}
	rt.drive()
	return nil
}

// Read returns an atom's value in the thread's current state.
func Read[T any](rt *Runtime, atom reactive.Atom[T]) T {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return reactive.Read(rt.registry, atom)
}

// Records returns journal records in [from, to); to beyond the length is clamped.
func (rt *Runtime) Records(from, to int64) ([]Record, error) {
	rt.mu.Lock()
	length := rt.view.length
	rt.mu.Unlock()
	to = min(to, length)
	if from >= to {
		return nil, nil
	}
	return rt.opts.Journal.Read(from, to)
}

// Length returns the number of committed records.
func (rt *Runtime) Length() int64 {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.view.length
}

// Changed returns a channel closed at the next commit or reported problem.
func (rt *Runtime) Changed() <-chan struct{} {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.changed
}

// Status summarizes a runtime for supervision.
type Status struct {
	// Active counts accepted work that has not completed.
	Active int
	// Running counts executions, producers, and cleanups in progress.
	Running  int
	Defect   string
	Problems []error
}

// Status reports the runtime's work and failures.
func (rt *Runtime) Status() Status {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	s := Status{Active: len(rt.ops), Running: len(rt.tasks), Problems: append([]error(nil), rt.problems...)}
	if rt.failure != nil {
		s.Problems = append(s.Problems, rt.failure)
	}
	if rt.defect != nil {
		s.Defect = rt.defect.Diagnostic
	}
	return s
}

// Wait blocks until no execution, producer, or cleanup is running and returns the first reported problem. Work waiting on timers or external promises does not hold Wait.
func (rt *Runtime) Wait(ctx context.Context) error {
	for {
		rt.mu.Lock()
		running, changed := len(rt.tasks), rt.changed
		var problem error
		if rt.failure != nil {
			problem = rt.failure
		} else if len(rt.problems) > 0 {
			problem = rt.problems[0]
		}
		rt.mu.Unlock()
		if running == 0 {
			return problem
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Close stops all work and timers. Unfinished work resumes when the journal is opened again.
func (rt *Runtime) Close() error {
	rt.mu.Lock()
	if rt.closed {
		rt.mu.Unlock()
		return nil
	}
	rt.closed = true
	for key, stop := range rt.timers {
		stop()
		delete(rt.timers, key)
	}
	rt.stop()
	rt.notify()
	rt.mu.Unlock()
	rt.group.Wait()
	return nil
}
