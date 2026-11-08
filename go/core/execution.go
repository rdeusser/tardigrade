package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/rdeusser/tardigrade/go/reactive"
)

// Execution is the capability set an act implementation receives for one accepted invocation.
type Execution struct {
	rt      *Runtime
	ref     Ref
	ctx     context.Context
	timeout time.Duration

	mu       sync.Mutex
	disarmed bool
	stopTime func() bool
	attempt  string
	sequence int
	prior    *RetryScheduled
}

func newExecution(rt *Runtime, op *operation, base context.Context) *Execution {
	ctx, cancel := context.WithCancelCause(base)
	x := &Execution{rt: rt, ref: op.ref, ctx: ctx, timeout: rt.policy.AttemptTimeout, attempt: attemptID(), prior: op.retry}
	if x.timeout > 0 {
		x.stopTime = rt.clock.AfterFunc(x.timeout, func() { cancel(errAttemptTimeout) })
	}
	context.AfterFunc(base, func() { cancel(errCancelled) })
	return x
}

func attemptID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// disarm releases the execution-wide deadline, which a Retry boundary replaces with per-attempt deadlines.
func (x *Execution) disarm() {
	x.mu.Lock()
	defer x.mu.Unlock()
	if !x.disarmed && x.stopTime != nil {
		x.stopTime()
	}
	x.disarmed = true
}

// Ref returns the accepted work's identity.
func (x *Execution) Ref() Ref { return x.ref }

// Context is cancelled when the work is cancelled, the actor stops, or the attempt deadline passes.
func (x *Execution) Context() context.Context { return x.ctx }

// Address returns the thread executing the work.
func (x *Execution) Address() ThreadCoordinate { return x.rt.address }

// Publish sends a transient update, such as streamed text, to execution stream observers.
func (x *Execution) Publish(payload any) {
	if x.rt.opts.Publish == nil || x.ctx.Err() != nil {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	x.mu.Lock()
	update := Update{Address: x.rt.address, Ref: x.ref, AttemptID: x.attempt, Sequence: x.sequence, Payload: data}
	x.sequence++
	x.mu.Unlock()
	x.rt.opts.Publish(update)
}

// Record commits domain events on behalf of the work; they are discarded once the work is cancelled.
func (x *Execution) Record(events ...Event) error {
	rt := x.rt
	rt.mu.Lock()
	defer rt.mu.Unlock()
	op := rt.ops[x.ref.Key()]
	if op == nil || op.cancel != nil {
		return nil
	}
	entries := make([]Record, 0, len(events))
	for _, e := range events {
		if isCore(e) {
			return fmt.Errorf("Record accepts domain events, got %s", e.EventType())
		}
		entries = append(entries, Record{Event: e, RecordedAt: rt.now()})
	}
	if err := rt.commit(entries); err != nil {
		return err
	}
	rt.drive()
	return nil
}

// Cancel requests cancellation of other accepted work.
func (x *Execution) Cancel(ref Ref, reason string) error { return x.rt.Cancel(ref, reason) }

// State reads an atom from the thread's current state.
func State[T any](x *Execution, atom reactive.Atom[T]) T {
	x.rt.mu.Lock()
	defer x.rt.mu.Unlock()
	return reactive.Read(x.rt.registry, atom)
}

// RetryDecision schedules the next attempt after Delay.
type RetryDecision struct {
	Delay  time.Duration
	Reason string
}

// RetryPolicy decides whether a failed attempt runs again. A zero AttemptTimeout inherits Policy.AttemptTimeout; NoDeadline disables it.
type RetryPolicy struct {
	AttemptTimeout time.Duration
	// Decide returns nil to stop retrying; attempt counts prior retries.
	Decide func(err error, attempt int) *RetryDecision
}

// Retry runs op until it succeeds or policy declines another attempt. Each scheduled attempt is journaled, so a recovered execution resumes its attempt count and absolute wake time. An execution supports one Retry boundary.
func Retry[T any](x *Execution, policy RetryPolicy, op func(ctx context.Context) (T, error)) (T, error) {
	var zero T
	x.mu.Lock()
	if x.disarmed && x.stopTime == nil {
		x.mu.Unlock()
		return zero, errors.New("an execution supports one retry boundary")
	}
	x.mu.Unlock()
	x.disarm()
	x.mu.Lock()
	x.stopTime = nil
	x.mu.Unlock()
	timeout := policy.AttemptTimeout
	if timeout == 0 {
		timeout = x.rt.policy.AttemptTimeout
	}
	attempt, dueAt := 0, int64(0)
	if x.prior != nil {
		attempt, dueAt = x.prior.Attempt, x.prior.DueAt
	}
	for {
		if !sleepUntil(x.ctx, x.rt.clock, dueAt) {
			return zero, context.Cause(x.ctx)
		}
		ctx, cancel := context.WithCancelCause(x.ctx)
		stop := func() bool { return false }
		if timeout > 0 {
			stop = x.rt.clock.AfterFunc(timeout, func() { cancel(errAttemptTimeout) })
		}
		value, err := op(ctx)
		timedOut := errors.Is(context.Cause(ctx), errAttemptTimeout)
		stop()
		cancel(nil)
		if err == nil {
			return value, nil
		}
		if timedOut {
			err = Reason{Kind: ReasonAttemptTimeout, Message: fmt.Sprintf("attempt timed out after %s", timeout)}
		}
		if x.ctx.Err() != nil || policy.Decide == nil {
			return zero, err
		}
		decision := policy.Decide(err, attempt)
		if decision == nil {
			return zero, err
		}
		attempt++
		dueAt = x.rt.now() + max(decision.Delay, 0).Milliseconds()
		if rerr := x.recordRetry(RetryScheduled{Ref: x.ref, Attempt: attempt, DueAt: dueAt, Reason: decision.Reason}); rerr != nil {
			return zero, rerr
		}
		x.mu.Lock()
		x.attempt, x.sequence = attemptID(), 0
		x.mu.Unlock()
	}
}

func (x *Execution) recordRetry(e RetryScheduled) error {
	rt := x.rt
	rt.mu.Lock()
	defer rt.mu.Unlock()
	op := rt.ops[x.ref.Key()]
	if op == nil || op.cancel != nil {
		return errCancelled
	}
	return rt.commit([]Record{{Event: e, RecordedAt: rt.now()}})
}
