package core

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/rdeusser/tardigrade/go/reactive"
)

// Policy defaults applied when a Policy field is zero.
const (
	DefaultAttemptTimeout      = 60 * time.Second
	DefaultPromiseTimeout      = 60 * time.Second
	DefaultCancellationRetry   = 5 * time.Second
	DefaultCheckpointEvery     = 500
	DefaultInputDigestMinBytes = 2 * 1024
)

// Policy bounds execution, waiting, and storage. Zero fields take the Default constants; NoDeadline disables a timeout and a negative CheckpointEvery disables checkpoints.
type Policy struct {
	// AttemptTimeout bounds each act execution until the act opens a Retry boundary, which bounds each attempt instead.
	AttemptTimeout time.Duration
	// PromiseTimeout bounds waiting for deferred work that set no timeout of its own.
	PromiseTimeout time.Duration
	// CancellationRetry spaces cleanup attempts after a cancellation handler fails.
	CancellationRetry time.Duration
	// CheckpointEvery saves atom states once this many records accumulate while no work is active.
	CheckpointEvery int64
	// InputDigestMinBytes retains act inputs at least this large beside the journal and records their digest.
	InputDigestMinBytes int
}

func (p Policy) resolved() Policy {
	if p.AttemptTimeout == 0 {
		p.AttemptTimeout = DefaultAttemptTimeout
	}
	if p.PromiseTimeout == 0 {
		p.PromiseTimeout = DefaultPromiseTimeout
	}
	if p.CancellationRetry == 0 {
		p.CancellationRetry = DefaultCancellationRetry
	}
	if p.CheckpointEvery == 0 {
		p.CheckpointEvery = DefaultCheckpointEvery
	}
	if p.InputDigestMinBytes == 0 {
		p.InputDigestMinBytes = DefaultInputDigestMinBytes
	}
	return p
}

// Clock supplies time to a runtime so tests and hosts can control it.
type Clock interface {
	Now() time.Time
	AfterFunc(d time.Duration, f func()) (stop func() bool)
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) AfterFunc(d time.Duration, f func()) func() bool {
	return time.AfterFunc(d, f).Stop
}

// SystemClock is the wall clock.
var SystemClock Clock = systemClock{}

// PromiseWatch asks a host to settle deferred work held by an external executor.
type PromiseWatch struct {
	Address    ThreadCoordinate
	Ref        Ref
	Handle     Handle
	DeadlineAt *int64
}

// Promises settles deferred work for executors other than "local" and "clock" by calling Runtime.Settle.
type Promises interface {
	Watch(w PromiseWatch) error
	// Cancel stops observing; executor cleanup belongs to the act's cancellation handler.
	Cancel(w PromiseWatch) error
}

// Update is a transient execution update, such as streamed model text. Updates are never journaled.
type Update struct {
	Address   ThreadCoordinate `json:"address"`
	Ref       Ref              `json:"ref"`
	AttemptID string           `json:"attemptId"`
	Sequence  int              `json:"sequence"`
	Payload   json.RawMessage  `json:"payload"`
}

// Options configure a runtime.
type Options struct {
	Actor   *Actor
	Journal Journal
	Address ThreadCoordinate
	// Acts implement every act the actor can propose.
	Acts   []Implementation
	Policy Policy
	Clock  Clock
	// Promises settles handles of external executors; nil rejects them.
	Promises Promises
	// Publish receives execution updates.
	Publish func(Update)
	// OnCommit observes each committed batch with the count of unfinished work after it. It runs under the runtime's lock and must not call the runtime.
	OnCommit func(position int64, records []Record, active int)
	// Created is appended as the first record of an empty journal.
	Created *ThreadCreated
}

// Errors returned by runtime operations.
var (
	ErrClosed          = errors.New("actor runtime is closed")
	ErrMessageConflict = errors.New("message identity reused with different input or sender")
	ErrUnknownMethod   = errors.New("unknown method")
	ErrUnknownCall     = errors.New("unknown method call")
	ErrNotCancellable  = errors.New("method does not support cancellation")
	ErrPromiseNotReady = errors.New("promise settlement requires a recorded deferral")
)

// InvalidInputError rejects input that violates a method or event contract before it is journaled.
type InvalidInputError struct{ Err error }

func (e *InvalidInputError) Error() string { return e.Err.Error() }
func (e *InvalidInputError) Unwrap() error { return e.Err }

// StoppedError reports an actor halted by a terminal defect.
type StoppedError struct{ Diagnostic string }

func (e *StoppedError) Error() string { return "actor stopped: " + e.Diagnostic }

type operation struct {
	ref          Ref
	origin       *int64
	request      StoredRequest
	input        json.RawMessage
	proposal     *actProposal
	settled      *EffectSettled
	died         *EffectDied
	promise      *PromiseSettled
	retry        *RetryScheduled
	cancel       *EffectCancelled
	cleanup      Event
	cleanupRetry *RetryScheduled
}

func (op *operation) deferred() *PromiseRecord {
	if op.settled != nil && op.settled.Result != nil {
		return op.settled.Result.Promise
	}
	return nil
}

func (op *operation) executionDone() bool {
	if op.died != nil || op.promise != nil {
		return true
	}
	return op.settled != nil && (op.settled.Reason != nil || op.settled.Result.Promise == nil)
}

func (op *operation) cleanupSucceeded() bool {
	settled, ok := op.cleanup.(CancellationSettled)
	return ok && settled.Failure == ""
}

func (op *operation) complete() bool {
	if op.cancel != nil {
		return op.cleanupSucceeded()
	}
	return op.executionDone()
}

func (op *operation) identity() Identity {
	return Identity{Atom: op.ref.Atom, Act: op.ref.Act, Origin: op.origin}
}

type task struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Runtime runs one thread of an actor: it replays the journal, commits proposals, and executes accepted work. Methods are safe for concurrent use.
type Runtime struct {
	opts    Options
	policy  Policy
	clock   Clock
	impls   map[string]Implementation
	address ThreadCoordinate

	mu        sync.Mutex
	registry  *reactive.Registry
	source    *journalSource
	view      *logView
	ops       map[string]*operation
	defect    *EffectDied
	failure   error
	problems  []error
	closed    bool
	accepting *actProposal

	ctx       context.Context
	stop      context.CancelFunc
	tasks     map[string]*task
	timers    map[string]func() bool
	watched   map[string]bool
	lastSaved int64
	changed   chan struct{}
	group     sync.WaitGroup
}

// Open replays a journal and resumes unfinished work.
func Open(opts Options) (*Runtime, error) {
	if opts.Actor == nil || opts.Journal == nil {
		return nil, errors.New("core: Open requires an actor and a journal")
	}
	rt := &Runtime{
		opts: opts, policy: opts.Policy.resolved(), clock: opts.Clock, address: opts.Address,
		impls: map[string]Implementation{}, registry: reactive.NewRegistry(), source: &journalSource{journal: opts.Journal},
		ops: map[string]*operation{}, tasks: map[string]*task{}, timers: map[string]func() bool{}, watched: map[string]bool{}, changed: make(chan struct{}),
	}
	if rt.clock == nil {
		rt.clock = SystemClock
	}
	if rt.address.Actor == "" {
		rt.address.Actor = opts.Actor.name
	}
	for _, impl := range opts.Acts {
		if _, ok := rt.impls[impl.act]; ok {
			return nil, fmt.Errorf("core: duplicate implementation for act %s", impl.act)
		}
		rt.impls[impl.act] = impl
	}
	rt.ctx, rt.stop = context.WithCancel(context.Background())
	if err := rt.replay(); err != nil {
		rt.stop()
		return nil, err
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.view.length == 0 && opts.Created != nil {
		if err := rt.commit([]Record{{Event: *opts.Created, RecordedAt: rt.now()}}); err != nil {
			rt.stop()
			return nil, err
		}
	}
	rt.recover()
	rt.drive()
	return rt, nil
}

func (rt *Runtime) replay() error {
	checkpoint, err := rt.opts.Journal.Checkpoint()
	if err != nil {
		return err
	}
	length, err := rt.opts.Journal.Length()
	if err != nil {
		return err
	}
	start := int64(0)
	seeds := map[string]AtomState{}
	if checkpoint != nil && checkpoint.Position <= length {
		start, seeds = checkpoint.Position, checkpoint.Atoms
	}
	tail, err := rt.opts.Journal.Read(start, length)
	if err != nil {
		return err
	}
	inputs, err := rt.opts.Journal.Inputs()
	if err != nil {
		return err
	}
	rt.view = &logView{source: rt.source, length: length, tailStart: start, tail: tail, seeds: seeds}
	reactive.Set(rt.registry, logAtom, rt.view)
	rt.lastSaved = start
	for i, record := range tail {
		op, err := rt.apply(record.Event, start+int64(i))
		if err != nil {
			return fmt.Errorf("replay position %d: %w", start+int64(i), err)
		}
		if op != nil && op.complete() {
			delete(rt.ops, op.ref.Key())
		}
	}
	rt.bindRecovered(start, length, tail)
	for key, op := range rt.ops {
		if op.input == nil {
			input, ok := inputs[key]
			if !ok && !(op.cancel != nil && op.cleanup != nil) {
				return fmt.Errorf("replay: retained input for %s is unavailable", key)
			}
			op.input = input
		}
	}
	return nil
}

// bindRecovered binds each unfinished operation to the proposal its actor made at the operation's acceptance, stepping the view through those positions.
func (rt *Runtime) bindRecovered(start, length int64, tail []Record) {
	pending := make([]*operation, 0, len(rt.ops))
	for _, op := range rt.ops {
		pending = append(pending, op)
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].ref.Seq < pending[j].ref.Seq })
	for _, op := range pending {
		view := &logView{source: rt.source, length: op.ref.Seq, tailStart: start, tail: tail[:op.ref.Seq-start], seeds: rt.view.seeds}
		reactive.Set(rt.registry, logAtom, view)
		_ = rt.bind(op)
	}
	reactive.Set(rt.registry, logAtom, rt.view)
}

func (rt *Runtime) now() int64 { return rt.clock.Now().UnixMilli() }

// apply folds a lifecycle event into the operation table and returns the operation it touched.
func (rt *Runtime) apply(e Event, position int64) (*operation, error) {
	ref, ok := lifecycleRef(e)
	if !ok {
		return nil, nil
	}
	key := ref.Key()
	if requested, ok := e.(EffectRequested); ok {
		if _, exists := rt.ops[key]; exists {
			return nil, fmt.Errorf("duplicate acceptance of %s", key)
		}
		if requested.Ref.Seq != position {
			return nil, fmt.Errorf("acceptance of %s is recorded at position %d", key, position)
		}
		op := &operation{ref: requested.Ref, origin: requested.Origin, request: requested.Request, input: requested.Request.Input}
		if rt.accepting != nil {
			op.proposal, op.input = rt.accepting, rt.accepting.input
		}
		rt.ops[key] = op
		return op, nil
	}
	op := rt.ops[key]
	if op == nil {
		return nil, fmt.Errorf("%s refers to unknown work %s", e.EventType(), key)
	}
	switch v := e.(type) {
	case EffectSettled:
		if op.settled != nil {
			return nil, fmt.Errorf("duplicate settlement of %s", key)
		}
		op.settled = &v
	case EffectDied:
		op.died = &v
		if rt.defect == nil {
			rt.defect = &v
		}
	case PromiseSettled:
		if op.deferred() == nil {
			return nil, fmt.Errorf("promise settlement of %s precedes its deferral", key)
		}
		op.promise = &v
	case RetryScheduled:
		prior := op.retry
		if v.Phase == "cancellation" {
			prior = op.cleanupRetry
		}
		if (prior == nil && v.Attempt != 1) || (prior != nil && v.Attempt != prior.Attempt+1) {
			return nil, fmt.Errorf("retry attempt of %s must advance by one", key)
		}
		if v.Phase == "cancellation" {
			op.cleanupRetry = &v
		} else {
			op.retry = &v
		}
	case EffectCancelled:
		op.cancel = &v
	case CancellationSettled, CancellationDied:
		if op.cancel == nil || op.cleanup != nil {
			return nil, fmt.Errorf("cleanup outcome of %s requires one pending cancellation", key)
		}
		op.cleanup = e
	}
	return op, nil
}

// followups derives the domain events an act's callbacks attach to a lifecycle event.
func followups(op *operation, e Event) []Event {
	p := op.proposal
	if p == nil {
		return nil
	}
	if cancelled, ok := e.(EffectCancelled); ok {
		if p.onSettled == nil {
			return nil
		}
		var handle *Handle
		if promise := op.deferred(); promise != nil {
			handle = &promise.Handle
		}
		return p.onSettled(nil, &Reason{Kind: ReasonCancelled, Message: cancelled.Reason}, handle, op.ref)
	}
	if op.cancel != nil {
		return nil
	}
	switch v := e.(type) {
	case EffectRequested:
		if p.onRequested != nil {
			return p.onRequested(op.ref)
		}
	case EffectSettled:
		if v.Reason != nil {
			if p.onSettled != nil {
				return p.onSettled(nil, v.Reason, nil, op.ref)
			}
		} else if v.Result.Promise != nil {
			if p.onDeferred != nil {
				return p.onDeferred(v.Result.Promise.Handle, op.ref)
			}
		} else if p.onSettled != nil {
			return p.onSettled(v.Result.Value, nil, nil, op.ref)
		}
	case PromiseSettled:
		if p.onSettled != nil {
			return p.onSettled(v.Value, v.Reason, &op.deferred().Handle, op.ref)
		}
	}
	return nil
}

func (rt *Runtime) cloneOps() map[string]*operation {
	copied := make(map[string]*operation, len(rt.ops))
	for k, op := range rt.ops {
		duplicate := *op
		copied[k] = &duplicate
	}
	return copied
}

func (rt *Runtime) proposals() (events map[string]Event, acts map[string]Proposal, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	events, acts = reactive.Read(rt.registry, rt.opts.Actor.root).proposals()
	return events, acts, nil
}

func panicError(r any) error {
	if err, ok := r.(error); ok {
		return err
	}
	return fmt.Errorf("%v", r)
}

// bind finds the current proposal for an operation recovered from the journal.
func (rt *Runtime) bind(op *operation) error {
	if op.proposal != nil {
		return nil
	}
	_, acts, err := rt.proposals()
	if err != nil {
		return err
	}
	for _, key := range sortedKeys(acts) {
		p, ok := acts[key].(*actProposal)
		if ok && p.act == op.ref.Act && equalOrigin(p.origin, op.origin) {
			op.proposal = p
			return nil
		}
	}
	return fmt.Errorf("no current proposal matches accepted work %s", op.ref.Key())
}

func equalOrigin(a, b *int64) bool {
	return (a == nil && b == nil) || (a != nil && b != nil && *a == *b)
}

// commit appends entries and their callback events as one journal batch. On failure the registry and operation table are restored.
func (rt *Runtime) commit(entries []Record) error {
	if rt.failure != nil {
		return rt.failure
	}
	if rt.closed {
		return ErrClosed
	}
	snapshot := rt.registry.Snapshot()
	priorOps, priorDefect, priorView := rt.cloneOps(), rt.defect, rt.view
	restore := func() {
		rt.registry.Restore(snapshot)
		rt.ops, rt.defect, rt.view = priorOps, priorDefect, priorView
	}
	records, changes, completed, err := rt.prepare(entries)
	if err != nil {
		restore()
		return err
	}
	if err := rt.opts.Journal.Append(priorView.length, records, changes); err != nil {
		restore()
		if !errors.Is(err, ErrConflict) {
			rt.failure = fmt.Errorf("actor state could not be persisted; reopen the thread: %w", err)
			return rt.failure
		}
		return err
	}
	for _, key := range completed {
		delete(rt.ops, key)
	}
	if rt.opts.OnCommit != nil {
		rt.opts.OnCommit(priorView.length, records, len(rt.ops))
	}
	for _, record := range records {
		rt.react(record.Event)
	}
	rt.notify()
	rt.maybeCheckpoint()
	return nil
}

func (rt *Runtime) prepare(entries []Record) (records []Record, changes Changes, completed []string, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicError(r)
		}
	}()
	position := rt.view.length
	changes.PutInputs = map[string]json.RawMessage{}
	touched := map[string]*operation{}
	for _, entry := range entries {
		op, err := rt.apply(entry.Event, position)
		if err != nil {
			return nil, changes, nil, err
		}
		records = append(records, entry)
		position++
		if op == nil {
			continue
		}
		touched[op.ref.Key()] = op
		if requested, ok := entry.Event.(EffectRequested); ok && requested.Request.Digest != "" {
			changes.PutInputs[op.ref.Key()] = op.input
		}
		if err := rt.bindIfNeeded(op, entry.Event); err != nil {
			return nil, changes, nil, err
		}
		for _, f := range followups(op, entry.Event) {
			if f == nil {
				continue
			}
			if isCore(f) {
				return nil, changes, nil, fmt.Errorf("act callbacks must return domain events, got %s", f.EventType())
			}
			records = append(records, Record{Event: f, RecordedAt: entry.RecordedAt, Message: nil})
			position++
		}
	}
	for key, op := range touched {
		if op.complete() {
			completed = append(completed, key)
			if op.request.Digest != "" {
				changes.RemoveInputs = append(changes.RemoveInputs, key)
			}
		}
	}
	rt.view = &logView{source: rt.source, length: position, tailStart: rt.view.tailStart, tail: append(rt.view.tail, records...), seeds: rt.view.seeds}
	reactive.Set(rt.registry, logAtom, rt.view)
	if _, _, err := rt.proposals(); err != nil {
		return nil, changes, nil, err
	}
	return records, changes, completed, nil
}

// bindIfNeeded binds recovered work before its callbacks run; work no current proposal matches settles without callback events.
func (rt *Runtime) bindIfNeeded(op *operation, e Event) error {
	switch e.(type) {
	case EffectSettled, PromiseSettled, EffectCancelled:
		if op.proposal == nil {
			if err := rt.bind(op); err != nil {
				rt.problems = append(rt.problems, err)
			}
		}
	}
	return nil
}

func (rt *Runtime) notify() {
	close(rt.changed)
	rt.changed = make(chan struct{})
}

// react starts and stops background work after a committed lifecycle event.
func (rt *Runtime) react(e Event) {
	ref, ok := lifecycleRef(e)
	if !ok {
		return
	}
	key := ref.Key()
	switch v := e.(type) {
	case EffectDied:
		for k, t := range rt.tasks {
			if len(k) < 8 || k[:8] != "cleanup:" {
				t.cancel()
			}
		}
		for k, stop := range rt.timers {
			stop()
			delete(rt.timers, k)
		}
	case EffectSettled:
		op := rt.ops[key]
		if op != nil && v.Result != nil && v.Result.Promise != nil && op.cancel == nil {
			rt.watch(op)
		}
	case PromiseSettled:
		rt.stopTimer("deadline:" + key)
		rt.stopTimer("clock:" + key)
		if t := rt.tasks["fork:"+key]; t != nil {
			t.cancel()
		}
	case EffectCancelled:
		rt.stopTimer("deadline:" + key)
		rt.stopTimer("clock:" + key)
		for _, prefix := range []string{"exec:", "fork:"} {
			if t := rt.tasks[prefix+key]; t != nil {
				t.cancel()
			}
		}
		if op := rt.ops[key]; op != nil {
			rt.startCleanup(op)
		}
	}
}

func (rt *Runtime) stopTimer(key string) {
	if stop, ok := rt.timers[key]; ok {
		stop()
		delete(rt.timers, key)
	}
}

func (rt *Runtime) report(err error) {
	if err != nil && !rt.closed {
		rt.problems = append(rt.problems, err)
		rt.notify()
	}
}

// drive commits cancellations, event proposals, and act acceptances until the root proposes nothing new.
func (rt *Runtime) drive() {
	for !rt.closed && rt.failure == nil {
		events, acts, err := rt.proposals()
		if err != nil {
			rt.report(fmt.Errorf("evaluate %s: %w", rt.opts.Actor.name, err))
			return
		}
		if rt.driveCancellation(acts) {
			continue
		}
		if rt.defect != nil {
			return
		}
		if keys := sortedKeys(events); len(keys) > 0 {
			event := events[keys[0]]
			if event == nil || isCore(event) {
				rt.report(fmt.Errorf("event proposal %s must be a domain event", keys[0]))
				return
			}
			if err := rt.commit([]Record{{Event: event, RecordedAt: rt.now()}}); err != nil {
				rt.report(fmt.Errorf("deliver %s: %w", keys[0], err))
				return
			}
			continue
		}
		accepted, err := rt.driveAcceptance(acts)
		if err != nil {
			rt.report(err)
			return
		}
		if !accepted {
			return
		}
	}
}

func (rt *Runtime) driveCancellation(acts map[string]Proposal) bool {
	for _, key := range sortedKeys(acts) {
		c, ok := acts[key].(cancelProposal)
		if !ok {
			continue
		}
		op := rt.ops[c.ref.Key()]
		if op == nil || op.cancel != nil || op.executionDone() {
			continue
		}
		if err := rt.commit([]Record{{Event: EffectCancelled{Ref: c.ref, Reason: c.reason}, RecordedAt: rt.now()}}); err != nil {
			rt.report(fmt.Errorf("cancel %s: %w", c.ref.Key(), err))
			return false
		}
		return true
	}
	return false
}

func (rt *Runtime) driveAcceptance(acts map[string]Proposal) (bool, error) {
	seen := map[string]string{}
	for _, key := range sortedKeys(acts) {
		p, ok := acts[key].(*actProposal)
		if !ok {
			continue
		}
		identity := Identity{Atom: rt.opts.Actor.name, Act: p.act, Origin: p.origin}
		if prior, ok := seen[identity.Key()]; ok {
			return false, fmt.Errorf("proposals %s and %s invoke %s with one origin; distinct invocations need distinct origins", prior, key, p.act)
		}
		seen[identity.Key()] = key
		if op := rt.active(identity); op != nil {
			if op.proposal == nil {
				op.proposal = p
			}
			continue
		}
		prior, err := rt.opts.Journal.Invocation(identity)
		if err != nil {
			return false, err
		}
		if prior != nil {
			continue
		}
		position := rt.view.length
		if p.origin != nil && *p.origin >= position {
			return false, fmt.Errorf("origin %d of %s must precede its acceptance at %d", *p.origin, p.act, position)
		}
		ref := Ref{Seq: position, Atom: rt.opts.Actor.name, Act: p.act}
		request := StoredRequest{Act: p.act, Input: p.input}
		if len(p.input) >= rt.policy.InputDigestMinBytes {
			sum := sha256.Sum256(p.input)
			request = StoredRequest{Act: p.act, Digest: hex.EncodeToString(sum[:])}
		}
		rt.accepting = p
		err = rt.commit([]Record{{Event: EffectRequested{Ref: ref, Origin: p.origin, Request: request}, RecordedAt: rt.now()}})
		rt.accepting = nil
		if err != nil {
			return false, fmt.Errorf("accept %s: %w", p.act, err)
		}
		if op := rt.ops[ref.Key()]; op != nil {
			op.input = p.input
			rt.dispatch(op, false)
		}
		return true, nil
	}
	return false, nil
}

func (rt *Runtime) active(identity Identity) *operation {
	key := identity.Key()
	for _, op := range rt.ops {
		if op.identity().Key() == key {
			return op
		}
	}
	return nil
}

// recover resumes work that was unfinished when the journal was last written.
func (rt *Runtime) recover() {
	for _, key := range sortedKeys(rt.ops) {
		op := rt.ops[key]
		switch {
		case op.cancel != nil:
			if op.cleanup == nil {
				rt.startCleanup(op)
			} else {
				rt.report(fmt.Errorf("cleanup of %s ended without success; its input is retained", key))
			}
		case op.died != nil:
		case op.settled == nil:
			rt.dispatch(op, false)
		case op.deferred() != nil && op.promise == nil && rt.defect == nil:
			if op.deferred().Handle.Executor == "local" {
				rt.dispatch(op, true)
			}
			rt.watch(op)
		}
	}
}

// watch schedules settlement of deferred work by its executor and expiry by its deadline.
func (rt *Runtime) watch(op *operation) {
	promise := op.deferred()
	key := op.ref.Key()
	ref := op.ref
	if promise.DeadlineAt != nil {
		deadline := *promise.DeadlineAt
		rt.schedule("deadline:"+key, deadline, func() {
			rt.settleLocked(ref, nil, &Reason{Kind: ReasonPromiseTimeout, Message: fmt.Sprintf("promise expired at %d", deadline)})
		})
	}
	switch promise.Handle.Executor {
	case "local":
	case "clock":
		value := promise.Handle.Value
		if len(value) == 0 {
			value = json.RawMessage("null")
		}
		rt.schedule("clock:"+key, promise.Handle.At, func() { rt.settleLocked(ref, value, nil) })
	default:
		if rt.watched[key] {
			return
		}
		rt.watched[key] = true
		w := PromiseWatch{Address: rt.address, Ref: ref, Handle: promise.Handle, DeadlineAt: promise.DeadlineAt}
		rt.group.Add(1)
		go func() {
			defer rt.group.Done()
			if rt.opts.Promises == nil {
				_ = rt.Settle(ref, nil, &Reason{Kind: ReasonFailed, Message: "no Promises service settles executor " + w.Handle.Executor})
			} else if err := rt.opts.Promises.Watch(w); err != nil {
				_ = rt.Settle(ref, nil, &Reason{Kind: ReasonFailed, Message: err.Error()})
			}
		}()
	}
}

func (rt *Runtime) schedule(key string, at int64, fire func()) {
	rt.stopTimer(key)
	delay := time.Duration(at-rt.now()) * time.Millisecond
	rt.timers[key] = rt.clock.AfterFunc(max(delay, 0), func() {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		if _, ok := rt.timers[key]; !ok || rt.closed {
			return
		}
		delete(rt.timers, key)
		fire()
	})
}

// settleLocked commits a promise settlement if the work still awaits one; callers hold the lock.
func (rt *Runtime) settleLocked(ref Ref, value json.RawMessage, reason *Reason) {
	op := rt.ops[ref.Key()]
	if op == nil || op.cancel != nil || op.promise != nil || op.deferred() == nil {
		return
	}
	if reason == nil && len(value) == 0 {
		value = json.RawMessage("null")
	}
	if err := rt.commit([]Record{{Event: PromiseSettled{Ref: ref, Value: value, Reason: reason}, RecordedAt: rt.now()}}); err != nil {
		rt.report(fmt.Errorf("settle %s: %w", ref.Key(), err))
		return
	}
	rt.drive()
}

// Settle records the outcome of deferred work held by an external executor. Repeating an identical settlement is a no-op.
func (rt *Runtime) Settle(ref Ref, value json.RawMessage, reason *Reason) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return ErrClosed
	}
	op := rt.ops[ref.Key()]
	if op == nil || op.cancel != nil {
		return nil
	}
	if op.deferred() == nil {
		return ErrPromiseNotReady
	}
	rt.settleLocked(ref, value, reason)
	return nil
}

func (rt *Runtime) startTask(key string) (context.Context, *task, bool) {
	if _, ok := rt.tasks[key]; ok {
		return nil, nil, false
	}
	ctx, cancel := context.WithCancel(rt.ctx)
	t := &task{cancel: cancel, done: make(chan struct{})}
	rt.tasks[key] = t
	rt.group.Add(1)
	return ctx, t, true
}

func (rt *Runtime) endTask(key string, t *task) {
	if rt.tasks[key] == t {
		delete(rt.tasks, key)
	}
	t.cancel()
	close(t.done)
	rt.group.Done()
	rt.notify()
}

var (
	errAttemptTimeout = errors.New("attempt timed out")
	errCancelled      = errors.New("work was cancelled")
)

// dispatch runs an operation's act. Recovering reruns a local deferral so the act recreates its producer.
func (rt *Runtime) dispatch(op *operation, recovering bool) {
	if rt.defect != nil {
		return
	}
	key := "exec:" + op.ref.Key()
	ctx, t, ok := rt.startTask(key)
	if !ok {
		return
	}
	impl, found := rt.impls[op.ref.Act]
	x := newExecution(rt, op, ctx)
	input := op.input
	go func() {
		var result rawResult
		var err error
		var diagnostic string
		if !found {
			diagnostic = "no implementation for act " + op.ref.Act
		} else {
			result, err, diagnostic = protect(func() (rawResult, error) { return impl.run(x, input) })
		}
		x.disarm()
		if err != nil && errors.Is(context.Cause(x.ctx), errAttemptTimeout) {
			err = Reason{Kind: ReasonAttemptTimeout, Message: fmt.Sprintf("attempt timed out after %s", x.timeout)}
		}
		rt.mu.Lock()
		defer rt.mu.Unlock()
		defer rt.endTask(key, t)
		rt.finish(op.ref, result, err, diagnostic, recovering)
	}()
}

func protect[T any](run func() (T, error)) (value T, err error, diagnostic string) {
	defer func() {
		if r := recover(); r != nil {
			diagnostic = fmt.Sprintf("%v\n%s", r, debug.Stack())
		}
	}()
	value, err = run()
	return value, err, ""
}

func (rt *Runtime) finish(ref Ref, result rawResult, err error, diagnostic string, recovering bool) {
	op := rt.ops[ref.Key()]
	if rt.closed || op == nil {
		return
	}
	if diagnostic != "" {
		if op.cancel == nil && rt.defect == nil {
			if cerr := rt.commit([]Record{{Event: EffectDied{Ref: ref, Diagnostic: diagnostic}, RecordedAt: rt.now()}}); cerr != nil {
				rt.report(cerr)
			}
		}
		return
	}
	if op.cancel != nil {
		if err == nil && result.handle != nil && op.settled == nil {
			_ = rt.commit([]Record{{Event: EffectSettled{Ref: ref, Result: &ExecutionResult{Promise: &PromiseRecord{Handle: *result.handle, DeadlineAt: rt.deadline(result)}}}, RecordedAt: rt.now()}})
		}
		return
	}
	if recovering {
		switch {
		case err != nil:
			rt.settleLocked(ref, nil, reasonOf(err))
		case result.fork != nil:
			rt.startFork(op, result.fork)
		case result.handle == nil:
			rt.settleLocked(ref, result.value, nil)
		}
		return
	}
	if op.settled != nil {
		return
	}
	settled := EffectSettled{Ref: ref}
	switch {
	case err != nil:
		settled.Reason = reasonOf(err)
	case result.handle != nil:
		settled.Result = &ExecutionResult{Promise: &PromiseRecord{Handle: *result.handle, DeadlineAt: rt.deadline(result)}}
	default:
		settled.Result = &ExecutionResult{Value: result.value}
	}
	if cerr := rt.commit([]Record{{Event: settled, RecordedAt: rt.now()}}); cerr != nil {
		rt.report(fmt.Errorf("settle %s: %w", ref.Key(), cerr))
		return
	}
	if result.fork != nil && err == nil {
		rt.startFork(op, result.fork)
	}
	rt.drive()
}

func (rt *Runtime) deadline(result rawResult) *int64 {
	timeout := result.options.Timeout
	if timeout == 0 {
		timeout = rt.policy.PromiseTimeout
	}
	if timeout < 0 {
		return nil
	}
	start := rt.now()
	if result.handle.Executor == "clock" && result.handle.At > start {
		start = result.handle.At
	}
	at := start + timeout.Milliseconds()
	return &at
}

func reasonOf(err error) *Reason {
	var reason Reason
	if errors.As(err, &reason) {
		return &reason
	}
	var pointer *Reason
	if errors.As(err, &pointer) && pointer != nil {
		return pointer
	}
	return &Reason{Kind: ReasonFailed, Message: err.Error()}
}

func (rt *Runtime) startFork(op *operation, work func(context.Context) (json.RawMessage, error)) {
	key := "fork:" + op.ref.Key()
	ctx, t, ok := rt.startTask(key)
	if !ok {
		return
	}
	ref := op.ref
	go func() {
		value, err, diagnostic := protect(func() (json.RawMessage, error) { return work(ctx) })
		rt.mu.Lock()
		defer rt.mu.Unlock()
		defer rt.endTask(key, t)
		if ctx.Err() != nil && rt.closed {
			return
		}
		switch {
		case diagnostic != "":
			if current := rt.ops[ref.Key()]; current != nil && current.cancel == nil && rt.defect == nil {
				if cerr := rt.commit([]Record{{Event: EffectDied{Ref: ref, Diagnostic: diagnostic}, RecordedAt: rt.now()}}); cerr != nil {
					rt.report(cerr)
				}
			}
		case err != nil:
			rt.settleLocked(ref, nil, reasonOf(err))
		default:
			rt.settleLocked(ref, value, nil)
		}
	}()
}

// startCleanup runs the act's cancellation handler until it succeeds, retrying at Policy.CancellationRetry.
func (rt *Runtime) startCleanup(op *operation) {
	key := "cleanup:" + op.ref.Key()
	ctx, t, ok := rt.startTask(key)
	if !ok {
		return
	}
	ref, input, reason := op.ref, op.input, op.cancel.Reason
	attempt, dueAt := 0, int64(0)
	if op.cleanupRetry != nil {
		attempt, dueAt = op.cleanupRetry.Attempt, op.cleanupRetry.DueAt
	}
	executing := rt.tasks["exec:"+ref.Key()]
	impl := rt.impls[ref.Act]
	go func() {
		if executing != nil {
			<-executing.done
		}
		var outcome Event
		for outcome == nil {
			if !sleepUntil(ctx, rt.clock, dueAt) {
				break
			}
			rt.mu.Lock()
			current := rt.ops[ref.Key()]
			var handle *Handle
			if current != nil && current.deferred() != nil {
				handle = &current.deferred().Handle
			}
			watch := current != nil && handle != nil && rt.watched[ref.Key()]
			rt.mu.Unlock()
			if current == nil {
				break
			}
			if watch && rt.opts.Promises != nil {
				_ = rt.opts.Promises.Cancel(PromiseWatch{Address: rt.address, Ref: ref, Handle: *handle})
			}
			var err error
			var diagnostic string
			if impl.cancel != nil {
				_, err, diagnostic = protect(func() (struct{}, error) {
					return struct{}{}, impl.cancel(Cleanup{Context: ctx, Ref: ref, Reason: reason, Handle: handle}, input)
				})
			}
			switch {
			case diagnostic != "":
				outcome = CancellationDied{Ref: ref, Diagnostic: diagnostic}
			case err == nil:
				outcome = CancellationSettled{Ref: ref}
			case ctx.Err() != nil:
				outcome = nil
			default:
				attempt++
				dueAt = rt.now() + rt.policy.CancellationRetry.Milliseconds()
				rt.mu.Lock()
				cerr := rt.commit([]Record{{Event: RetryScheduled{Ref: ref, Phase: "cancellation", Attempt: attempt, DueAt: dueAt, Reason: err.Error()}, RecordedAt: rt.now()}})
				rt.mu.Unlock()
				if cerr != nil {
					outcome = CancellationSettled{Ref: ref, Failure: err.Error()}
				}
				continue
			}
			if ctx.Err() != nil {
				break
			}
		}
		rt.mu.Lock()
		defer rt.mu.Unlock()
		defer rt.endTask(key, t)
		if outcome == nil || rt.closed {
			return
		}
		if err := rt.commit([]Record{{Event: outcome, RecordedAt: rt.now()}}); err != nil {
			rt.report(err)
			return
		}
		if settled, ok := outcome.(CancellationSettled); ok && settled.Failure != "" {
			rt.report(fmt.Errorf("cleanup of %s failed: %s", ref.Key(), settled.Failure))
		}
		if died, ok := outcome.(CancellationDied); ok {
			rt.report(fmt.Errorf("cleanup of %s died: %s", ref.Key(), died.Diagnostic))
		}
		rt.drive()
	}()
}

func sleepUntil(ctx context.Context, clock Clock, dueAt int64) bool {
	delay := time.Duration(dueAt-clock.Now().UnixMilli()) * time.Millisecond
	if delay <= 0 {
		return ctx.Err() == nil
	}
	wake := make(chan struct{})
	stop := clock.AfterFunc(delay, func() { close(wake) })
	defer stop()
	select {
	case <-wake:
		return true
	case <-ctx.Done():
		return false
	}
}

func (rt *Runtime) maybeCheckpoint() {
	every := rt.policy.CheckpointEvery
	if every < 0 || rt.view.length-rt.lastSaved < every || len(rt.ops) > 0 || rt.defect != nil {
		return
	}
	atoms := map[string]AtomState{}
	var conflict error
	rt.registry.Each(func(_ any, value any) {
		c, ok := value.(checkpointer)
		if !ok {
			return
		}
		name, source, state, err := c.checkpoint()
		if source != rt.source {
			return
		}
		if err != nil {
			conflict = fmt.Errorf("checkpoint %s: %w", name, err)
			return
		}
		if prior, ok := atoms[name]; ok && !bytes.Equal(prior.State, state.State) && prior.Position == state.Position {
			conflict = fmt.Errorf("checkpoint: durable atom name %q is used by two atoms", name)
			return
		}
		if prior, ok := atoms[name]; !ok || state.Position > prior.Position {
			atoms[name] = state
		}
	})
	if conflict != nil {
		rt.report(conflict)
		return
	}
	checkpoint := Checkpoint{Position: rt.view.length, Atoms: atoms}
	if err := rt.opts.Journal.SaveCheckpoint(checkpoint); err != nil {
		rt.report(fmt.Errorf("save checkpoint: %w", err))
		return
	}
	rt.lastSaved = rt.view.length
	rt.view = &logView{source: rt.source, length: rt.view.length, tailStart: rt.view.length, seeds: atoms}
	reactive.Set(rt.registry, logAtom, rt.view)
}
