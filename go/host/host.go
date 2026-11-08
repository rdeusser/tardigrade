// Package host runs an actor's threads over a SQLite store and serves them over HTTP.
package host

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/sqlite"
)

// Defaults for Options fields left zero.
const (
	DefaultMaxDepth     = 3
	DefaultStreamBuffer = 64
)

// ThreadExecutor names the promise executor for method calls on child threads.
const ThreadExecutor = "thread"

// Options configure a host.
type Options struct {
	Actor *core.Actor
	Store *sqlite.Store
	// Acts returns a thread's act implementations; implementations may capture the host and the thread address.
	Acts   func(h *Host, address core.ThreadCoordinate) []core.Implementation
	Policy core.Policy
	Clock  core.Clock
	// MaxDepth bounds child thread nesting below a root thread.
	MaxDepth int
	// StreamBuffer bounds unread execution updates per subscriber; further updates are dropped.
	StreamBuffer int
}

// ErrUnknownThread reports a thread that was never created.
var ErrUnknownThread = errors.New("unknown thread")

// Host owns a store and the runtimes of the threads it has opened.
type Host struct {
	opts    Options
	mu      sync.Mutex
	threads map[core.ThreadCoordinate]*core.Runtime
	opening map[core.ThreadCoordinate]*sync.Mutex
	watches map[core.ThreadCoordinate]map[string]watch
	subs    map[core.ThreadCoordinate]map[chan core.Update]struct{}
	active  map[core.ThreadCoordinate]bool
	closed  bool
}

type watch struct {
	parent core.ThreadCoordinate
	ref    core.Ref
	call   delegation
}

type delegation struct {
	Parent string                `json:"parent"`
	Child  core.ThreadCoordinate `json:"child"`
	Method string                `json:"method"`
	Call   string                `json:"call"`
}

// New opens a host and resumes every thread recorded with unfinished work.
func New(opts Options) (*Host, error) {
	if opts.Actor == nil || opts.Store == nil {
		return nil, errors.New("host: New requires an actor and a store")
	}
	if opts.MaxDepth == 0 {
		opts.MaxDepth = DefaultMaxDepth
	}
	if opts.StreamBuffer == 0 {
		opts.StreamBuffer = DefaultStreamBuffer
	}
	if opts.Clock == nil {
		opts.Clock = core.SystemClock
	}
	h := &Host{opts: opts, threads: map[core.ThreadCoordinate]*core.Runtime{}, opening: map[core.ThreadCoordinate]*sync.Mutex{},
		watches: map[core.ThreadCoordinate]map[string]watch{}, subs: map[core.ThreadCoordinate]map[chan core.Update]struct{}{}, active: map[core.ThreadCoordinate]bool{}}
	threads, err := opts.Store.Threads(opts.Actor.Name(), "")
	if err != nil {
		return nil, err
	}
	for _, info := range threads {
		if info.Active {
			if _, err := h.runtime(info.Address); err != nil {
				h.Close()
				return nil, fmt.Errorf("resume %s/%s: %w", info.Address.Instance, info.Address.Thread, err)
			}
		}
	}
	return h, nil
}

// Actor returns the hosted actor.
func (h *Host) Actor() *core.Actor { return h.opts.Actor }

func randomName() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func validName(name string) bool {
	return name != "" && !strings.ContainsAny(name, "/")
}

// CreateThread allocates a thread. An existing name returns the existing thread, so a retried creation is absorbed; an empty name is generated. A nonempty parent creates a child of that thread.
func (h *Host) CreateThread(instance, name, parent string) (core.ThreadCoordinate, error) {
	if name == "" {
		name = randomName()
	}
	if !validName(instance) || !validName(name) {
		return core.ThreadCoordinate{}, &core.InvalidInputError{Err: errors.New("instance and thread names must be nonempty and contain no slash")}
	}
	address := core.ThreadCoordinate{Actor: h.opts.Actor.Name(), Instance: instance, Thread: name}
	info := core.ThreadInfo{Address: address, CreatedAt: h.opts.Clock.Now().UnixMilli()}
	if parent != "" {
		parentInfo, err := h.opts.Store.Thread(core.ThreadCoordinate{Actor: address.Actor, Instance: instance, Thread: parent})
		if err != nil {
			return address, err
		}
		if parentInfo == nil {
			return address, fmt.Errorf("%w: %s", ErrUnknownThread, parent)
		}
		if parentInfo.Depth+1 > h.opts.MaxDepth {
			return address, &core.InvalidInputError{Err: fmt.Errorf("child depth limit %d reached", h.opts.MaxDepth)}
		}
		info.Parent, info.Depth = parent, parentInfo.Depth+1
	}
	if existing, err := h.opts.Store.Thread(address); err != nil || existing != nil {
		if existing != nil && existing.Parent != info.Parent {
			return address, &core.InvalidInputError{Err: fmt.Errorf("thread %s exists with another parent", name)}
		}
		return address, err
	}
	if _, err := h.opts.Store.CreateThread(info); err != nil {
		return address, err
	}
	_, err := h.runtime(address)
	return address, err
}

// Thread returns the runtime of an existing thread, opening it when needed.
func (h *Host) Thread(instance, thread string) (*core.Runtime, error) {
	return h.runtime(core.ThreadCoordinate{Actor: h.opts.Actor.Name(), Instance: instance, Thread: thread})
}

// Threads lists an instance's threads; an empty instance lists every instance.
func (h *Host) Threads(instance string) ([]core.ThreadInfo, error) {
	return h.opts.Store.Threads(h.opts.Actor.Name(), instance)
}

func (h *Host) runtime(address core.ThreadCoordinate) (*core.Runtime, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, core.ErrClosed
	}
	if rt := h.threads[address]; rt != nil {
		h.mu.Unlock()
		return rt, nil
	}
	gate := h.opening[address]
	if gate == nil {
		gate = &sync.Mutex{}
		h.opening[address] = gate
	}
	h.mu.Unlock()
	gate.Lock()
	defer gate.Unlock()
	h.mu.Lock()
	if rt := h.threads[address]; rt != nil {
		h.mu.Unlock()
		return rt, nil
	}
	h.mu.Unlock()
	info, err := h.opts.Store.Thread(address)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("%w: %s/%s", ErrUnknownThread, address.Instance, address.Thread)
	}
	created := &core.ThreadCreated{Address: address, Depth: info.Depth}
	if info.Parent != "" {
		created.Parent = &core.ThreadCoordinate{Actor: address.Actor, Instance: address.Instance, Thread: info.Parent}
	}
	var acts []core.Implementation
	if h.opts.Acts != nil {
		acts = h.opts.Acts(h, address)
	}
	h.mu.Lock()
	h.active[address] = info.Active
	h.mu.Unlock()
	rt, err := core.Open(core.Options{
		Actor: h.opts.Actor, Journal: h.opts.Store.Journal(address), Address: address, Acts: acts,
		Policy: h.opts.Policy, Clock: h.opts.Clock, Promises: h, Publish: h.publish, Created: created,
		OnCommit: func(_ int64, _ []core.Record, active int) { h.committed(address, active > 0) },
	})
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		go rt.Close()
		return nil, core.ErrClosed
	}
	h.threads[address] = rt
	if len(h.watches[address]) > 0 {
		go h.check(address)
	}
	return rt, nil
}

// committed runs under the thread's runtime lock after each commit.
func (h *Host) committed(address core.ThreadCoordinate, active bool) {
	h.mu.Lock()
	changed := h.active[address] != active
	h.active[address] = active
	watched := len(h.watches[address]) > 0
	h.mu.Unlock()
	if changed {
		_ = h.opts.Store.SetActive(address, active)
	}
	if watched {
		go h.check(address)
	}
}

// Delegate calls a method on a child thread of parent and returns the handle of its eventual result. The child name and call ID derive from ref, so a delegation repeated after a crash reaches the same call.
func (h *Host) Delegate(parent core.ThreadCoordinate, ref core.Ref, method string, input any) (core.Handle, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return core.Handle{}, err
	}
	child, err := h.CreateThread(parent.Instance, fmt.Sprintf("%s.%d", parent.Thread, ref.Seq), parent.Thread)
	if err != nil {
		return core.Handle{}, err
	}
	rt, err := h.runtime(child)
	if err != nil {
		return core.Handle{}, err
	}
	call := ref.Key()
	if err := rt.Invoke(method, call, core.Sender{Kind: ThreadExecutor, ID: parent.Thread}, data); err != nil {
		return core.Handle{}, err
	}
	id, _ := json.Marshal(delegation{Parent: parent.Thread, Child: child, Method: method, Call: call})
	return core.Handle{Executor: ThreadExecutor, ID: string(id)}, nil
}

// CancelDelegation asks a delegated call to stop; calls whose method is not cancellable are left running.
func (h *Host) CancelDelegation(handle core.Handle, reason string) error {
	var d delegation
	if err := json.Unmarshal([]byte(handle.ID), &d); err != nil {
		return err
	}
	rt, err := h.runtime(d.Child)
	if err != nil {
		return err
	}
	err = rt.CancelCall(d.Method, d.Call, reason, core.Sender{Kind: ThreadExecutor, ID: d.Parent})
	if errors.Is(err, core.ErrNotCancellable) {
		return nil
	}
	return err
}

// Watch implements core.Promises for the thread executor.
func (h *Host) Watch(w core.PromiseWatch) error {
	if w.Handle.Executor != ThreadExecutor {
		return fmt.Errorf("host: no executor %q", w.Handle.Executor)
	}
	var d delegation
	if err := json.Unmarshal([]byte(w.Handle.ID), &d); err != nil {
		return err
	}
	h.mu.Lock()
	if h.watches[d.Child] == nil {
		h.watches[d.Child] = map[string]watch{}
	}
	h.watches[d.Child][w.Ref.Key()] = watch{parent: w.Address, ref: w.Ref, call: d}
	h.mu.Unlock()
	if _, err := h.runtime(d.Child); err != nil {
		return err
	}
	go h.check(d.Child)
	return nil
}

// Cancel implements core.Promises by dropping a watch.
func (h *Host) Cancel(w core.PromiseWatch) error {
	var d delegation
	if err := json.Unmarshal([]byte(w.Handle.ID), &d); err != nil {
		return err
	}
	h.mu.Lock()
	delete(h.watches[d.Child], w.Ref.Key())
	h.mu.Unlock()
	return nil
}

// check settles parent promises whose child calls finished.
func (h *Host) check(child core.ThreadCoordinate) {
	h.mu.Lock()
	rt := h.threads[child]
	pending := make([]watch, 0, len(h.watches[child]))
	for _, w := range h.watches[child] {
		pending = append(pending, w)
	}
	h.mu.Unlock()
	if rt == nil {
		return
	}
	for _, w := range pending {
		state, err := rt.Call(w.call.Method, w.call.Call)
		if err != nil || state.Status == core.StatusPending {
			continue
		}
		parent, err := h.runtime(w.parent)
		if err != nil {
			continue
		}
		var value json.RawMessage
		var reason *core.Reason
		switch state.Status {
		case core.StatusCompleted:
			value = state.Output
		case core.StatusCancelled:
			reason = &core.Reason{Kind: core.ReasonCancelled, Message: state.Reason}
		default:
			reason = &core.Reason{Kind: core.ReasonFailed, Message: state.Error}
		}
		if err := parent.Settle(w.ref, value, reason); err == nil {
			h.mu.Lock()
			delete(h.watches[child], w.ref.Key())
			h.mu.Unlock()
		}
	}
}

func (h *Host) publish(update core.Update) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs[update.Address] {
		select {
		case ch <- update:
		default:
		}
	}
}

// Subscribe receives a thread's execution updates until stop is called. Updates beyond Options.StreamBuffer unread are dropped.
func (h *Host) Subscribe(address core.ThreadCoordinate) (updates <-chan core.Update, stop func()) {
	ch := make(chan core.Update, h.opts.StreamBuffer)
	h.mu.Lock()
	if h.subs[address] == nil {
		h.subs[address] = map[chan core.Update]struct{}{}
	}
	h.subs[address][ch] = struct{}{}
	h.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs[address], ch)
			h.mu.Unlock()
		})
	}
}

// Close closes every runtime; unfinished work resumes when a host opens the store again.
func (h *Host) Close() error {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil
	}
	h.closed = true
	threads := h.threads
	h.threads = map[core.ThreadCoordinate]*core.Runtime{}
	h.mu.Unlock()
	var group sync.WaitGroup
	for _, rt := range threads {
		group.Add(1)
		go func() {
			defer group.Done()
			_ = rt.Close()
		}()
	}
	group.Wait()
	return nil
}

// WaitIdle blocks until no thread has running work, for tests and one-shot tools.
func (h *Host) WaitIdle(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		h.mu.Lock()
		threads := make([]*core.Runtime, 0, len(h.threads))
		for _, rt := range h.threads {
			threads = append(threads, rt)
		}
		h.mu.Unlock()
		busy := false
		for _, rt := range threads {
			if rt.Status().Running > 0 {
				busy = true
				break
			}
		}
		if !busy {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("host: threads still running")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
