// Package host runs actors' threads over a SQLite store and serves them through the HostService API.
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

	"github.com/rdeusser/tardigrade/go/sqlite"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// Defaults for Options fields left zero.
const (
	DefaultMaxDepth     = 3
	DefaultStreamBuffer = 64
)

// Options configure a host.
type Options struct {
	// Actors are the actors the host serves; duplicate names fail New.
	Actors []*tardigrade.Actor
	Store  *sqlite.Store
	// Handlers returns the act handlers of a thread; handlers may capture the host and the thread address, whose Actor names the thread's actor.
	Handlers func(h *Host, address tardigrade.ThreadCoordinate) []tardigrade.Handler
	Policy   tardigrade.Policy
	Clock    tardigrade.Clock
	// MaxDepth bounds child thread nesting below a root thread.
	MaxDepth int
	// StreamBuffer bounds unread execution updates per subscriber; further updates are dropped.
	StreamBuffer int
}

// Errors reported for addresses the host cannot resolve.
var (
	ErrUnknownActor  = errors.New("unknown actor")
	ErrUnknownThread = errors.New("unknown thread")
)

// Host owns a store and the runtimes of the threads it has opened.
type Host struct {
	opts    Options
	actors  map[string]*tardigrade.Actor
	mu      sync.Mutex
	threads map[tardigrade.ThreadCoordinate]*tardigrade.Runtime
	opening map[tardigrade.ThreadCoordinate]*sync.Mutex
	watches map[tardigrade.ThreadCoordinate]map[string]watch
	subs    map[tardigrade.ThreadCoordinate]map[chan tardigrade.Update]struct{}
	active  map[tardigrade.ThreadCoordinate]bool
	closed  bool
}

type watch struct {
	parent tardigrade.ThreadCoordinate
	ref    tardigrade.Ref
	call   delegation
}

type delegation struct {
	Parent tardigrade.ThreadCoordinate `json:"parent"`
	Child  tardigrade.ThreadCoordinate `json:"child"`
	Call   string                      `json:"call"`
}

// sender identifies a parent thread as the sender of the calls it delegates.
func sender(parent tardigrade.ThreadCoordinate) tardigrade.Sender {
	return tardigrade.Sender{Kind: string(tardigrade.ExecutorThread), ID: parent.Actor + "/" + parent.Instance + "/" + parent.Thread}
}

// New opens a host and resumes every thread recorded with unfinished work.
func New(opts Options) (*Host, error) {
	if len(opts.Actors) == 0 || opts.Store == nil {
		return nil, errors.New("host: New requires actors and a store")
	}
	actors := make(map[string]*tardigrade.Actor, len(opts.Actors))
	for _, actor := range opts.Actors {
		if _, ok := actors[actor.Name()]; ok {
			return nil, fmt.Errorf("host: duplicate actor %q", actor.Name())
		}
		actors[actor.Name()] = actor
	}
	if opts.MaxDepth == 0 {
		opts.MaxDepth = DefaultMaxDepth
	}
	if opts.StreamBuffer == 0 {
		opts.StreamBuffer = DefaultStreamBuffer
	}
	if opts.Clock == nil {
		opts.Clock = tardigrade.SystemClock
	}
	h := &Host{opts: opts, actors: actors, threads: map[tardigrade.ThreadCoordinate]*tardigrade.Runtime{}, opening: map[tardigrade.ThreadCoordinate]*sync.Mutex{},
		watches: map[tardigrade.ThreadCoordinate]map[string]watch{}, subs: map[tardigrade.ThreadCoordinate]map[chan tardigrade.Update]struct{}{}, active: map[tardigrade.ThreadCoordinate]bool{}}
	threads, err := opts.Store.Threads("", "")
	if err != nil {
		return nil, err
	}
	for _, info := range threads {
		if info.Active {
			if _, err := h.runtime(info.Address); err != nil {
				h.Close()
				return nil, fmt.Errorf("resume %s: %w", path(info.Address), err)
			}
		}
	}
	return h, nil
}

// Actors returns the served actors in the order Options listed them.
func (h *Host) Actors() []*tardigrade.Actor { return h.opts.Actors }

func path(address tardigrade.ThreadCoordinate) string {
	return address.Actor + "/" + address.Instance + "/" + address.Thread
}

func randomName() string {
	var b [6]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func validName(name string) bool {
	return name != "" && !strings.ContainsAny(name, "/")
}

// CreateThread allocates a thread of an actor. An existing name returns the existing thread, so a retried creation is absorbed; an empty name is generated. A non-nil parent creates a child of that thread, which may belong to another actor.
func (h *Host) CreateThread(actor, instance, name string, parent *tardigrade.ThreadCoordinate) (tardigrade.ThreadCoordinate, error) {
	if name == "" {
		name = randomName()
	}
	address := tardigrade.ThreadCoordinate{Actor: actor, Instance: instance, Thread: name}
	if h.actors[actor] == nil {
		return address, fmt.Errorf("%w: %s", ErrUnknownActor, actor)
	}
	if !validName(instance) || !validName(name) {
		return address, &tardigrade.InvalidInputError{Err: errors.New("instance and thread names must be nonempty and contain no slash")}
	}
	info := tardigrade.ThreadInfo{Address: address, CreatedAt: h.opts.Clock.Now().UnixMilli()}
	if parent != nil {
		parentInfo, err := h.opts.Store.Thread(*parent)
		if err != nil {
			return address, err
		}
		if parentInfo == nil {
			return address, fmt.Errorf("%w: %s", ErrUnknownThread, path(*parent))
		}
		if parentInfo.Depth+1 > h.opts.MaxDepth {
			return address, &tardigrade.InvalidInputError{Err: fmt.Errorf("child depth limit %d reached", h.opts.MaxDepth)}
		}
		info.Parent, info.Depth = parent, parentInfo.Depth+1
	}
	if existing, err := h.opts.Store.Thread(address); err != nil || existing != nil {
		if existing != nil && !sameParent(existing.Parent, info.Parent) {
			return address, &tardigrade.InvalidInputError{Err: fmt.Errorf("thread %s exists with another parent", path(address))}
		}
		return address, err
	}
	if _, err := h.opts.Store.CreateThread(info); err != nil {
		return address, err
	}
	_, err := h.runtime(address)
	return address, err
}

func sameParent(a, b *tardigrade.ThreadCoordinate) bool {
	return a == b || a != nil && b != nil && *a == *b
}

// Thread returns the runtime of an existing thread, opening it when needed.
func (h *Host) Thread(address tardigrade.ThreadCoordinate) (*tardigrade.Runtime, error) {
	return h.runtime(address)
}

// Threads lists threads in creation order. An empty actor lists every actor and an empty instance lists every instance.
func (h *Host) Threads(actor, instance string) ([]tardigrade.ThreadInfo, error) {
	return h.opts.Store.Threads(actor, instance)
}

func (h *Host) runtime(address tardigrade.ThreadCoordinate) (*tardigrade.Runtime, error) {
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return nil, tardigrade.ErrClosed
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
	actor := h.actors[address.Actor]
	if actor == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownActor, address.Actor)
	}
	info, err := h.opts.Store.Thread(address)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, fmt.Errorf("%w: %s", ErrUnknownThread, path(address))
	}
	created := &tardigrade.ThreadCreated{Address: address, Parent: info.Parent, Depth: info.Depth}
	var handlers []tardigrade.Handler
	if h.opts.Handlers != nil {
		handlers = h.opts.Handlers(h, address)
	}
	h.mu.Lock()
	h.active[address] = info.Active
	h.mu.Unlock()
	rt, err := tardigrade.Open(tardigrade.Options{
		Actor: actor, Journal: h.opts.Store.Journal(address), Address: address, Handlers: handlers,
		Policy: h.opts.Policy, Clock: h.opts.Clock, Promises: h, Publish: h.publish, Created: created,
		OnCommit: func(_ int64, _ []tardigrade.Record, active int) { h.committed(address, active > 0) },
	})
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		go rt.Close()
		return nil, tardigrade.ErrClosed
	}
	h.threads[address] = rt
	if len(h.watches[address]) > 0 {
		go h.check(address)
	}
	return rt, nil
}

// committed runs under the thread's runtime lock after each commit.
func (h *Host) committed(address tardigrade.ThreadCoordinate, active bool) {
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

// Delegate calls a method of actor on a child thread of parent and returns the handle of its eventual result. The child is named after the parent thread and ref, and the call ID is ref.Key(), so a delegation repeated after a crash reaches the same call.
func (h *Host) Delegate(parent tardigrade.ThreadCoordinate, ref tardigrade.Ref, actor, method string, input any) (tardigrade.Handle, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return tardigrade.Handle{}, err
	}
	child, err := h.CreateThread(actor, parent.Instance, fmt.Sprintf("%s.%d", parent.Thread, ref.Seq), &parent)
	if err != nil {
		return tardigrade.Handle{}, err
	}
	rt, err := h.runtime(child)
	if err != nil {
		return tardigrade.Handle{}, err
	}
	call := ref.Key()
	if err := rt.Invoke(method, call, sender(parent), data); err != nil {
		return tardigrade.Handle{}, err
	}
	id, _ := json.Marshal(delegation{Parent: parent, Child: child, Call: call})
	return tardigrade.Handle{Executor: tardigrade.ExecutorThread, ID: string(id)}, nil
}

// CancelDelegation asks a delegated call to stop; calls whose method is not cancellable are left running.
func (h *Host) CancelDelegation(handle tardigrade.Handle, reason string) error {
	var d delegation
	if err := json.Unmarshal([]byte(handle.ID), &d); err != nil {
		return err
	}
	rt, err := h.runtime(d.Child)
	if err != nil {
		return err
	}
	err = rt.CancelCall(d.Call, reason, sender(d.Parent))
	if errors.Is(err, tardigrade.ErrNotCancellable) {
		return nil
	}
	return err
}

// Watch implements tardigrade.Promises for the thread executor.
func (h *Host) Watch(w tardigrade.PromiseWatch) error {
	if w.Handle.Executor != tardigrade.ExecutorThread {
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

// Cancel implements tardigrade.Promises by dropping a watch.
func (h *Host) Cancel(w tardigrade.PromiseWatch) error {
	var d delegation
	if err := json.Unmarshal([]byte(w.Handle.ID), &d); err != nil {
		return err
	}
	h.mu.Lock()
	delete(h.watches[d.Child], w.Ref.Key())
	h.mu.Unlock()
	return nil
}

// check finishes parent promises whose child calls finished.
func (h *Host) check(child tardigrade.ThreadCoordinate) {
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
		state, err := rt.Call(w.call.Call)
		if err != nil || state.Status == tardigrade.StatusPending {
			continue
		}
		parent, err := h.runtime(w.parent)
		if err != nil {
			continue
		}
		var value json.RawMessage
		var failure *tardigrade.Failure
		switch state.Status {
		case tardigrade.StatusCompleted:
			value = state.Output
		case tardigrade.StatusCancelled:
			failure = &tardigrade.Failure{Kind: tardigrade.FailureCancelled, Message: state.Reason}
		default:
			failure = &tardigrade.Failure{Kind: tardigrade.FailureError, Message: state.Error}
		}
		if err := parent.Finish(w.ref, value, failure); err == nil {
			h.mu.Lock()
			delete(h.watches[child], w.ref.Key())
			h.mu.Unlock()
		}
	}
}

func (h *Host) publish(update tardigrade.Update) {
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
func (h *Host) Subscribe(address tardigrade.ThreadCoordinate) (updates <-chan tardigrade.Update, stop func()) {
	ch := make(chan tardigrade.Update, h.opts.StreamBuffer)
	h.mu.Lock()
	if h.subs[address] == nil {
		h.subs[address] = map[chan tardigrade.Update]struct{}{}
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
	h.threads = map[tardigrade.ThreadCoordinate]*tardigrade.Runtime{}
	h.mu.Unlock()
	var group sync.WaitGroup
	for _, rt := range threads {
		group.Go(func() {
			_ = rt.Close()
		})
	}
	group.Wait()
	return nil
}

// WaitIdle blocks until no thread has running work, for tests and one-shot tools.
func (h *Host) WaitIdle(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		h.mu.Lock()
		threads := make([]*tardigrade.Runtime, 0, len(h.threads))
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
