package tardigrade

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
)

// Event is a journal payload named by its type. Event types are value-receiver structs whose JSON encoding has no "type" field; the journal adds the discriminator.
type Event interface {
	EventType() string
}

// RawEvent holds a journal event whose type no handler in this process registered. No reducer accepts it, so it cannot change state.
type RawEvent struct {
	Type string
	Data json.RawMessage
}

// EventType returns the recorded type name.
func (e RawEvent) EventType() string { return e.Type }

var catalog = struct {
	sync.RWMutex
	types map[string]reflect.Type
}{types: map[string]reflect.Type{}}

// RegisterEvent makes E decodable from journals and returns its name. Registering one Go type twice is a no-op; two Go types claiming one name panic.
func RegisterEvent[E Event]() string {
	var zero E
	return register(reflect.TypeOf(zero), zero.EventType())
}

func register(t reflect.Type, name string) string {
	if name == "" {
		panic(fmt.Errorf("tardigrade: event type %s has an empty name", t))
	}
	catalog.Lock()
	defer catalog.Unlock()
	if prior, ok := catalog.types[name]; ok && prior != t {
		panic(fmt.Errorf("tardigrade: event name %q is claimed by %s and %s", name, prior, t))
	}
	catalog.types[name] = t
	return name
}

// EncodeEvent returns an event's JSON object with its "type" discriminator first.
func EncodeEvent(e Event) (json.RawMessage, error) {
	if raw, ok := e.(RawEvent); ok {
		return raw.Data, nil
	}
	body, err := json.Marshal(e)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", e.EventType(), err)
	}
	if len(body) < 2 || body[0] != '{' {
		return nil, fmt.Errorf("encode %s: events must encode as JSON objects", e.EventType())
	}
	name, _ := json.Marshal(e.EventType())
	out := make([]byte, 0, len(body)+len(name)+9)
	out = append(out, `{"type":`...)
	out = append(out, name...)
	if !bytes.Equal(body, []byte("{}")) {
		out = append(out, ',')
		out = append(out, body[1:]...)
	} else {
		out = append(out, '}')
	}
	return out, nil
}

// DecodeEvent parses a journal event. Types without a registered Go type decode as RawEvent.
func DecodeEvent(data json.RawMessage) (Event, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, fmt.Errorf("decode event: %w", err)
	}
	var name string
	if err := json.Unmarshal(fields["type"], &name); err != nil || name == "" {
		return nil, fmt.Errorf("decode event: missing type")
	}
	catalog.RLock()
	t, ok := catalog.types[name]
	catalog.RUnlock()
	if !ok {
		return RawEvent{Type: name, Data: append(json.RawMessage(nil), data...)}, nil
	}
	delete(fields, "type")
	body, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	value := reflect.New(t)
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value.Interface()); err != nil {
		return nil, fmt.Errorf("decode %s: %w", name, err)
	}
	return value.Elem().Interface().(Event), nil
}

// Ref identifies accepted work by its acceptance position, owning actor, and act.
type Ref struct {
	Seq  int64  `json:"seq"`
	Atom string `json:"atom"`
	Act  string `json:"act"`
}

// Key returns the ref's stable string identity.
func (r Ref) Key() string {
	name, _ := json.Marshal(r.Atom)
	act, _ := json.Marshal(r.Act)
	return "[" + strconv.FormatInt(r.Seq, 10) + "," + string(name) + "," + string(act) + "]"
}

// Executor names who produces a deferred result.
type Executor string

// Executors the runtime and host resolve. Any other executor is resolved by the host's Promises service.
const (
	// ExecutorLocal is a producer started with Fork.
	ExecutorLocal Executor = "local"
	// ExecutorClock is a timer that finishes at Handle.At with Handle.Value.
	ExecutorClock Executor = "clock"
	// ExecutorThread is a method call on a child thread, resolved by the host.
	ExecutorThread Executor = "thread"
)

// Handle identifies an eventual result held by an executor.
type Handle struct {
	Executor Executor        `json:"executor"`
	ID       string          `json:"id"`
	Endpoint string          `json:"endpoint,omitempty"`
	Mode     string          `json:"mode,omitempty"`
	At       int64           `json:"at,omitempty"`
	Value    json.RawMessage `json:"value,omitempty"`
}

// FailureKind classifies a failure.
type FailureKind string

// Failure kinds.
const (
	// FailureError is an error the work returned.
	FailureError FailureKind = "error"
	// FailureAttemptTimeout is an attempt that outlived its deadline.
	FailureAttemptTimeout FailureKind = "attempt_timeout"
	// FailurePromiseTimeout is deferred work that did not finish before its deadline.
	FailurePromiseTimeout FailureKind = "promise_timeout"
	// FailureCancelled is work cancelled before it finished.
	FailureCancelled FailureKind = "cancelled"
)

// Failure is why work finished without a value. It implements error.
type Failure struct {
	Kind    FailureKind `json:"kind"`
	Message string      `json:"message"`
}

func (f Failure) Error() string { return f.Message }

// StoredRequest is an accepted act request. Large inputs are retained beside the journal until the work completes and the event keeps their digest.
type StoredRequest struct {
	Act    string          `json:"act"`
	Input  json.RawMessage `json:"input,omitempty"`
	Digest string          `json:"digest,omitempty"`
}

// EffectRequested records durable acceptance of an act request at position Ref.Seq.
type EffectRequested struct {
	Ref     Ref           `json:"ref"`
	Origin  *int64        `json:"origin,omitempty"`
	Request StoredRequest `json:"request"`
}

// ExecutionResult is an execution's immediate value or the promise of one.
type ExecutionResult struct {
	Value   json.RawMessage `json:"value,omitempty"`
	Promise *PromiseRecord  `json:"promise,omitempty"`
}

// PromiseRecord is a submitted handle and its expiry; a nil DeadlineAt never expires.
type PromiseRecord struct {
	Handle     Handle `json:"handle"`
	DeadlineAt *int64 `json:"deadlineAt,omitempty"`
}

// EffectFinished records how an execution finished: Result on success, Err on failure.
type EffectFinished struct {
	Ref    Ref              `json:"ref"`
	Result *ExecutionResult `json:"result,omitempty"`
	Err    *Failure         `json:"error,omitempty"`
}

// EffectDied records a terminal defect; the actor stops proposing work.
type EffectDied struct {
	Ref        Ref    `json:"ref"`
	Diagnostic string `json:"diagnostic"`
}

// PromiseFinished records how deferred work finished: Value on success, Err on failure.
type PromiseFinished struct {
	Ref   Ref             `json:"ref"`
	Value json.RawMessage `json:"value,omitempty"`
	Err   *Failure        `json:"error,omitempty"`
}

// RetryScheduled records the next attempt and its absolute wake time in Unix milliseconds. Phase is empty for execution and "cancellation" for cleanup.
type RetryScheduled struct {
	Ref     Ref    `json:"ref"`
	Phase   string `json:"phase,omitempty"`
	Attempt int    `json:"attempt"`
	DueAt   int64  `json:"dueAt"`
	Reason  string `json:"reason"`
}

// EffectCancelled records a cancellation decision; cleanup may still be pending.
type EffectCancelled struct {
	Ref    Ref    `json:"ref"`
	Reason string `json:"reason"`
}

// CleanupFinished records the end of cleanup; a nonempty Error records a final cleanup failure.
type CleanupFinished struct {
	Ref   Ref    `json:"ref"`
	Error string `json:"error,omitempty"`
}

// CleanupDied records a cleanup defect.
type CleanupDied struct {
	Ref        Ref    `json:"ref"`
	Diagnostic string `json:"diagnostic"`
}

// ThreadCoordinate addresses one thread of one actor instance.
type ThreadCoordinate struct {
	Actor    string `json:"actor"`
	Instance string `json:"instance"`
	Thread   string `json:"thread"`
}

// ThreadInfo is a thread directory entry.
type ThreadInfo struct {
	Address   ThreadCoordinate `json:"address"`
	Parent    string           `json:"parent,omitempty"`
	Depth     int              `json:"depth"`
	CreatedAt int64            `json:"createdAt"`
	Active    bool             `json:"active"`
}

// ThreadCreated is the first record of a thread's journal.
type ThreadCreated struct {
	Address ThreadCoordinate  `json:"address"`
	Parent  *ThreadCoordinate `json:"parent,omitempty"`
	Depth   int               `json:"depth"`
}

func (EffectRequested) EventType() string { return "EffectRequested" }
func (EffectFinished) EventType() string  { return "EffectFinished" }
func (EffectDied) EventType() string      { return "EffectDied" }
func (PromiseFinished) EventType() string { return "PromiseFinished" }
func (RetryScheduled) EventType() string  { return "RetryScheduled" }
func (EffectCancelled) EventType() string { return "EffectCancelled" }
func (CleanupFinished) EventType() string { return "CleanupFinished" }
func (CleanupDied) EventType() string     { return "CleanupDied" }
func (ThreadCreated) EventType() string   { return "ThreadCreated" }

func init() {
	RegisterEvent[EffectRequested]()
	RegisterEvent[EffectFinished]()
	RegisterEvent[EffectDied]()
	RegisterEvent[PromiseFinished]()
	RegisterEvent[RetryScheduled]()
	RegisterEvent[EffectCancelled]()
	RegisterEvent[CleanupFinished]()
	RegisterEvent[CleanupDied]()
	RegisterEvent[ThreadCreated]()
}

// lifecycleRef returns the ref of an effect lifecycle event.
func lifecycleRef(e Event) (Ref, bool) {
	switch v := e.(type) {
	case EffectRequested:
		return v.Ref, true
	case EffectFinished:
		return v.Ref, true
	case EffectDied:
		return v.Ref, true
	case PromiseFinished:
		return v.Ref, true
	case RetryScheduled:
		return v.Ref, true
	case EffectCancelled:
		return v.Ref, true
	case CleanupFinished:
		return v.Ref, true
	case CleanupDied:
		return v.Ref, true
	}
	return Ref{}, false
}

// isCore reports whether an event name is reserved for framework records.
func isCore(e Event) bool {
	if _, ok := lifecycleRef(e); ok {
		return true
	}
	_, ok := e.(ThreadCreated)
	return ok
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
