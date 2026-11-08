package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"
)

// Record is one journal entry: an event and its delivery context.
type Record struct {
	Event      Event
	RecordedAt int64
	Message    *Message
}

// Message is the delivery context of an inbound record. Invocation is set for method calls.
type Message struct {
	ID         string      `json:"id"`
	From       Sender      `json:"from"`
	Invocation *Invocation `json:"invocation,omitempty"`
}

// Sender identifies who delivered a message.
type Sender struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Invocation is a method call carried by a message.
type Invocation struct {
	Method string          `json:"method"`
	Input  json.RawMessage `json:"input"`
}

// Meta is the delivery context a reducer receives with each event; Position is the zero-based journal position.
type Meta struct {
	Position   int64
	RecordedAt int64
	Message    *Message
}

type recordJSON struct {
	Event      json.RawMessage `json:"event"`
	RecordedAt int64           `json:"recordedAt,omitempty"`
	Message    *Message        `json:"message,omitempty"`
}

// MarshalJSON encodes a record as {"event", "recordedAt", "message"}.
func (r Record) MarshalJSON() ([]byte, error) {
	event, err := EncodeEvent(r.Event)
	if err != nil {
		return nil, err
	}
	return json.Marshal(recordJSON{Event: event, RecordedAt: r.RecordedAt, Message: r.Message})
}

// UnmarshalJSON decodes a record written by MarshalJSON.
func (r *Record) UnmarshalJSON(data []byte) error {
	var raw recordJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	event, err := DecodeEvent(raw.Event)
	if err != nil {
		return err
	}
	*r = Record{Event: event, RecordedAt: raw.RecordedAt, Message: raw.Message}
	return nil
}

// ErrConflict reports an append whose expected length is stale.
var ErrConflict = errors.New("journal: expected length is stale")

// Changes are side-table writes committed atomically with an append.
type Changes struct {
	// PutInputs retains large act inputs by ref key until their work completes.
	PutInputs map[string]json.RawMessage
	// RemoveInputs releases retained inputs of completed work.
	RemoveInputs []string
}

// Checkpoint holds durable atom states so replay folds only later records.
type Checkpoint struct {
	Position int64                `json:"position"`
	Atoms    map[string]AtomState `json:"atoms"`
}

// AtomState is one durable atom's encoded state at a journal position.
type AtomState struct {
	Position int64           `json:"position"`
	State    json.RawMessage `json:"state"`
}

// Identity identifies an invocation by its owning actor, act, and origin; a nil origin identifies a startup invocation.
type Identity struct {
	Atom   string
	Act    string
	Origin *int64
}

// Key returns the identity's stable string form.
func (i Identity) Key() string {
	origin := "null"
	if i.Origin != nil {
		origin = strconv.FormatInt(*i.Origin, 10)
	}
	atom, _ := json.Marshal(i.Atom)
	act, _ := json.Marshal(i.Act)
	return "[" + string(atom) + "," + string(act) + "," + origin + "]"
}

// Journal is one thread's ordered event log. Append commits a batch atomically or fails with ErrConflict when expected differs from the current length.
type Journal interface {
	Length() (int64, error)
	// Read returns records in [from, to).
	Read(from, to int64) ([]Record, error)
	Append(expected int64, records []Record, changes Changes) error
	// Message returns the record and position of a message ID, or nil when absent.
	Message(id string) (*Record, int64, error)
	// Invocation returns the ref accepted for an identity, or nil when none was accepted.
	Invocation(identity Identity) (*Ref, error)
	// Inputs returns retained act inputs by ref key.
	Inputs() (map[string]json.RawMessage, error)
	Checkpoint() (*Checkpoint, error)
	SaveCheckpoint(checkpoint Checkpoint) error
}

// MemoryJournal is an in-process Journal for tests and ephemeral actors.
type MemoryJournal struct {
	mu          sync.Mutex
	records     []Record
	messages    map[string]int64
	invocations map[string]Ref
	inputs      map[string]json.RawMessage
	checkpoint  *Checkpoint
}

// NewMemoryJournal returns an empty in-memory journal.
func NewMemoryJournal() *MemoryJournal {
	return &MemoryJournal{messages: map[string]int64{}, invocations: map[string]Ref{}, inputs: map[string]json.RawMessage{}}
}

func (j *MemoryJournal) Length() (int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return int64(len(j.records)), nil
}

func (j *MemoryJournal) Read(from, to int64) ([]Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if from < 0 || to < from || to > int64(len(j.records)) {
		return nil, fmt.Errorf("journal: range [%d, %d) is outside length %d", from, to, len(j.records))
	}
	return append([]Record(nil), j.records[from:to]...), nil
}

func (j *MemoryJournal) Append(expected int64, records []Record, changes Changes) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if expected != int64(len(j.records)) {
		return ErrConflict
	}
	for _, record := range records {
		if record.Message != nil {
			if _, ok := j.messages[record.Message.ID]; ok {
				return fmt.Errorf("journal: duplicate message %q", record.Message.ID)
			}
		}
		if request, ok := record.Event.(EffectRequested); ok {
			key := Identity{Atom: request.Ref.Atom, Act: request.Ref.Act, Origin: request.Origin}.Key()
			if _, ok := j.invocations[key]; ok {
				return fmt.Errorf("journal: duplicate invocation %s", key)
			}
		}
	}
	for i, record := range records {
		position := expected + int64(i)
		if record.Message != nil {
			j.messages[record.Message.ID] = position
		}
		if request, ok := record.Event.(EffectRequested); ok {
			j.invocations[Identity{Atom: request.Ref.Atom, Act: request.Ref.Act, Origin: request.Origin}.Key()] = request.Ref
		}
	}
	j.records = append(j.records, records...)
	for key, input := range changes.PutInputs {
		j.inputs[key] = input
	}
	for _, key := range changes.RemoveInputs {
		delete(j.inputs, key)
	}
	return nil
}

func (j *MemoryJournal) Message(id string) (*Record, int64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	position, ok := j.messages[id]
	if !ok {
		return nil, 0, nil
	}
	record := j.records[position]
	return &record, position, nil
}

func (j *MemoryJournal) Invocation(identity Identity) (*Ref, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	ref, ok := j.invocations[identity.Key()]
	if !ok {
		return nil, nil
	}
	return &ref, nil
}

func (j *MemoryJournal) Inputs() (map[string]json.RawMessage, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	copied := make(map[string]json.RawMessage, len(j.inputs))
	for k, v := range j.inputs {
		copied[k] = v
	}
	return copied, nil
}

func (j *MemoryJournal) Checkpoint() (*Checkpoint, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.checkpoint, nil
}

func (j *MemoryJournal) SaveCheckpoint(checkpoint Checkpoint) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.checkpoint = &checkpoint
	return nil
}
