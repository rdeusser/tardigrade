package sqlite

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/rdeusser/tardigrade/go/core"
)

type noted struct {
	Text string `json:"text"`
}

func (noted) EventType() string { return "sqlite.noted" }

func TestJournalContract(t *testing.T) {
	core.RegisterEvent[noted]()
	path := filepath.Join(t.TempDir(), "host.sqlite")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	address := core.ThreadCoordinate{Actor: "a", Instance: "main", Thread: "t1"}
	if created, err := store.CreateThread(core.ThreadInfo{Address: address}); err != nil || !created {
		t.Fatalf("create = %v, %v", created, err)
	}
	if created, _ := store.CreateThread(core.ThreadInfo{Address: address}); created {
		t.Fatal("thread created twice")
	}
	j := store.Journal(address)
	ref := core.Ref{Seq: 1, Atom: "a", Act: "act"}
	records := []core.Record{
		{Event: noted{Text: "hi"}, RecordedAt: 5, Message: &core.Message{ID: "m1", From: core.Sender{Kind: "external", ID: "x"}}},
		{Event: core.EffectRequested{Ref: ref, Origin: core.At(0), Request: core.StoredRequest{Act: "act", Digest: "d"}}},
	}
	if err := j.Append(0, records, core.Changes{PutInputs: map[string]json.RawMessage{ref.Key(): json.RawMessage(`{"big":true}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := j.Append(0, records[:1], core.Changes{}); !errors.Is(err, core.ErrConflict) {
		t.Fatalf("stale append = %v", err)
	}
	store.Close()

	store, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	j = store.Journal(address)
	if length, _ := j.Length(); length != 2 {
		t.Fatalf("length = %d", length)
	}
	read, err := j.Read(0, 2)
	if err != nil || read[0].Event.(noted).Text != "hi" || read[0].RecordedAt != 5 {
		t.Fatalf("read = %+v, %v", read, err)
	}
	if record, position, _ := j.Message("m1"); record == nil || position != 0 {
		t.Fatal("message index missing")
	}
	if got, _ := j.Invocation(core.Identity{Atom: "a", Act: "act", Origin: core.At(0)}); got == nil || *got != ref {
		t.Fatalf("invocation = %v", got)
	}
	if got, _ := j.Invocation(core.Identity{Atom: "a", Act: "act"}); got != nil {
		t.Fatal("startup identity matched an origin")
	}
	if inputs, _ := j.Inputs(); string(inputs[ref.Key()]) != `{"big":true}` {
		t.Fatalf("inputs = %v", inputs)
	}
	if err := j.SaveCheckpoint(core.Checkpoint{Position: 2, Atoms: map[string]core.AtomState{"x": {Position: 2, State: json.RawMessage("1")}}}); err != nil {
		t.Fatal(err)
	}
	if checkpoint, _ := j.Checkpoint(); checkpoint == nil || checkpoint.Atoms["x"].Position != 2 {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
	if _, err := j.Read(1, 3); err == nil {
		t.Fatal("read past length succeeded")
	}
}
