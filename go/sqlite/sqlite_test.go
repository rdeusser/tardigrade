package sqlite

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"github.com/rdeusser/tardigrade/go/tardigrade"
)

type noted struct {
	Text string `json:"text"`
}

func (noted) EventType() string { return "sqlite.noted" }

func TestJournalContract(t *testing.T) {
	tardigrade.RegisterEvent[noted]()
	path := filepath.Join(t.TempDir(), "host.sqlite")
	store, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	address := tardigrade.ThreadCoordinate{Actor: "a", Instance: "main", Thread: "t1"}
	if created, err := store.CreateThread(tardigrade.ThreadInfo{Address: address}); err != nil || !created {
		t.Fatalf("create = %v, %v", created, err)
	}
	if created, _ := store.CreateThread(tardigrade.ThreadInfo{Address: address}); created {
		t.Fatal("thread created twice")
	}
	j := store.Journal(address)
	ref := tardigrade.Ref{Seq: 1, Atom: "a", Act: "act"}
	records := []tardigrade.Record{
		{Event: noted{Text: "hi"}, RecordedAt: 5, Message: &tardigrade.Message{ID: "m1", From: tardigrade.Sender{Kind: "external", ID: "x"}}},
		{Event: tardigrade.EffectRequested{Ref: ref, Origin: new(int64(0)), Request: tardigrade.StoredRequest{Act: "act", Digest: "d"}}},
	}
	if err := j.Append(0, records, tardigrade.Changes{PutInputs: map[string]json.RawMessage{ref.Key(): json.RawMessage(`{"big":true}`)}}); err != nil {
		t.Fatal(err)
	}
	if err := j.Append(0, records[:1], tardigrade.Changes{}); !errors.Is(err, tardigrade.ErrConflict) {
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
	if got, _ := j.Invocation(tardigrade.Identity{Atom: "a", Act: "act", Origin: new(int64(0))}); got == nil || *got != ref {
		t.Fatalf("invocation = %v", got)
	}
	if got, _ := j.Invocation(tardigrade.Identity{Atom: "a", Act: "act"}); got != nil {
		t.Fatal("startup identity matched an origin")
	}
	if inputs, _ := j.Inputs(); string(inputs[ref.Key()]) != `{"big":true}` {
		t.Fatalf("inputs = %v", inputs)
	}
	if err := j.SaveCheckpoint(tardigrade.Checkpoint{Position: 2, Atoms: map[string]tardigrade.AtomState{"x": {Position: 2, State: json.RawMessage("1")}}}); err != nil {
		t.Fatal(err)
	}
	if checkpoint, _ := j.Checkpoint(); checkpoint == nil || checkpoint.Atoms["x"].Position != 2 {
		t.Fatalf("checkpoint = %+v", checkpoint)
	}
	if _, err := j.Read(1, 3); err == nil {
		t.Fatal("read past length succeeded")
	}
}
