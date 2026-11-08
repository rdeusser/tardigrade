package agent_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rdeusser/tardigrade/go/agent"
	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/host"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/sqlite"
)

type router struct {
	mu   sync.Mutex
	seen []string
}

func (r *router) Generate(_ context.Context, req model.Request, _ func(model.Delta)) (model.Reply, error) {
	last := req.Messages[len(req.Messages)-1]
	r.mu.Lock()
	r.seen = append(r.seen, last.Role+":"+last.Text)
	r.mu.Unlock()
	switch {
	case last.Role == "user" && last.Text == "research tardigrades":
		return toolCall("delegate_task", `{"message":"find one fact"}`), nil
	case last.Role == "user" && last.Text == "find one fact":
		return text("they survive vacuum"), nil
	case last.Role == "tool":
		return text("delegated"), nil
	case strings.Contains(last.Text, "they survive vacuum"):
		return text("the child says they survive vacuum"), nil
	}
	return text("unexpected"), nil
}

func TestDelegationReturnsChildAnswerAsTurn(t *testing.T) {
	store, err := sqlite.Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	toMessage := func(message string) any { return agent.MessageInput{Text: message} }
	actor := agent.New(agent.Options{Name: "researcher", System: "Research.", Model: ref, ContextWindowTokens: 100_000, Tools: library.Specs(library.Agents(nil, toMessage))})
	fake := &router{}
	h, err := host.New(host.Options{Actor: actor, Store: store, Acts: func(h *host.Host, _ core.ThreadCoordinate) []core.Implementation {
		return agent.Acts(agent.Services{Model: fake, Tools: library.NewRegistry(library.Agents(h, toMessage))})
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	if _, err := h.CreateThread("main", "lead", ""); err != nil {
		t.Fatal(err)
	}
	rt, _ := h.Thread("main", "lead")
	input, _ := json.Marshal(agent.MessageInput{Text: "research tardigrades"})
	if err := rt.Invoke("message", "m1", core.Sender{Kind: "external", ID: "test"}, input); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(10 * time.Second)
	for {
		turns := core.Read(rt, agent.InferenceState).Turns
		if len(turns) == 2 && turns[1].Settlement == agent.OutcomeCompleted {
			if *turns[1].Answer != "the child says they survive vacuum" {
				t.Fatalf("follow-up answer = %q", *turns[1].Answer)
			}
			break
		}
		select {
		case <-rt.Changed():
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatalf("turns = %+v; status %+v; model saw %v", turns, rt.Status(), fake.seen)
		}
	}
	threads, _ := h.Threads("main")
	if len(threads) != 2 || threads[1].Parent != "lead" {
		t.Fatalf("threads = %+v", threads)
	}
}
