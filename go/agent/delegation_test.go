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
	"github.com/rdeusser/tardigrade/go/host"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/sqlite"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

type router struct {
	mu   sync.Mutex
	seen []string
}

func (r *router) Generate(_ context.Context, req model.Request, _ func(model.Delta)) (model.Reply, error) {
	last := req.Messages[len(req.Messages)-1]
	r.mu.Lock()
	r.seen = append(r.seen, string(last.Role)+":"+last.Text)
	r.mu.Unlock()
	switch {
	case last.Role == model.RoleUser && last.Text == "research tardigrades":
		return toolCall("delegate_task", `{"message":"find one fact"}`), nil
	case last.Role == model.RoleUser && last.Text == "find one fact":
		return text("they survive vacuum"), nil
	case last.Role == model.RoleTool:
		return text("delegated"), nil
	case strings.Contains(last.Text, "they survive vacuum"):
		return text("the child says they survive vacuum"), nil
	}
	return text("unexpected"), nil
}

func TestDelegationToAnotherActorReturnsAnswerAsTurn(t *testing.T) {
	store, err := sqlite.Open(filepath.Join(t.TempDir(), "host.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	toMessage := func(message string) any { return agent.MessageInput{Text: message} }
	lead := agent.New(agent.Options{Name: "lead", System: "Lead.", Model: ref, ContextWindowTokens: 100_000, Tools: library.Specs(library.Agents(nil, "researcher", toMessage))})
	researcher := agent.New(agent.Options{Name: "researcher", System: "Research.", Model: ref, ContextWindowTokens: 100_000})
	fake := &router{}
	h, err := host.New(host.Options{Actors: []*tardigrade.Actor{lead, researcher}, Store: store, Handlers: func(h *host.Host, address tardigrade.ThreadCoordinate) []tardigrade.Handler {
		services := agent.Services{Model: fake}
		if address.Actor == "lead" {
			services.Tools = library.NewRegistry(library.Agents(h, "researcher", toMessage))
		}
		return agent.Handlers(services)
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	address, err := h.CreateThread("lead", "main", "t1", nil)
	if err != nil {
		t.Fatal(err)
	}
	rt, _ := h.Thread(address)
	input, _ := json.Marshal(agent.MessageInput{Text: "research tardigrades"})
	if err := rt.Invoke("message", "m1", tardigrade.Sender{Kind: "external", ID: "test"}, input); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(10 * time.Second)
	for {
		turns := tardigrade.Read(rt, agent.TurnState).All
		if len(turns) == 2 && turns[1].Outcome == agent.OutcomeCompleted {
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
	threads, _ := h.Threads("researcher", "main")
	if len(threads) != 1 || threads[0].Parent == nil || *threads[0].Parent != address {
		t.Fatalf("threads = %+v", threads)
	}
}
