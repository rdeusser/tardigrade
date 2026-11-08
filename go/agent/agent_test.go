package agent_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rdeusser/tardigrade/go/agent"
	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
)

type scripted struct {
	mu       sync.Mutex
	requests []model.Request
	reply    func(n int, r model.Request) (model.Reply, error)
}

func (s *scripted) Generate(ctx context.Context, r model.Request, onDelta func(model.Delta)) (model.Reply, error) {
	s.mu.Lock()
	n := len(s.requests)
	s.requests = append(s.requests, r)
	s.mu.Unlock()
	reply, err := s.reply(n, r)
	if onDelta != nil && reply.Text != "" {
		onDelta(model.Delta{Type: "text", Text: reply.Text})
	}
	return reply, err
}

func (s *scripted) calls() []model.Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]model.Request(nil), s.requests...)
}

var ref = model.Ref{Provider: "test", ModelID: "m"}

func text(t string) model.Reply { return model.Reply{Text: t, FinishReason: model.FinishStop} }

func toolCall(name, input string) model.Reply {
	return model.Reply{ToolCalls: []model.ToolCall{{ID: "p1", Name: name, Input: json.RawMessage(input)}}, FinishReason: model.FinishToolCalls}
}

type harness struct {
	t     *testing.T
	rt    *core.Runtime
	model *scripted
}

func run(t *testing.T, o agent.Options, s agent.Services, reply func(n int, r model.Request) (model.Reply, error)) *harness {
	t.Helper()
	fake := &scripted{reply: reply}
	s.Model = fake
	if o.Name == "" {
		o.Name = "assistant"
	}
	o.Model, o.System = ref, "You help."
	if o.ContextWindowTokens == 0 {
		o.ContextWindowTokens = 100_000
	}
	rt, err := core.Open(core.Options{Actor: agent.New(o), Journal: core.NewMemoryJournal(), Acts: agent.Acts(s)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = rt.Close() })
	return &harness{t: t, rt: rt, model: fake}
}

func (h *harness) say(id, text string) {
	h.t.Helper()
	input, _ := json.Marshal(agent.MessageInput{Text: text})
	if err := h.rt.Invoke("message", id, core.Sender{Kind: "external", ID: "test"}, input); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) await(id string) core.MethodState {
	h.t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		changed := h.rt.Changed()
		state, err := h.rt.Call("message", id)
		if err != nil {
			h.t.Fatal(err)
		}
		if state.Status != core.StatusPending {
			return state
		}
		if problems := h.rt.Status().Problems; len(problems) > 0 {
			h.t.Fatalf("runtime problems: %v", problems)
		}
		select {
		case <-changed:
		case <-time.After(50 * time.Millisecond):
		case <-deadline:
			h.t.Fatalf("call %s still pending; journal %v", id, h.types())
		}
	}
}

func (h *harness) types() []string {
	records, _ := h.rt.Records(0, h.rt.Length())
	names := make([]string, len(records))
	for i, r := range records {
		names[i] = r.Event.EventType()
	}
	return names
}

func output(t *testing.T, state core.MethodState) string {
	t.Helper()
	var out agent.MessageOutput
	if state.Status != core.StatusCompleted || json.Unmarshal(state.Output, &out) != nil {
		t.Fatalf("state = %+v", state)
	}
	return out.Text
}

func TestTurnCompletesWithModelText(t *testing.T) {
	h := run(t, agent.Options{}, agent.Services{}, func(int, model.Request) (model.Reply, error) { return text("hello"), nil })
	h.say("m1", "hi")
	if got := output(t, h.await("m1")); got != "hello" {
		t.Fatalf("reply = %q", got)
	}
	request := h.model.calls()[0]
	if request.System != "You help." || len(request.Messages) != 1 || request.Messages[0].Text != "hi" {
		t.Fatalf("request = %+v", request)
	}
}

func TestToolResultReturnsToModel(t *testing.T) {
	notes := library.Notes(0)
	h := run(t, agent.Options{Tools: library.Specs(notes)}, agent.Services{Tools: library.NewRegistry(notes)}, func(n int, r model.Request) (model.Reply, error) {
		if n == 0 {
			return toolCall("write_text", `{"key":"k","value":"v"}`), nil
		}
		return text("stored"), nil
	})
	h.say("m1", "remember v")
	if got := output(t, h.await("m1")); got != "stored" {
		t.Fatalf("reply = %q", got)
	}
	second := h.model.calls()[1].Messages
	if len(second) != 3 || second[2].Role != "tool" || second[2].ToolCallID != "p1" || !strings.Contains(second[2].Text, `"chars":1`) {
		t.Fatalf("second request messages = %+v", second)
	}
}

func TestPermissionDenialReachesModel(t *testing.T) {
	notes := library.Notes(0)
	policy := agent.PermissionPolicy{Default: agent.Ask, Actions: map[string]agent.PermissionRule{}}
	h := run(t, agent.Options{Tools: library.Specs(notes), Permissions: &policy}, agent.Services{Tools: library.NewRegistry(notes),
		Permission: func(_ *core.Execution, r agent.PermissionRequest) (agent.Decision, error) {
			return agent.Decision{Allowed: false, Reason: "not " + r.Resource}, nil
		}}, func(n int, r model.Request) (model.Reply, error) {
		if n == 0 {
			return toolCall("write_text", `{"key":"k","value":"v"}`), nil
		}
		return text("denied"), nil
	})
	h.say("m1", "write")
	output(t, h.await("m1"))
	last := h.model.calls()[1].Messages[2]
	if !last.Error || last.Text != "not write_text" {
		t.Fatalf("tool message = %+v", last)
	}
}

func TestBudgetExhaustionDeniesTools(t *testing.T) {
	notes := library.Notes(0)
	h := run(t, agent.Options{Tools: library.Specs(notes), Budget: &agent.BudgetPolicy{Limit: 1, OnExhausted: "deny"}}, agent.Services{Tools: library.NewRegistry(notes)}, func(n int, r model.Request) (model.Reply, error) {
		if n < 2 {
			return toolCall("list_texts", `{}`), nil
		}
		return text("done"), nil
	})
	h.say("m1", "go")
	output(t, h.await("m1"))
	calls := h.model.calls()
	if len(calls[2].Tools) != 0 || !strings.Contains(calls[2].System, "Answer without tools.") {
		t.Fatalf("third call tools = %v system = %q", calls[2].Tools, calls[2].System)
	}
	if denied := calls[2].Messages[4]; !denied.Error || !strings.Contains(denied.Text, "exhausted") {
		t.Fatalf("second tool result = %+v", denied)
	}
}

func TestCompactionSummarizesOldTurns(t *testing.T) {
	h := run(t, agent.Options{ContextWindowTokens: 200}, agent.Services{}, func(n int, r model.Request) (model.Reply, error) {
		if strings.HasPrefix(r.System, "Summarize") {
			return text("SUMMARY"), nil
		}
		return text(strings.Repeat("x", 200)), nil
	})
	for _, id := range []string{"a", "b", "c"} {
		h.say(id, strings.Repeat("y", 200))
		output(t, h.await(id))
	}
	var compacted bool
	for _, r := range h.model.calls() {
		if len(r.Messages) > 0 && strings.Contains(r.Messages[0].Text, "SUMMARY") {
			compacted = true
		}
	}
	if !compacted {
		t.Fatalf("no request carried the summary; journal %v", h.types())
	}
}

func TestCancellationStopsTurn(t *testing.T) {
	started := make(chan struct{})
	h := run(t, agent.Options{}, agent.Services{}, func(int, model.Request) (model.Reply, error) {
		close(started)
		time.Sleep(100 * time.Millisecond)
		return text("late"), nil
	})
	h.say("m1", "slow")
	<-started
	if err := h.rt.CancelCall("message", "m1", "user stopped", core.Sender{Kind: "external", ID: "test"}); err != nil {
		t.Fatal(err)
	}
	if state := h.await("m1"); state.Status != core.StatusCancelled || state.Reason != "user stopped" {
		t.Fatalf("state = %+v; journal %v", state, h.types())
	}
}

func TestBackgroundToolStartsFollowUpTurn(t *testing.T) {
	alarm := library.Alarm()
	h := run(t, agent.Options{Tools: library.Specs(alarm)}, agent.Services{Tools: library.NewRegistry(alarm)}, func(n int, r model.Request) (model.Reply, error) {
		switch n {
		case 0:
			return toolCall("alarm__set_alarm", `{"message":"wake","afterSeconds":0.05}`), nil
		case 1:
			return text("alarm set"), nil
		}
		return text("woke up"), nil
	})
	h.say("m1", "remind me")
	output(t, h.await("m1"))
	deadline := time.After(5 * time.Second)
	for len(h.model.calls()) < 3 {
		select {
		case <-h.rt.Changed():
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatalf("no follow-up turn; journal %v", h.types())
		}
	}
	last := h.model.calls()[2].Messages
	if !strings.Contains(last[len(last)-1].Text, `"wake"`) {
		t.Fatalf("follow-up turn = %+v", last[len(last)-1])
	}
}
