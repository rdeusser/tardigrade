package agent

import (
	"fmt"
	"slices"

	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
)

// ModelState is the configured model, or nil before configuration.
var ModelState = core.DurableAtom("agent.model", (*ModelConfigured)(nil),
	core.On(func(s *ModelConfigured, e ModelConfigured, _ core.Meta) *ModelConfigured {
		if s != nil {
			panic(fmt.Errorf("model is already configured"))
		}
		return &e
	}),
)

// CallState tracks one inference call of a turn.
type CallState struct {
	CallID   string `json:"callId"`
	Returned bool   `json:"returned"`
}

// EffectState tracks accepted work a turn must drain before cancellation completes.
type EffectState struct {
	Ref     core.Ref `json:"ref"`
	Pending bool     `json:"pending"`
}

// Turn is one request and its progress.
type Turn struct {
	TurnID       string         `json:"turnId"`
	Invocation   *InvocationRef `json:"invocation,omitempty"`
	Settlement   string         `json:"settlement,omitempty"`
	Answer       *string        `json:"answer,omitempty"`
	AnswerCallID string         `json:"answerCallId,omitempty"`
	Calls        []CallState    `json:"calls"`
	Outstanding  []string       `json:"outstanding"`
	Effects      []EffectState  `json:"effects"`
	Failure      *string        `json:"failure,omitempty"`
	Cancellation *string        `json:"cancellation,omitempty"`
}

// Inference is the agent's turn state. TurnID is the earliest unsettled turn; CallID names its next or running model call; Origin is the position that made CallID ready.
type Inference struct {
	Turns      []Turn `json:"turns"`
	TurnID     string `json:"turnId"`
	CallID     string `json:"callId"`
	Origin     *int64 `json:"origin,omitempty"`
	NeedsReply bool   `json:"needsReply"`
	Running    bool   `json:"running"`
	Waiting    bool   `json:"waiting"`
}

func ptr[T any](v T) *T { return &v }

// update replaces the last turn that matches; each identifier matches at most one turn.
func update(turns []Turn, matches func(Turn) bool, change func(Turn) Turn) ([]Turn, bool) {
	for i := len(turns) - 1; i >= 0; i-- {
		if matches(turns[i]) {
			next := slices.Clone(turns)
			next[i] = change(turns[i])
			return next, true
		}
	}
	return turns, false
}

func derive(state Inference, turns []Turn, position int64) Inference {
	var turn *Turn
	for i := range turns {
		if turns[i].Settlement == "" {
			turn = &turns[i]
			break
		}
	}
	next := Inference{Turns: turns}
	if turn == nil {
		next.CallID = fmt.Sprintf("model::%d", 0)
	} else {
		next.TurnID = turn.TurnID
		next.CallID = fmt.Sprintf("model:%s:%d", turn.TurnID, len(turn.Calls))
		for _, call := range turn.Calls {
			if !call.Returned {
				next.CallID, next.Running = call.CallID, true
				break
			}
		}
		next.NeedsReply = turn.Answer == nil
		next.Waiting = len(turn.Outstanding) > 0
	}
	next.Origin = state.Origin
	if next.CallID != state.CallID {
		next.Origin = &position
	}
	return next
}

func returnCall(turn Turn, callID string) Turn {
	turn.Calls = slices.Clone(turn.Calls)
	for i := range turn.Calls {
		if turn.Calls[i].CallID == callID {
			turn.Calls[i].Returned = true
		}
	}
	return turn
}

func settleEffect(state Inference, ref core.Ref, m core.Meta) Inference {
	turns, changed := update(state.Turns, func(t Turn) bool {
		return slices.ContainsFunc(t.Effects, func(e EffectState) bool { return e.Pending && e.Ref == ref })
	}, func(t Turn) Turn {
		t.Effects = slices.Clone(t.Effects)
		for i := range t.Effects {
			if t.Effects[i].Ref == ref {
				t.Effects[i].Pending = false
			}
		}
		return t
	})
	if !changed {
		return state
	}
	return derive(state, turns, m.Position)
}

// InferenceState folds turns, model calls, tool results, and accepted work.
var InferenceState = core.DurableAtom("agent.inference", Inference{CallID: "model::0"},
	core.On(func(s Inference, e TurnRequested, m core.Meta) Inference {
		for _, t := range s.Turns {
			if t.TurnID == e.TurnID {
				panic(fmt.Errorf("duplicate turn: %s", e.TurnID))
			}
			if e.Invocation != nil && t.Invocation != nil && *t.Invocation == *e.Invocation {
				panic(fmt.Errorf("duplicate turn for invocation %s:%s", e.Invocation.Method, e.Invocation.ID))
			}
		}
		turns := append(slices.Clone(s.Turns), Turn{TurnID: e.TurnID, Invocation: e.Invocation, Calls: []CallState{}, Outstanding: []string{}, Effects: []EffectState{}})
		return derive(s, turns, m.Position)
	}),
	core.On(func(s Inference, e AbortRequested, m core.Meta) Inference {
		turns, changed := update(s.Turns, func(t Turn) bool {
			return t.Invocation != nil && *t.Invocation == e.Invocation && t.Settlement == "" && t.Cancellation == nil
		}, func(t Turn) Turn { t.Cancellation = ptr(e.Reason); return t })
		if !changed {
			return s
		}
		return derive(s, turns, m.Position)
	}),
	core.On(func(s Inference, e core.EffectRequested, m core.Meta) Inference {
		turns, changed := update(s.Turns, func(t Turn) bool { return t.TurnID == s.TurnID && s.TurnID != "" }, func(t Turn) Turn {
			t.Effects = append(slices.Clone(t.Effects), EffectState{Ref: e.Ref, Pending: true})
			return t
		})
		if !changed {
			return s
		}
		return derive(s, turns, m.Position)
	}),
	core.On(func(s Inference, e core.EffectSettled, m core.Meta) Inference {
		if e.Result != nil && e.Result.Promise != nil {
			return s
		}
		return settleEffect(s, e.Ref, m)
	}),
	core.On(func(s Inference, e core.PromiseSettled, m core.Meta) Inference { return settleEffect(s, e.Ref, m) }),
	core.On(func(s Inference, e core.EffectCancelled, m core.Meta) Inference { return settleEffect(s, e.Ref, m) }),
	core.On(func(s Inference, e core.EffectDied, m core.Meta) Inference { return settleEffect(s, e.Ref, m) }),
	core.On(func(s Inference, e ModelCalled, m core.Meta) Inference {
		if e.Purpose != PurposeInference {
			return s
		}
		if !s.NeedsReply || s.Running || s.Waiting || e.TurnID != s.TurnID || e.CallID != s.CallID {
			panic(fmt.Errorf("model call is unavailable: %s", e.CallID))
		}
		turns, _ := update(s.Turns, func(t Turn) bool { return t.TurnID == e.TurnID }, func(t Turn) Turn {
			t.Calls = append(slices.Clone(t.Calls), CallState{CallID: e.CallID})
			return t
		})
		return derive(s, turns, m.Position)
	}),
	core.On(func(s Inference, e ModelReturned, m core.Meta) Inference {
		if e.Purpose != PurposeInference {
			return s
		}
		if !s.Running || e.CallID != s.CallID {
			panic(fmt.Errorf("no matching running model call: %s", e.CallID))
		}
		turns, _ := update(s.Turns, func(t Turn) bool {
			return slices.ContainsFunc(t.Calls, func(c CallState) bool { return c.CallID == e.CallID })
		}, func(t Turn) Turn {
			t = returnCall(t, e.CallID)
			t.Answer, t.AnswerCallID = nil, ""
			if len(e.ToolCalls) == 0 {
				t.Answer, t.AnswerCallID = ptr(e.Text), e.CallID
			}
			t.Outstanding = make([]string, len(e.ToolCalls))
			for i, call := range e.ToolCalls {
				t.Outstanding[i] = call.CallID
			}
			return t
		})
		return derive(s, turns, m.Position)
	}),
	core.On(func(s Inference, e ModelFailed, m core.Meta) Inference {
		if !s.Running || e.CallID != s.CallID {
			panic(fmt.Errorf("no matching running model call: %s", e.CallID))
		}
		turns, _ := update(s.Turns, func(t Turn) bool {
			return slices.ContainsFunc(t.Calls, func(c CallState) bool { return c.CallID == e.CallID })
		}, func(t Turn) Turn { t = returnCall(t, e.CallID); t.Failure = ptr(e.Reason); return t })
		return derive(s, turns, m.Position)
	}),
	core.On(func(s Inference, e TurnSettled, m core.Meta) Inference {
		index := slices.IndexFunc(s.Turns, func(t Turn) bool { return t.TurnID == e.TurnID })
		if index < 0 || s.Turns[index].Settlement != "" || e.TurnID != s.TurnID {
			panic(fmt.Errorf("no matching active turn: %s", e.TurnID))
		}
		turn := s.Turns[index]
		switch e.Outcome {
		case OutcomeCompleted:
			if turn.Answer == nil || turn.AnswerCallID != e.CallID || s.Running || s.Waiting {
				panic(fmt.Errorf("turn has no final answer: %s", e.TurnID))
			}
		case OutcomeCancelled:
			if turn.Cancellation == nil || slices.ContainsFunc(turn.Effects, func(w EffectState) bool { return w.Pending }) || len(turn.Outstanding) > 0 {
				panic(fmt.Errorf("turn cancellation must drain accepted work before settlement"))
			}
			turn.Cancellation = ptr(e.Reason)
		default:
			if len(turn.Outstanding) > 0 {
				panic(fmt.Errorf("outstanding tools must settle before ending a turn"))
			}
			turn.Failure = ptr(e.Reason)
		}
		turn.Settlement = e.Outcome
		turns := slices.Clone(s.Turns)
		turns[index] = turn
		return derive(s, turns, m.Position)
	}),
	core.On(func(s Inference, e ToolReturned, m core.Meta) Inference {
		turns, changed := update(s.Turns, func(t Turn) bool { return slices.Contains(t.Outstanding, e.CallID) }, func(t Turn) Turn {
			t.Outstanding = slices.DeleteFunc(slices.Clone(t.Outstanding), func(id string) bool { return id == e.CallID })
			return t
		})
		if !changed {
			return s
		}
		return derive(s, turns, m.Position)
	}),
)

// QueuedTool is a tool call awaiting execution.
type QueuedTool struct {
	Call    ToolCall `json:"call"`
	TurnID  string   `json:"turnId,omitempty"`
	Running bool     `json:"running"`
}

// Tools is the tool queue; Pending is its head and Origin the position that made it the head.
type Tools struct {
	Queue   []QueuedTool `json:"queue"`
	Pending *ToolCall    `json:"pending,omitempty"`
	TurnID  string       `json:"turnId,omitempty"`
	Origin  *int64       `json:"origin,omitempty"`
	Running bool         `json:"running"`
}

func deriveTools(s Tools, queue []QueuedTool, position int64) Tools {
	next := Tools{Queue: queue}
	if len(queue) > 0 {
		head := queue[0]
		next.Pending, next.TurnID, next.Running = &head.Call, head.TurnID, head.Running
		if s.Pending != nil && s.Pending.CallID == head.Call.CallID {
			next.Origin = s.Origin
		} else {
			next.Origin = &position
		}
	}
	return next
}

// PendingTools queues tool calls in reply order.
var PendingTools = core.DurableAtom("agent.tools", Tools{},
	core.On(func(s Tools, e ModelReturned, m core.Meta) Tools {
		if e.Purpose != PurposeInference || len(e.ToolCalls) == 0 {
			return s
		}
		queue := slices.Clone(s.Queue)
		for _, call := range e.ToolCalls {
			queue = append(queue, QueuedTool{Call: call, TurnID: e.TurnID})
		}
		return deriveTools(s, queue, m.Position)
	}),
	core.On(func(s Tools, e ToolCalled, m core.Meta) Tools {
		index := slices.IndexFunc(s.Queue, func(q QueuedTool) bool { return q.Call.CallID == e.CallID && !q.Running })
		if index < 0 {
			return s
		}
		queue := slices.Clone(s.Queue)
		queue[index].Running = true
		return deriveTools(s, queue, m.Position)
	}),
	core.On(func(s Tools, e ToolReturned, m core.Meta) Tools {
		if !slices.ContainsFunc(s.Queue, func(q QueuedTool) bool { return q.Call.CallID == e.CallID }) {
			return s
		}
		queue := slices.DeleteFunc(slices.Clone(s.Queue), func(q QueuedTool) bool { return q.Call.CallID == e.CallID })
		return deriveTools(s, queue, m.Position)
	}),
)

// Permissions is the permission policy and its decisions.
type Permissions struct {
	Policy    *PermissionPolicy    `json:"policy,omitempty"`
	Decisions []PermissionResolved `json:"decisions"`
}

// Decided returns the latest decision for a request.
func (p Permissions) Decided(action, requestID string) *Decision {
	for i := len(p.Decisions) - 1; i >= 0; i-- {
		if p.Decisions[i].Action == action && p.Decisions[i].RequestID == requestID {
			return &p.Decisions[i].Decision
		}
	}
	return nil
}

// PermissionState folds the permission policy and decisions.
var PermissionState = core.DurableAtom("agent.permissions", Permissions{},
	core.On(func(s Permissions, e PermissionConfigured, _ core.Meta) Permissions {
		if s.Policy != nil {
			panic(fmt.Errorf("permission policy is already configured"))
		}
		return Permissions{Policy: &e.Policy, Decisions: s.Decisions}
	}),
	core.On(func(s Permissions, e PermissionUpdated, _ core.Meta) Permissions {
		if s.Policy == nil {
			panic(fmt.Errorf("permission policy is not configured"))
		}
		return Permissions{Policy: &e.Policy, Decisions: s.Decisions}
	}),
	core.On(func(s Permissions, e PermissionResolved, _ core.Meta) Permissions {
		if prior := s.Decided(e.Action, e.RequestID); prior != nil && *prior == e.Decision {
			return s
		}
		return Permissions{Policy: s.Policy, Decisions: append(slices.Clone(s.Decisions), e)}
	}),
)

// Budget is one metric's allowance for the current turn.
type Budget struct {
	Metric    string           `json:"metric"`
	TurnID    string           `json:"turnId,omitempty"`
	Policy    BudgetPolicy     `json:"policy"`
	Granted   int              `json:"granted"`
	Decisions []BudgetResolved `json:"decisions"`
}

// Decided returns the decision recorded for a call.
func (b Budget) Decided(callID string) *BudgetDecision {
	for i := range b.Decisions {
		if b.Decisions[i].CallID == callID {
			return &b.Decisions[i].Decision
		}
	}
	return nil
}

func budgetIndex(s []Budget, metric string) int {
	return slices.IndexFunc(s, func(b Budget) bool { return b.Metric == metric })
}

// BudgetState folds budget policies, per-turn grants, and decisions.
var BudgetState = core.DurableAtom("agent.budget", []Budget(nil),
	core.On(func(s []Budget, e BudgetConfigured, _ core.Meta) []Budget {
		if budgetIndex(s, e.Metric) >= 0 {
			panic(fmt.Errorf("budget policy is already configured: %s", e.Metric))
		}
		return append(slices.Clone(s), Budget{Metric: e.Metric, Policy: e.Policy})
	}),
	core.On(func(s []Budget, e BudgetUpdated, _ core.Meta) []Budget {
		i := budgetIndex(s, e.Metric)
		if i < 0 {
			panic(fmt.Errorf("budget policy is not configured: %s", e.Metric))
		}
		next := slices.Clone(s)
		next[i].Policy = e.Policy
		return next
	}),
	core.On(func(s []Budget, e ModelCalled, _ core.Meta) []Budget {
		if e.Purpose != PurposeInference || !slices.ContainsFunc(s, func(b Budget) bool { return b.TurnID != e.TurnID }) {
			return s
		}
		next := slices.Clone(s)
		for i := range next {
			if next[i].TurnID != e.TurnID {
				next[i] = Budget{Metric: next[i].Metric, TurnID: e.TurnID, Policy: next[i].Policy}
			}
		}
		return next
	}),
	core.On(func(s []Budget, e TurnSettled, _ core.Meta) []Budget {
		next := slices.Clone(s)
		for i := range next {
			if next[i].TurnID == e.TurnID {
				next[i] = Budget{Metric: next[i].Metric, Policy: next[i].Policy}
			}
		}
		return next
	}),
	core.On(func(s []Budget, e BudgetResolved, _ core.Meta) []Budget {
		i := budgetIndex(s, e.Metric)
		if i < 0 {
			panic(fmt.Errorf("budget policy is not configured: %s", e.Metric))
		}
		if prior := s[i].Decided(e.CallID); prior != nil {
			if *prior != e.Decision {
				panic(fmt.Errorf("conflicting budget resolution: %s:%s", e.Metric, e.CallID))
			}
			return s
		}
		next := slices.Clone(s)
		if e.Decision.Allowed {
			next[i].Granted += e.Decision.Additional
		}
		next[i].Decisions = append(slices.Clone(next[i].Decisions), e)
		return next
	}),
)

// TurnSpend is a turn's measured cost; USD is nil once any call reported no cost.
type TurnSpend struct {
	TurnID    string   `json:"turnId"`
	ToolCalls int      `json:"toolCalls"`
	Input     int      `json:"input"`
	Output    int      `json:"output"`
	USD       *float64 `json:"usd"`
}

// Spend is per-turn cost with the turn attribution of pending tool calls.
type Spend struct {
	Turns []TurnSpend       `json:"turns"`
	Tools map[string]string `json:"tools"`
}

// SpendOf returns a turn's spend.
func (s Spend) SpendOf(turnID string) TurnSpend {
	for _, t := range s.Turns {
		if t.TurnID == turnID {
			return t
		}
	}
	return TurnSpend{TurnID: turnID}
}

func changeTurn(s Spend, turnID string, change func(TurnSpend) TurnSpend) Spend {
	i := slices.IndexFunc(s.Turns, func(t TurnSpend) bool { return t.TurnID == turnID })
	if i < 0 {
		return s
	}
	turns := slices.Clone(s.Turns)
	turns[i] = change(turns[i])
	return Spend{Turns: turns, Tools: s.Tools}
}

// SpendState counts budgeted tool calls, tokens, and reported cost per turn.
var SpendState = core.DurableAtom("agent.spend", Spend{},
	core.On(func(s Spend, e TurnRequested, _ core.Meta) Spend {
		return Spend{Turns: append(slices.Clone(s.Turns), TurnSpend{TurnID: e.TurnID, USD: ptr(0.0)}), Tools: s.Tools}
	}),
	core.On(func(s Spend, e ModelReturned, _ core.Meta) Spend {
		if e.TurnID == "" {
			return s
		}
		tools := map[string]string{}
		for k, v := range s.Tools {
			tools[k] = v
		}
		for _, call := range e.ToolCalls {
			tools[call.CallID] = e.TurnID
		}
		s = Spend{Turns: s.Turns, Tools: tools}
		return changeTurn(s, e.TurnID, func(t TurnSpend) TurnSpend {
			if e.Usage != nil {
				t.Input += e.Usage.Input
				t.Output += e.Usage.Output
			}
			if t.USD != nil && e.Usage != nil && e.Usage.USD != nil {
				t.USD = ptr(*t.USD + *e.Usage.USD)
			} else {
				t.USD = nil
			}
			return t
		})
	}),
	core.On(func(s Spend, e ToolCalled, _ core.Meta) Spend {
		turnID, ok := s.Tools[e.CallID]
		if !ok {
			return s
		}
		tools := map[string]string{}
		for k, v := range s.Tools {
			if k != e.CallID {
				tools[k] = v
			}
		}
		s = Spend{Turns: s.Turns, Tools: tools}
		if !e.Counted {
			return s
		}
		return changeTurn(s, turnID, func(t TurnSpend) TurnSpend { t.ToolCalls++; return t })
	}),
)

// Compaction is the summary state. Through counts conversation messages the summary covers; Anchor is the latest provider-measured context size.
type Compaction struct {
	Origin    *int64   `json:"origin,omitempty"`
	Through   int      `json:"through"`
	Attempts  int      `json:"attempts"`
	Summary   string   `json:"summary"`
	Failure   *string  `json:"failure,omitempty"`
	Pending   *Pending `json:"pending,omitempty"`
	Anchor    *Anchor  `json:"anchor,omitempty"`
	UsageCall *Pending `json:"usageCall,omitempty"`
}

// Pending is an accepted summary or inference call.
type Pending struct {
	CallID  string    `json:"callId"`
	Through int       `json:"through"`
	Model   model.Ref `json:"model"`
}

// Anchor is the token count a provider reported for the context through the reply at Position.
type Anchor struct {
	Tokens   int       `json:"tokens"`
	Position int64     `json:"position"`
	Through  int       `json:"through"`
	Model    model.Ref `json:"model"`
}

// CompactionState folds summaries and measured context sizes.
var CompactionState = core.DurableAtom("agent.compaction", Compaction{},
	core.On(func(s Compaction, e ModelCalled, _ core.Meta) Compaction {
		if e.Purpose == PurposeInference {
			s.UsageCall = &Pending{CallID: e.CallID, Through: s.Through, Model: e.Model}
			return s
		}
		if s.Pending != nil || e.Through <= s.Through {
			panic(fmt.Errorf("compaction call is unavailable: %s", e.CallID))
		}
		s.Attempts++
		s.Pending = &Pending{CallID: e.CallID, Through: e.Through, Model: e.Model}
		return s
	}),
	core.On(func(s Compaction, e ModelReturned, m core.Meta) Compaction {
		s.Origin = &m.Position
		if e.Purpose == PurposeCompaction {
			if s.Pending == nil || s.Pending.CallID != e.CallID {
				panic(fmt.Errorf("no matching running compaction: %s", e.CallID))
			}
			return Compaction{Origin: s.Origin, Through: s.Pending.Through, Attempts: s.Attempts, Summary: e.Text}
		}
		if call := s.UsageCall; call != nil && call.CallID == e.CallID {
			s.UsageCall = nil
			if e.Usage != nil && e.Usage.Input > 0 && call.Through == s.Through {
				s.Anchor = &Anchor{Tokens: e.Usage.Input + e.Usage.Output, Position: m.Position, Through: call.Through, Model: call.Model}
			}
		}
		return s
	}),
	core.On(func(s Compaction, e ModelFailed, _ core.Meta) Compaction {
		if s.UsageCall != nil && s.UsageCall.CallID == e.CallID {
			s.UsageCall = nil
		}
		return s
	}),
	core.On(func(s Compaction, e CompactionFailed, m core.Meta) Compaction {
		if s.Pending == nil || s.Pending.CallID != e.CallID {
			panic(fmt.Errorf("no matching running compaction: %s", e.CallID))
		}
		s.Origin, s.Pending, s.Failure = &m.Position, nil, ptr(e.Reason)
		return s
	}),
	core.On(func(s Compaction, e TurnSettled, m core.Meta) Compaction {
		s.Origin, s.Failure = &m.Position, nil
		return s
	}),
)

// Entry is one conversation message with the turn and journal position that produced it.
type Entry struct {
	TurnID   string        `json:"turnId"`
	Position int64         `json:"position"`
	Message  model.Message `json:"message"`
	Calls    []ToolCall    `json:"calls,omitempty"`
}

// Conversation is the ordered model-facing history.
var Conversation = core.DurableAtom("agent.conversation", []Entry(nil),
	core.On(func(s []Entry, e TurnRequested, m core.Meta) []Entry {
		message := model.Message{Role: "user", Text: e.Text}
		if len(e.Content) > 0 {
			message = model.Message{Role: "user", Content: e.Content}
		}
		return append(slices.Clone(s), Entry{TurnID: e.TurnID, Position: m.Position, Message: message})
	}),
	core.On(func(s []Entry, e ModelReturned, m core.Meta) []Entry {
		if e.Purpose != PurposeInference || e.TurnID == "" {
			return s
		}
		calls := make([]model.ToolCall, len(e.ToolCalls))
		for i, c := range e.ToolCalls {
			calls[i] = model.ToolCall{ID: c.ProviderID, Name: c.Name, Input: c.Input}
		}
		message := model.Message{Role: "assistant", Text: e.Text, ToolCalls: calls, Reasoning: e.Reasoning, Continuation: e.Continuation}
		return append(slices.Clone(s), Entry{TurnID: e.TurnID, Position: m.Position, Message: message, Calls: e.ToolCalls})
	}),
	core.On(func(s []Entry, e ToolReturned, m core.Meta) []Entry {
		owner, turnID := e.Tool, e.TurnID
		for i := len(s) - 1; i >= 0 && owner == nil; i-- {
			for _, c := range s[i].Calls {
				if c.CallID == e.CallID {
					owner, turnID = &ToolOwner{Name: c.Name, ProviderID: c.ProviderID}, s[i].TurnID
				}
			}
		}
		if owner == nil {
			return s
		}
		message := model.Message{Role: "tool", ToolCallID: owner.ProviderID, Name: owner.Name, Text: model.TextOf(e.Content, "\n")}
		if e.Error != nil {
			message.Text, message.Error = *e.Error, true
		} else if slices.ContainsFunc(e.Content, func(p model.Part) bool { return p.Type == "file" }) {
			message.Content = e.Content
		}
		return append(slices.Clone(s), Entry{TurnID: turnID, Position: m.Position, Message: message})
	}),
)

// Submission is a background tool result the agent awaits.
type Submission struct {
	CallID  string          `json:"callId"`
	Promise library.Promise `json:"promise"`
}

// Submissions tracks background tool promises until their results start turns.
var Submissions = core.DurableAtom("agent.submissions", []Submission(nil),
	core.On(func(s []Submission, e ToolReturned, _ core.Meta) []Submission {
		if e.Promise == nil {
			return s
		}
		return append(slices.Clone(s), Submission{CallID: e.CallID, Promise: *e.Promise})
	}),
	core.On(func(s []Submission, e TurnRequested, _ core.Meta) []Submission {
		return slices.DeleteFunc(slices.Clone(s), func(item Submission) bool { return e.TurnID == "promise:"+item.CallID })
	}),
)
