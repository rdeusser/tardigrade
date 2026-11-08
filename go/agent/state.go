package agent

import (
	"fmt"
	"maps"
	"slices"

	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// ModelState is the configured model, or nil before configuration.
var ModelState = tardigrade.DurableAtom("agent.model", (*ModelConfigured)(nil),
	tardigrade.On(func(s *ModelConfigured, e ModelConfigured, _ tardigrade.Meta) *ModelConfigured {
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
	Ref     tardigrade.Ref `json:"ref"`
	Pending bool           `json:"pending"`
}

// Turn is one request and its progress. Outcome is empty while the turn runs.
type Turn struct {
	TurnID       string         `json:"turnId"`
	Invocation   *InvocationRef `json:"invocation,omitempty"`
	Outcome      Outcome        `json:"outcome,omitempty"`
	Answer       *string        `json:"answer,omitempty"`
	AnswerCallID string         `json:"answerCallId,omitempty"`
	Calls        []CallState    `json:"calls"`
	Outstanding  []string       `json:"outstanding"`
	Effects      []EffectState  `json:"effects"`
	Failure      *string        `json:"failure,omitempty"`
	Cancellation *string        `json:"cancellation,omitempty"`
}

// Turns is the agent's turn history. Current is the earliest unfinished turn and Call its running or next model call; CallOrigin is the journal position that made Call ready.
type Turns struct {
	All        []Turn `json:"all"`
	Current    string `json:"current"`
	Call       string `json:"call"`
	CallOrigin *int64 `json:"callOrigin,omitempty"`
	// NeedsReply is set while the current turn has no answer.
	NeedsReply bool `json:"needsReply"`
	// Calling is set while a model call of the current turn runs.
	Calling bool `json:"calling"`
	// AwaitingTools is set while the current turn waits for tool results.
	AwaitingTools bool `json:"awaitingTools"`
}

// replaceTurn replaces the last turn that matches; each identifier matches at most one turn.
func replaceTurn(all []Turn, matches func(Turn) bool, change func(Turn) Turn) ([]Turn, bool) {
	for i := len(all) - 1; i >= 0; i-- {
		if matches(all[i]) {
			next := slices.Clone(all)
			next[i] = change(all[i])
			return next, true
		}
	}
	return all, false
}

// advance returns the turns with history all, recomputing the current turn and its next call; a new call takes position as its origin.
func (t Turns) advance(all []Turn, position int64) Turns {
	next := Turns{All: all, Call: "model::0", CallOrigin: t.CallOrigin}
	for _, turn := range all {
		if turn.Outcome != "" {
			continue
		}
		next.Current = turn.TurnID
		next.Call = fmt.Sprintf("model:%s:%d", turn.TurnID, len(turn.Calls))
		for _, call := range turn.Calls {
			if !call.Returned {
				next.Call, next.Calling = call.CallID, true
				break
			}
		}
		next.NeedsReply = turn.Answer == nil
		next.AwaitingTools = len(turn.Outstanding) > 0
		break
	}
	if next.Call != t.Call {
		next.CallOrigin = new(position)
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

func hasCall(callID string) func(Turn) bool {
	return func(turn Turn) bool {
		return slices.ContainsFunc(turn.Calls, func(c CallState) bool { return c.CallID == callID })
	}
}

func finishEffect(turns Turns, ref tardigrade.Ref, m tardigrade.Meta) Turns {
	all, changed := replaceTurn(turns.All, func(turn Turn) bool {
		return slices.ContainsFunc(turn.Effects, func(e EffectState) bool { return e.Pending && e.Ref == ref })
	}, func(turn Turn) Turn {
		turn.Effects = slices.Clone(turn.Effects)
		for i := range turn.Effects {
			if turn.Effects[i].Ref == ref {
				turn.Effects[i].Pending = false
			}
		}
		return turn
	})
	if !changed {
		return turns
	}
	return turns.advance(all, m.Position)
}

// TurnState folds turns, model calls, tool results, and accepted work.
var TurnState = tardigrade.DurableAtom("agent.turns", Turns{Call: "model::0"},
	tardigrade.On(func(turns Turns, e TurnRequested, m tardigrade.Meta) Turns {
		for _, turn := range turns.All {
			if turn.TurnID == e.TurnID {
				panic(fmt.Errorf("duplicate turn: %s", e.TurnID))
			}
			if e.Invocation != nil && turn.Invocation != nil && *turn.Invocation == *e.Invocation {
				panic(fmt.Errorf("duplicate turn for invocation %s:%s", e.Invocation.Method, e.Invocation.ID))
			}
		}
		all := append(slices.Clone(turns.All), Turn{TurnID: e.TurnID, Invocation: e.Invocation, Calls: []CallState{}, Outstanding: []string{}, Effects: []EffectState{}})
		return turns.advance(all, m.Position)
	}),
	tardigrade.On(func(turns Turns, e AbortRequested, m tardigrade.Meta) Turns {
		all, changed := replaceTurn(turns.All, func(turn Turn) bool {
			return turn.Invocation != nil && *turn.Invocation == e.Invocation && turn.Outcome == "" && turn.Cancellation == nil
		}, func(turn Turn) Turn { turn.Cancellation = new(e.Reason); return turn })
		if !changed {
			return turns
		}
		return turns.advance(all, m.Position)
	}),
	tardigrade.On(func(turns Turns, e tardigrade.EffectRequested, m tardigrade.Meta) Turns {
		all, changed := replaceTurn(turns.All, func(turn Turn) bool { return turns.Current != "" && turn.TurnID == turns.Current }, func(turn Turn) Turn {
			turn.Effects = append(slices.Clone(turn.Effects), EffectState{Ref: e.Ref, Pending: true})
			return turn
		})
		if !changed {
			return turns
		}
		return turns.advance(all, m.Position)
	}),
	tardigrade.On(func(turns Turns, e tardigrade.EffectFinished, m tardigrade.Meta) Turns {
		if e.Result != nil && e.Result.Promise != nil {
			return turns
		}
		return finishEffect(turns, e.Ref, m)
	}),
	tardigrade.On(func(turns Turns, e tardigrade.PromiseFinished, m tardigrade.Meta) Turns {
		return finishEffect(turns, e.Ref, m)
	}),
	tardigrade.On(func(turns Turns, e tardigrade.EffectCancelled, m tardigrade.Meta) Turns {
		return finishEffect(turns, e.Ref, m)
	}),
	tardigrade.On(func(turns Turns, e tardigrade.EffectDied, m tardigrade.Meta) Turns {
		return finishEffect(turns, e.Ref, m)
	}),
	tardigrade.On(func(turns Turns, e ModelCalled, m tardigrade.Meta) Turns {
		if e.Purpose != PurposeInference {
			return turns
		}
		if !turns.NeedsReply || turns.Calling || turns.AwaitingTools || e.TurnID != turns.Current || e.CallID != turns.Call {
			panic(fmt.Errorf("model call is unavailable: %s", e.CallID))
		}
		all, _ := replaceTurn(turns.All, func(turn Turn) bool { return turn.TurnID == e.TurnID }, func(turn Turn) Turn {
			turn.Calls = append(slices.Clone(turn.Calls), CallState{CallID: e.CallID})
			return turn
		})
		return turns.advance(all, m.Position)
	}),
	tardigrade.On(func(turns Turns, e ModelReturned, m tardigrade.Meta) Turns {
		if e.Purpose != PurposeInference {
			return turns
		}
		if !turns.Calling || e.CallID != turns.Call {
			panic(fmt.Errorf("no matching running model call: %s", e.CallID))
		}
		all, _ := replaceTurn(turns.All, hasCall(e.CallID), func(turn Turn) Turn {
			turn = returnCall(turn, e.CallID)
			turn.Answer, turn.AnswerCallID = nil, ""
			if len(e.ToolCalls) == 0 {
				turn.Answer, turn.AnswerCallID = new(e.Text), e.CallID
			}
			turn.Outstanding = make([]string, len(e.ToolCalls))
			for i, call := range e.ToolCalls {
				turn.Outstanding[i] = call.CallID
			}
			return turn
		})
		return turns.advance(all, m.Position)
	}),
	tardigrade.On(func(turns Turns, e ModelFailed, m tardigrade.Meta) Turns {
		if !turns.Calling || e.CallID != turns.Call {
			panic(fmt.Errorf("no matching running model call: %s", e.CallID))
		}
		all, _ := replaceTurn(turns.All, hasCall(e.CallID), func(turn Turn) Turn {
			turn = returnCall(turn, e.CallID)
			turn.Failure = new(e.Reason)
			return turn
		})
		return turns.advance(all, m.Position)
	}),
	tardigrade.On(func(turns Turns, e TurnFinished, m tardigrade.Meta) Turns {
		index := slices.IndexFunc(turns.All, func(turn Turn) bool { return turn.TurnID == e.TurnID })
		if index < 0 || turns.All[index].Outcome != "" || e.TurnID != turns.Current {
			panic(fmt.Errorf("no matching active turn: %s", e.TurnID))
		}
		turn := turns.All[index]
		switch e.Outcome {
		case OutcomeCompleted:
			if turn.Answer == nil || turn.AnswerCallID != e.CallID || turns.Calling || turns.AwaitingTools {
				panic(fmt.Errorf("turn has no final answer: %s", e.TurnID))
			}
		case OutcomeCancelled:
			if turn.Cancellation == nil || slices.ContainsFunc(turn.Effects, func(w EffectState) bool { return w.Pending }) || len(turn.Outstanding) > 0 {
				panic(fmt.Errorf("turn cancellation must drain accepted work before the turn finishes"))
			}
			turn.Cancellation = new(e.Reason)
		default:
			if len(turn.Outstanding) > 0 {
				panic(fmt.Errorf("outstanding tools must finish before the turn ends"))
			}
			turn.Failure = new(e.Reason)
		}
		turn.Outcome = e.Outcome
		all := slices.Clone(turns.All)
		all[index] = turn
		return turns.advance(all, m.Position)
	}),
	tardigrade.On(func(turns Turns, e ToolReturned, m tardigrade.Meta) Turns {
		all, changed := replaceTurn(turns.All, func(turn Turn) bool { return slices.Contains(turn.Outstanding, e.CallID) }, func(turn Turn) Turn {
			turn.Outstanding = slices.DeleteFunc(slices.Clone(turn.Outstanding), func(id string) bool { return id == e.CallID })
			return turn
		})
		if !changed {
			return turns
		}
		return turns.advance(all, m.Position)
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
var PendingTools = tardigrade.DurableAtom("agent.tools", Tools{},
	tardigrade.On(func(s Tools, e ModelReturned, m tardigrade.Meta) Tools {
		if e.Purpose != PurposeInference || len(e.ToolCalls) == 0 {
			return s
		}
		queue := slices.Clone(s.Queue)
		for _, call := range e.ToolCalls {
			queue = append(queue, QueuedTool{Call: call, TurnID: e.TurnID})
		}
		return deriveTools(s, queue, m.Position)
	}),
	tardigrade.On(func(s Tools, e ToolCalled, m tardigrade.Meta) Tools {
		index := slices.IndexFunc(s.Queue, func(q QueuedTool) bool { return q.Call.CallID == e.CallID && !q.Running })
		if index < 0 {
			return s
		}
		queue := slices.Clone(s.Queue)
		queue[index].Running = true
		return deriveTools(s, queue, m.Position)
	}),
	tardigrade.On(func(s Tools, e ToolReturned, m tardigrade.Meta) Tools {
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
var PermissionState = tardigrade.DurableAtom("agent.permissions", Permissions{},
	tardigrade.On(func(s Permissions, e PermissionConfigured, _ tardigrade.Meta) Permissions {
		if s.Policy != nil {
			panic(fmt.Errorf("permission policy is already configured"))
		}
		return Permissions{Policy: &e.Policy, Decisions: s.Decisions}
	}),
	tardigrade.On(func(s Permissions, e PermissionUpdated, _ tardigrade.Meta) Permissions {
		if s.Policy == nil {
			panic(fmt.Errorf("permission policy is not configured"))
		}
		return Permissions{Policy: &e.Policy, Decisions: s.Decisions}
	}),
	tardigrade.On(func(s Permissions, e PermissionResolved, _ tardigrade.Meta) Permissions {
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
var BudgetState = tardigrade.DurableAtom("agent.budget", []Budget(nil),
	tardigrade.On(func(s []Budget, e BudgetConfigured, _ tardigrade.Meta) []Budget {
		if budgetIndex(s, e.Metric) >= 0 {
			panic(fmt.Errorf("budget policy is already configured: %s", e.Metric))
		}
		return append(slices.Clone(s), Budget{Metric: e.Metric, Policy: e.Policy})
	}),
	tardigrade.On(func(s []Budget, e BudgetUpdated, _ tardigrade.Meta) []Budget {
		i := budgetIndex(s, e.Metric)
		if i < 0 {
			panic(fmt.Errorf("budget policy is not configured: %s", e.Metric))
		}
		next := slices.Clone(s)
		next[i].Policy = e.Policy
		return next
	}),
	tardigrade.On(func(s []Budget, e ModelCalled, _ tardigrade.Meta) []Budget {
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
	tardigrade.On(func(s []Budget, e TurnFinished, _ tardigrade.Meta) []Budget {
		next := slices.Clone(s)
		for i := range next {
			if next[i].TurnID == e.TurnID {
				next[i] = Budget{Metric: next[i].Metric, Policy: next[i].Policy}
			}
		}
		return next
	}),
	tardigrade.On(func(s []Budget, e BudgetResolved, _ tardigrade.Meta) []Budget {
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
var SpendState = tardigrade.DurableAtom("agent.spend", Spend{},
	tardigrade.On(func(s Spend, e TurnRequested, _ tardigrade.Meta) Spend {
		return Spend{Turns: append(slices.Clone(s.Turns), TurnSpend{TurnID: e.TurnID, USD: new(0.0)}), Tools: s.Tools}
	}),
	tardigrade.On(func(s Spend, e ModelReturned, _ tardigrade.Meta) Spend {
		if e.TurnID == "" {
			return s
		}
		tools := map[string]string{}
		maps.Copy(tools, s.Tools)
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
				t.USD = new(*t.USD + *e.Usage.USD)
			} else {
				t.USD = nil
			}
			return t
		})
	}),
	tardigrade.On(func(s Spend, e ToolCalled, _ tardigrade.Meta) Spend {
		turnID, ok := s.Tools[e.CallID]
		if !ok {
			return s
		}
		tools := maps.Clone(s.Tools)
		delete(tools, e.CallID)
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
var CompactionState = tardigrade.DurableAtom("agent.compaction", Compaction{},
	tardigrade.On(func(s Compaction, e ModelCalled, _ tardigrade.Meta) Compaction {
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
	tardigrade.On(func(s Compaction, e ModelReturned, m tardigrade.Meta) Compaction {
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
	tardigrade.On(func(s Compaction, e ModelFailed, _ tardigrade.Meta) Compaction {
		if s.UsageCall != nil && s.UsageCall.CallID == e.CallID {
			s.UsageCall = nil
		}
		return s
	}),
	tardigrade.On(func(s Compaction, e CompactionFailed, m tardigrade.Meta) Compaction {
		if s.Pending == nil || s.Pending.CallID != e.CallID {
			panic(fmt.Errorf("no matching running compaction: %s", e.CallID))
		}
		s.Origin, s.Pending, s.Failure = &m.Position, nil, new(e.Reason)
		return s
	}),
	tardigrade.On(func(s Compaction, e TurnFinished, m tardigrade.Meta) Compaction {
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
var Conversation = tardigrade.DurableAtom("agent.conversation", []Entry(nil),
	tardigrade.On(func(s []Entry, e TurnRequested, m tardigrade.Meta) []Entry {
		message := model.Message{Role: model.RoleUser, Text: e.Text}
		if len(e.Content) > 0 {
			message = model.Message{Role: model.RoleUser, Content: e.Content}
		}
		return append(slices.Clone(s), Entry{TurnID: e.TurnID, Position: m.Position, Message: message})
	}),
	tardigrade.On(func(s []Entry, e ModelReturned, m tardigrade.Meta) []Entry {
		if e.Purpose != PurposeInference || e.TurnID == "" {
			return s
		}
		calls := make([]model.ToolCall, len(e.ToolCalls))
		for i, c := range e.ToolCalls {
			calls[i] = model.ToolCall{ID: c.ProviderID, Name: c.Name, Input: c.Input}
		}
		message := model.Message{Role: model.RoleAssistant, Text: e.Text, ToolCalls: calls, Reasoning: e.Reasoning, Continuation: e.Continuation}
		return append(slices.Clone(s), Entry{TurnID: e.TurnID, Position: m.Position, Message: message, Calls: e.ToolCalls})
	}),
	tardigrade.On(func(s []Entry, e ToolReturned, m tardigrade.Meta) []Entry {
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
		message := model.Message{Role: model.RoleTool, ToolCallID: owner.ProviderID, Name: owner.Name, Text: model.TextOf(e.Content, "\n")}
		if e.Error != nil {
			message.Text, message.Error = *e.Error, true
		} else if slices.ContainsFunc(e.Content, func(p model.Part) bool { return p.Type == model.PartFile }) {
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
var Submissions = tardigrade.DurableAtom("agent.submissions", []Submission(nil),
	tardigrade.On(func(s []Submission, e ToolReturned, _ tardigrade.Meta) []Submission {
		if e.Promise == nil {
			return s
		}
		return append(slices.Clone(s), Submission{CallID: e.CallID, Promise: *e.Promise})
	}),
	tardigrade.On(func(s []Submission, e TurnRequested, _ tardigrade.Meta) []Submission {
		return slices.DeleteFunc(slices.Clone(s), func(item Submission) bool { return e.TurnID == "promise:"+item.CallID })
	}),
)
