package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/reactive"
)

// DefaultRequestBudgetTool names the tool a model calls to request more tool calls.
const DefaultRequestBudgetTool = "request_budget"

// Options configure an agent actor.
type Options struct {
	Name   string
	System string
	// Model and ContextWindowTokens are recorded once per thread when it first runs.
	Model               model.Ref
	ContextWindowTokens int
	// Tools are the specs offered to the model; their implementations are supplied to the host through Services.Tools.
	Tools      []library.Spec
	Compaction CompactionOptions
	// Permissions, when set, gates each tool call; "ask" decisions go to Services.Permission.
	Permissions *PermissionPolicy
	// Budget, when set, limits counted tool calls per turn; a RequestTool lets the model ask Services.Budget for more.
	Budget *BudgetPolicy
	// Methods add endpoints beside "message".
	Methods map[string]core.Method
}

// View summarizes what the agent is doing.
type View struct {
	Position string `json:"position"`
}

// MessageInput is the "message" method input: exactly one of Text or Content.
type MessageInput struct {
	Text    string       `json:"text,omitempty"`
	Content []model.Part `json:"content,omitempty"`
}

// Validate requires exactly one of Text or Content.
func (m MessageInput) Validate() error {
	if (m.Text == "") == (len(m.Content) == 0) {
		return errors.New("exactly one of text or content is required")
	}
	return nil
}

// MessageOutput is the "message" method result.
type MessageOutput struct {
	Text string `json:"text"`
}

// Reply projects a settled turn into a method result; it returns nil while the turn runs.
func Reply(g reactive.Getter, turnID string) *core.MethodResult[MessageOutput] {
	for _, t := range core.Get(g, InferenceState).Turns {
		if t.TurnID != turnID {
			continue
		}
		switch t.Settlement {
		case OutcomeCompleted:
			return core.Completed(MessageOutput{Text: *t.Answer})
		case OutcomeFailed:
			return core.FailedResult[MessageOutput](*t.Failure)
		case OutcomeCancelled:
			return core.CancelledResult[MessageOutput](*t.Cancellation)
		}
	}
	return nil
}

// MessageMethod starts a turn per call and replies with the turn's answer; cancellation aborts the turn.
var MessageMethod = core.NewMethod(core.MethodSpec[MessageInput, MessageOutput]{
	OnReceive: func(in MessageInput, c core.MethodContext) core.Event {
		text := in.Text
		if len(in.Content) > 0 {
			text = model.TextOf(in.Content, "")
		}
		return TurnRequested{TurnID: c.ID, Text: text, Content: in.Content, Source: SourceUser, Invocation: &InvocationRef{Method: c.Method, ID: c.ID}}
	},
	Result: func(_ MessageInput, g reactive.Getter, c core.MethodContext) *core.MethodResult[MessageOutput] {
		return Reply(g, c.ID)
	},
	OnCancel: func(_ MessageInput, c core.MethodContext, reason string) core.Event {
		return AbortRequested{Invocation: InvocationRef{Method: c.Method, ID: c.ID}, Reason: reason}
	},
})

// New defines an agent actor that converses, calls tools, and compacts its context.
func New(o Options) *core.Actor {
	if o.Name == "" || o.Model.Provider == "" || o.Model.ModelID == "" || o.ContextWindowTokens < 1 {
		panic(fmt.Errorf("agent: New requires a name, a model, and a positive context window"))
	}
	specs := o.Tools
	byName := map[string]library.Spec{}
	var permissions reactive.Atom[core.Output[Permissions]]
	var budget reactive.Atom[core.Output[BudgetView]]
	if o.Budget != nil {
		policy := *o.Budget
		if policy.RequestTool != "" {
			specs = append(append([]library.Spec(nil), specs...), library.Spec{Name: policy.RequestTool, Method: policy.RequestTool, Execution: library.Foreground,
				Description: "Request additional tool calls when your budget is exhausted. Waits for a decision and consumes no tool budget.",
				InputSchema: json.RawMessage(`{"type":"object","properties":{"amount":{"type":"integer","minimum":1},"reason":{"type":"string"}},"required":["amount","reason"],"additionalProperties":false}`)})
		}
		budget = budgetAtom(policy)
	}
	for _, s := range specs {
		byName[s.Name] = s
	}
	if o.Permissions != nil {
		permissions = permissionsAtom(*o.Permissions, byName)
	}
	tools := toolsAtom(specs, permissions, budget, promisesAtom)
	context := compactAtom(o.Compaction)
	root := core.EffectAtom(func(g reactive.Getter) core.Output[View] {
		return infer(g, o, tools, context)
	})
	methods := map[string]core.Method{"message": MessageMethod}
	for name, m := range o.Methods {
		methods[name] = m
	}
	return core.DefineActor(o.Name, root, methods)
}

func infer(g reactive.Getter, o Options, toolsAtom reactive.Atom[core.Output[ToolsView]], contextAtom reactive.Atom[core.Output[ContextView]]) core.Output[View] {
	state := core.Get(g, InferenceState)
	var turn *Turn
	for i := range state.Turns {
		if state.Turns[i].TurnID == state.TurnID {
			turn = &state.Turns[i]
		}
	}
	if turn != nil && turn.Cancellation != nil {
		reason := *turn.Cancellation
		acts := map[string]core.Proposal{}
		for _, work := range turn.Effects {
			if work.Pending {
				acts[work.Ref.Key()] = core.Cancel(work.Ref, reason)
			}
		}
		events := map[string]core.Event{}
		switch {
		case len(acts) > 0:
		case len(turn.Outstanding) > 0:
			for _, callID := range turn.Outstanding {
				events[callID] = ToolReturned{CallID: callID, TurnID: turn.TurnID, Content: []model.Part{}, Error: ptr(reason)}
			}
		default:
			events["inference"] = TurnSettled{TurnID: turn.TurnID, Outcome: OutcomeCancelled, Reason: reason}
		}
		return core.Output[View]{View: View{Position: "stopping"}, Events: events, Acts: acts}
	}
	cancelled := map[core.Ref]bool{}
	for _, t := range state.Turns {
		if t.Cancellation != nil {
			for _, work := range t.Effects {
				cancelled[work.Ref] = true
			}
		}
	}
	tools, context := core.Get(g, toolsAtom), core.Get(g, contextAtom)
	events := map[string]core.Event{}
	for k, e := range merge(merge(nil, context.Events), tools.Events) {
		if requested, ok := e.(TurnRequested); ok && requested.PromiseRef != nil && cancelled[*requested.PromiseRef] {
			continue
		}
		events[k] = e
	}
	acts := map[string]core.Proposal{}
	if state.TurnID != "" {
		acts = merge(merge(acts, context.Acts), tools.Acts)
	}
	out := func(position string) core.Output[View] {
		return core.Output[View]{View: View{Position: position}, Events: events, Acts: acts}
	}
	configured := core.Get(g, ModelState)
	if configured == nil {
		events["model"] = ModelConfigured{Model: o.Model, ContextWindowTokens: o.ContextWindowTokens}
		return out("configuring")
	}
	if state.Running {
		return out("running")
	}
	if turn != nil && turn.Failure != nil {
		events["inference"] = TurnSettled{TurnID: turn.TurnID, Outcome: OutcomeFailed, Reason: *turn.Failure}
		return out("settling")
	}
	if state.TurnID != "" && !state.NeedsReply {
		events["inference"] = TurnSettled{TurnID: state.TurnID, Outcome: OutcomeCompleted, CallID: turn.AnswerCallID}
		return out("settling")
	}
	if !state.NeedsReply {
		return out("idle")
	}
	if !state.Waiting && context.View.Position == ContextFailed {
		events["inference"] = TurnSettled{TurnID: state.TurnID, Outcome: OutcomeFailed, Reason: context.View.Reason}
		return out("settling")
	}
	if state.Waiting || context.View.Position != ContextReady {
		return out("waiting")
	}
	system := o.System
	if tools.View.Instructions != "" {
		system = strings.TrimSpace(system + "\n\n" + tools.View.Instructions)
	}
	offered := make([]model.Tool, len(tools.View.Specs))
	for i, s := range tools.View.Specs {
		description := s.Description
		if s.Execution == library.Background {
			description += " Execution: background; the result arrives later as a new message."
		}
		offered[i] = model.Tool{Name: s.Name, Description: description, InputSchema: s.InputSchema}
	}
	callID, turnID, selected := state.CallID, state.TurnID, *configured
	acts["inference"] = Generate.Request(core.Request[ModelInput, model.Reply]{
		Origin: state.Origin,
		Input:  ModelInput{Model: selected.Model, System: system, Tools: offered, Context: context.View.Messages},
		OnRequested: func(core.Ref) []core.Event {
			return []core.Event{ModelCalled{Purpose: PurposeInference, Model: selected.Model, ContextWindowTokens: selected.ContextWindowTokens, TurnID: turnID, CallID: callID}}
		},
		OnSettled: func(outcome core.Outcome[model.Reply], ref core.Ref) []core.Event {
			if outcome.Reason != nil {
				return []core.Event{ModelFailed{CallID: callID, Reason: outcome.Reason.Message}}
			}
			reply := outcome.Value
			seen := map[string]bool{}
			calls := make([]ToolCall, len(reply.ToolCalls))
			for i, call := range reply.ToolCalls {
				if seen[call.ID] {
					return []core.Event{ModelFailed{CallID: callID, Reason: "duplicate provider tool call IDs"}}
				}
				seen[call.ID] = true
				calls[i] = ToolCall{CallID: fmt.Sprintf("[%d,%q,%q,%d]", ref.Seq, ref.Atom, ref.Act, i), ProviderID: call.ID, Name: call.Name, Input: call.Input}
			}
			usage := reply.Usage
			return []core.Event{ModelReturned{Purpose: PurposeInference, TurnID: turnID, CallID: callID, Text: reply.Text, Reasoning: reply.Reasoning, Continuation: reply.Continuation, ToolCalls: calls, Usage: &usage}}
		},
	})
	return out("ready")
}
