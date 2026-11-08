package agent

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/reactive"
	"github.com/rdeusser/tardigrade/go/tardigrade"
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
	// Tools are the specs offered to the model; the host runs them through Services.Tools.
	Tools      []library.Spec
	Compaction CompactionOptions
	// Permissions, when set, gates each tool call; "ask" decisions go to Services.Permission.
	Permissions *PermissionPolicy
	// Budget, when set, limits counted tool calls per turn; a RequestTool lets the model ask Services.Budget for more.
	Budget *BudgetPolicy
	// Methods add endpoints beside "message".
	Methods map[string]tardigrade.Method
}

// Phase is what the agent, or its context, is doing.
type Phase string

// Phases of the agent view and the context view.
const (
	PhaseConfiguring Phase = "configuring"
	PhaseIdle        Phase = "idle"
	PhaseWaiting     Phase = "waiting"
	PhaseReady       Phase = "ready"
	PhaseRunning     Phase = "running"
	PhaseFinishing   Phase = "finishing"
	PhaseStopping    Phase = "stopping"
	PhaseCompacting  Phase = "compacting"
	PhaseFailed      Phase = "failed"
)

// View summarizes what the agent is doing.
type View struct {
	Phase Phase `json:"phase"`
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

// Reply projects a finished turn into a method result; it returns nil while the turn runs.
func Reply(g reactive.Getter, turnID string) *tardigrade.MethodResult[MessageOutput] {
	for _, t := range tardigrade.Get(g, TurnState).All {
		if t.TurnID != turnID {
			continue
		}
		switch t.Outcome {
		case OutcomeCompleted:
			return tardigrade.Completed(MessageOutput{Text: *t.Answer})
		case OutcomeFailed:
			return tardigrade.FailedResult[MessageOutput](*t.Failure)
		case OutcomeCancelled:
			return tardigrade.CancelledResult[MessageOutput](*t.Cancellation)
		}
	}
	return nil
}

// MessageMethod starts a turn per call and replies with the turn's answer; cancellation aborts the turn.
var MessageMethod = tardigrade.NewMethod(tardigrade.MethodSpec[MessageInput, MessageOutput]{
	OnReceive: func(in MessageInput, c tardigrade.MethodContext) tardigrade.Event {
		text := in.Text
		if len(in.Content) > 0 {
			text = model.TextOf(in.Content, "")
		}
		return TurnRequested{TurnID: c.ID, Text: text, Content: in.Content, Source: SourceUser, Invocation: &InvocationRef{Method: c.Method, ID: c.ID}}
	},
	Result: func(_ MessageInput, g reactive.Getter, c tardigrade.MethodContext) *tardigrade.MethodResult[MessageOutput] {
		return Reply(g, c.ID)
	},
	OnCancel: func(_ MessageInput, c tardigrade.MethodContext, reason string) tardigrade.Event {
		return AbortRequested{Invocation: InvocationRef{Method: c.Method, ID: c.ID}, Reason: reason}
	},
})

// New defines an agent actor that converses, calls tools, and compacts its context.
func New(o Options) *tardigrade.Actor {
	if o.Name == "" || o.Model.Provider == "" || o.Model.ModelID == "" || o.ContextWindowTokens < 1 {
		panic(fmt.Errorf("agent: New requires a name, a model, and a positive context window"))
	}
	specs := o.Tools
	byName := map[string]library.Spec{}
	var permissions reactive.Atom[tardigrade.Output[Permissions]]
	var budget reactive.Atom[tardigrade.Output[BudgetView]]
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
	root := tardigrade.EffectAtom(func(g reactive.Getter) tardigrade.Output[View] {
		return infer(g, o, tools, context)
	})
	methods := map[string]tardigrade.Method{"message": MessageMethod}
	maps.Copy(methods, o.Methods)
	return tardigrade.DefineActor(o.Name, root, methods)
}

func infer(g reactive.Getter, o Options, toolsAtom reactive.Atom[tardigrade.Output[ToolsView]], contextAtom reactive.Atom[tardigrade.Output[ContextView]]) tardigrade.Output[View] {
	turns := tardigrade.Get(g, TurnState)
	var turn *Turn
	for i := range turns.All {
		if turns.All[i].TurnID == turns.Current {
			turn = &turns.All[i]
		}
	}
	if turn != nil && turn.Cancellation != nil {
		reason := *turn.Cancellation
		acts := map[string]tardigrade.Proposal{}
		for _, work := range turn.Effects {
			if work.Pending {
				acts[work.Ref.Key()] = tardigrade.Cancel(work.Ref, reason)
			}
		}
		events := map[string]tardigrade.Event{}
		switch {
		case len(acts) > 0:
		case len(turn.Outstanding) > 0:
			for _, callID := range turn.Outstanding {
				events[callID] = ToolReturned{CallID: callID, TurnID: turn.TurnID, Content: []model.Part{}, Error: new(reason)}
			}
		default:
			events["inference"] = TurnFinished{TurnID: turn.TurnID, Outcome: OutcomeCancelled, Reason: reason}
		}
		return tardigrade.Output[View]{View: View{Phase: PhaseStopping}, Events: events, Acts: acts}
	}
	cancelled := map[tardigrade.Ref]bool{}
	for _, t := range turns.All {
		if t.Cancellation != nil {
			for _, work := range t.Effects {
				cancelled[work.Ref] = true
			}
		}
	}
	tools, context := tardigrade.Get(g, toolsAtom), tardigrade.Get(g, contextAtom)
	events := map[string]tardigrade.Event{}
	maps.Copy(events, context.Events)
	maps.Copy(events, tools.Events)
	maps.DeleteFunc(events, func(_ string, e tardigrade.Event) bool {
		requested, ok := e.(TurnRequested)
		return ok && requested.PromiseRef != nil && cancelled[*requested.PromiseRef]
	})
	acts := map[string]tardigrade.Proposal{}
	if turns.Current != "" {
		maps.Copy(acts, context.Acts)
		maps.Copy(acts, tools.Acts)
	}
	out := func(phase Phase) tardigrade.Output[View] {
		return tardigrade.Output[View]{View: View{Phase: phase}, Events: events, Acts: acts}
	}
	configured := tardigrade.Get(g, ModelState)
	if configured == nil {
		events["model"] = ModelConfigured{Model: o.Model, ContextWindowTokens: o.ContextWindowTokens}
		return out(PhaseConfiguring)
	}
	if turns.Calling {
		return out(PhaseRunning)
	}
	if turn != nil && turn.Failure != nil {
		events["inference"] = TurnFinished{TurnID: turn.TurnID, Outcome: OutcomeFailed, Reason: *turn.Failure}
		return out(PhaseFinishing)
	}
	if turns.Current != "" && !turns.NeedsReply {
		events["inference"] = TurnFinished{TurnID: turns.Current, Outcome: OutcomeCompleted, CallID: turn.AnswerCallID}
		return out(PhaseFinishing)
	}
	if !turns.NeedsReply {
		return out(PhaseIdle)
	}
	if !turns.AwaitingTools && context.View.Phase == PhaseFailed {
		events["inference"] = TurnFinished{TurnID: turns.Current, Outcome: OutcomeFailed, Reason: context.View.Reason}
		return out(PhaseFinishing)
	}
	if turns.AwaitingTools || context.View.Phase != PhaseReady {
		return out(PhaseWaiting)
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
	callID, turnID, selected := turns.Call, turns.Current, *configured
	acts["inference"] = Generate.Request(tardigrade.Request[ModelInput, model.Reply]{
		Origin: turns.CallOrigin,
		Input:  ModelInput{Model: selected.Model, System: system, Tools: offered, Context: context.View.Messages},
		OnRequested: func(tardigrade.Ref) []tardigrade.Event {
			return []tardigrade.Event{ModelCalled{Purpose: PurposeInference, Model: selected.Model, ContextWindowTokens: selected.ContextWindowTokens, TurnID: turnID, CallID: callID}}
		},
		OnFinished: func(outcome tardigrade.Outcome[model.Reply], ref tardigrade.Ref) []tardigrade.Event {
			if outcome.Err != nil {
				return []tardigrade.Event{ModelFailed{CallID: callID, Reason: outcome.Err.Message}}
			}
			reply := outcome.Value
			seen := map[string]bool{}
			calls := make([]ToolCall, len(reply.ToolCalls))
			for i, call := range reply.ToolCalls {
				if seen[call.ID] {
					return []tardigrade.Event{ModelFailed{CallID: callID, Reason: "duplicate provider tool call IDs"}}
				}
				seen[call.ID] = true
				calls[i] = ToolCall{CallID: fmt.Sprintf("[%d,%q,%q,%d]", ref.Seq, ref.Atom, ref.Act, i), ProviderID: call.ID, Name: call.Name, Input: call.Input}
			}
			usage := reply.Usage
			return []tardigrade.Event{ModelReturned{Purpose: PurposeInference, TurnID: turnID, CallID: callID, Text: reply.Text, Reasoning: reply.Reasoning, Continuation: reply.Continuation, ToolCalls: calls, Usage: &usage}}
		},
	})
	return out(PhaseReady)
}
