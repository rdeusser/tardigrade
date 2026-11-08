// Package agent assembles a tool-using, self-compacting model agent from durable atoms and effect atoms.
package agent

import (
	"encoding/json"

	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// InvocationRef identifies the method call that started a turn.
type InvocationRef struct {
	Method string `json:"method"`
	ID     string `json:"id"`
}

// Source is who started a turn.
type Source string

// Turn sources.
const (
	SourceUser  Source = "user"
	SourceAgent Source = "agent"
	SourceTool  Source = "tool"
)

// TurnRequested asks the agent for a reply.
type TurnRequested struct {
	TurnID     string          `json:"turnId"`
	Text       string          `json:"text"`
	Content    []model.Part    `json:"content,omitempty"`
	Invocation *InvocationRef  `json:"invocationRef,omitempty"`
	Source     Source          `json:"source,omitempty"`
	PromiseRef *tardigrade.Ref `json:"promiseRef,omitempty"`
}

// ModelConfigured records the model the agent calls.
type ModelConfigured struct {
	Model               model.Ref `json:"model"`
	ContextWindowTokens int       `json:"contextWindowTokens"`
}

// Purpose is why the agent called a model.
type Purpose string

// Model call purposes.
const (
	// PurposeInference is a call that answers a turn.
	PurposeInference Purpose = "inference"
	// PurposeCompaction is a call that summarizes older messages.
	PurposeCompaction Purpose = "compaction"
)

// ModelCalled records acceptance of a model call. Compaction calls carry Through, the message count the summary will cover.
type ModelCalled struct {
	Purpose             Purpose   `json:"purpose"`
	Model               model.Ref `json:"model"`
	ContextWindowTokens int       `json:"contextWindowTokens"`
	TurnID              string    `json:"turnId,omitempty"`
	CallID              string    `json:"callId"`
	Through             int       `json:"through,omitempty"`
}

// ToolCall is a model tool call; CallID is unique in the journal and ProviderID is the provider's identifier.
type ToolCall struct {
	CallID     string          `json:"callId"`
	ProviderID string          `json:"providerId"`
	Name       string          `json:"name"`
	Input      json.RawMessage `json:"input"`
}

// ModelReturned records a model reply.
type ModelReturned struct {
	Purpose      Purpose             `json:"purpose"`
	TurnID       string              `json:"turnId,omitempty"`
	CallID       string              `json:"callId"`
	Text         string              `json:"text"`
	Reasoning    string              `json:"reasoning,omitempty"`
	Continuation *model.Continuation `json:"continuation,omitempty"`
	ToolCalls    []ToolCall          `json:"toolCalls,omitempty"`
	Usage        *model.Usage        `json:"usage,omitempty"`
}

// ModelFailed records a failed inference call.
type ModelFailed struct {
	CallID string `json:"callId"`
	Reason string `json:"reason"`
}

// CompactionFailed records a failed summary call.
type CompactionFailed struct {
	CallID string `json:"callId"`
	Reason string `json:"reason"`
}

// ToolCalled records acceptance of a tool execution; Counted marks calls charged to the tool budget.
type ToolCalled struct {
	CallID  string `json:"callId"`
	Counted bool   `json:"counted"`
}

// ToolOwner names the tool call a result answers.
type ToolOwner struct {
	Name       string `json:"name"`
	ProviderID string `json:"providerId"`
}

// ToolReturned records a tool result. Error is set on failure; Promise is set when the tool continues in the background.
type ToolReturned struct {
	CallID  string           `json:"callId"`
	TurnID  string           `json:"turnId,omitempty"`
	Tool    *ToolOwner       `json:"tool,omitempty"`
	Content []model.Part     `json:"content"`
	Error   *string          `json:"error,omitempty"`
	Promise *library.Promise `json:"promise,omitempty"`
}

// Outcome is how a turn ended.
type Outcome string

// Turn outcomes.
const (
	OutcomeCompleted Outcome = "completed"
	OutcomeFailed    Outcome = "failed"
	OutcomeCancelled Outcome = "cancelled"
)

// TurnFinished ends a turn. A completed turn names the reply in CallID; other outcomes carry Reason.
type TurnFinished struct {
	TurnID  string  `json:"turnId"`
	Outcome Outcome `json:"outcome"`
	CallID  string  `json:"callId,omitempty"`
	Reason  string  `json:"reason,omitempty"`
}

// AbortRequested asks the turn of an invocation to stop.
type AbortRequested struct {
	Invocation InvocationRef `json:"ref"`
	Reason     string        `json:"reason"`
}

// PermissionMode decides whether an action runs.
type PermissionMode string

// Permission modes.
const (
	Allow PermissionMode = "allow"
	Deny  PermissionMode = "deny"
	// Ask sends the decision to Services.Permission.
	Ask PermissionMode = "ask"
)

// PermissionRule sets modes for one action: per resource, for read-only resources, and by default.
type PermissionRule struct {
	Default   PermissionMode            `json:"default,omitempty"`
	Resources map[string]PermissionMode `json:"resources"`
	ReadOnly  PermissionMode            `json:"readOnly,omitempty"`
}

// PermissionPolicy sets the mode of each action; "tool.execute" governs tool calls with tool names as resources.
type PermissionPolicy struct {
	Default PermissionMode            `json:"default"`
	Actions map[string]PermissionRule `json:"actions"`
}

// Decision is a permission outcome.
type Decision struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// PermissionConfigured records the initial permission policy.
type PermissionConfigured struct {
	Policy PermissionPolicy `json:"policy"`
}

// PermissionUpdated replaces the permission policy.
type PermissionUpdated struct {
	Policy PermissionPolicy `json:"policy"`
}

// PermissionResolved records a permission decision for one request.
type PermissionResolved struct {
	Action    string   `json:"action"`
	RequestID string   `json:"requestId"`
	Decision  Decision `json:"decision"`
}

// Exhaustion is what a budget does once it runs out.
type Exhaustion string

// Exhaustion behaviours.
const (
	// ExhaustedWait blocks further counted calls until a BudgetResolved grants more.
	ExhaustedWait Exhaustion = "wait"
	// ExhaustedDeny refuses further counted calls and offers the model no tools.
	ExhaustedDeny Exhaustion = "deny"
)

// BudgetPolicy limits a metric per turn. RequestTool names a tool the model calls to ask for more; OnExhausted chooses what happens when none is left, and the zero value waits.
type BudgetPolicy struct {
	Limit       int        `json:"limit"`
	RequestTool string     `json:"requestTool,omitempty"`
	OnExhausted Exhaustion `json:"onExhausted,omitempty"`
}

// BudgetDecision grants Additional units or refuses with Reason.
type BudgetDecision struct {
	Allowed    bool   `json:"allowed"`
	Additional int    `json:"additional,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// BudgetConfigured records the initial budget of a metric.
type BudgetConfigured struct {
	Metric string       `json:"metric"`
	Policy BudgetPolicy `json:"policy"`
}

// BudgetUpdated replaces a metric's base allowance and keeps grants of the current turn.
type BudgetUpdated struct {
	Metric string       `json:"metric"`
	Policy BudgetPolicy `json:"policy"`
}

// BudgetResolved records a budget decision for one tool call.
type BudgetResolved struct {
	Metric   string         `json:"metric"`
	CallID   string         `json:"callId"`
	Decision BudgetDecision `json:"decision"`
}

func (TurnRequested) EventType() string        { return "TurnRequested" }
func (ModelConfigured) EventType() string      { return "ModelConfigured" }
func (ModelCalled) EventType() string          { return "ModelCalled" }
func (ModelReturned) EventType() string        { return "ModelReturned" }
func (ModelFailed) EventType() string          { return "ModelFailed" }
func (CompactionFailed) EventType() string     { return "CompactionFailed" }
func (ToolCalled) EventType() string           { return "ToolCalled" }
func (ToolReturned) EventType() string         { return "ToolReturned" }
func (TurnFinished) EventType() string         { return "TurnFinished" }
func (AbortRequested) EventType() string       { return "AbortRequested" }
func (PermissionConfigured) EventType() string { return "PermissionConfigured" }
func (PermissionUpdated) EventType() string    { return "PermissionUpdated" }
func (PermissionResolved) EventType() string   { return "PermissionResolved" }
func (BudgetConfigured) EventType() string     { return "BudgetConfigured" }
func (BudgetUpdated) EventType() string        { return "BudgetUpdated" }
func (BudgetResolved) EventType() string       { return "BudgetResolved" }
