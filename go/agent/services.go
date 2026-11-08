package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// DefaultModelTimeout bounds each model call attempt.
const DefaultModelTimeout = 90 * time.Second

// Services are what the agent's act handlers call on a host: the model, the tools, and the deciders of permission and budget requests.
type Services struct {
	Model model.Model
	Tools library.Registry
	// Permission decides "ask" permission requests; nil denies them.
	Permission func(x *tardigrade.Execution, request PermissionRequest) (Decision, error)
	// Budget decides requests for more tool calls; nil denies them.
	Budget func(x *tardigrade.Execution, request BudgetRequest) (BudgetDecision, error)
	// AttemptTimeout bounds each model attempt; zero uses DefaultModelTimeout and tardigrade.NoDeadline disables it.
	AttemptTimeout time.Duration
	// RetryAttempts, RetryBase, and RetryMax shape model retries; zero fields use the model package defaults.
	RetryAttempts int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

// ModelDelta is the execution update payload carrying streamed model text.
type ModelDelta struct {
	Type string          `json:"type"`
	Kind model.DeltaType `json:"kind"`
	Text string          `json:"text"`
}

// Handlers returns the handlers of every act an agent proposes: model calls, tool runs, and permission and budget requests.
func Handlers(s Services) []tardigrade.Handler {
	if s.AttemptTimeout == 0 {
		s.AttemptTimeout = DefaultModelTimeout
	}
	if s.RetryAttempts == 0 {
		s.RetryAttempts = model.DefaultRetryAttempts
	}
	if s.RetryBase == 0 {
		s.RetryBase = model.DefaultRetryBase
	}
	if s.RetryMax == 0 {
		s.RetryMax = model.DefaultRetryMax
	}
	call := func(x *tardigrade.Execution, in ModelInput) (model.Reply, error) {
		if s.Model == nil {
			return model.Reply{}, errors.New("no model service is configured")
		}
		policy := tardigrade.RetryPolicy{AttemptTimeout: s.AttemptTimeout, Decide: func(err error, attempt int) *tardigrade.RetryDecision {
			delay, ok := model.Backoff(err, attempt, s.RetryAttempts, s.RetryBase, s.RetryMax)
			if !ok {
				return nil
			}
			return &tardigrade.RetryDecision{Delay: delay, Reason: err.Error()}
		}}
		reply, err := tardigrade.Retry(x, policy, func(ctx context.Context) (model.Reply, error) {
			return s.Model.Generate(ctx, model.Request{Model: in.Model, System: in.System, Messages: in.Context, Tools: in.Tools}, func(d model.Delta) {
				x.Publish(ModelDelta{Type: "model.delta", Kind: d.Type, Text: d.Text})
			})
		})
		if err != nil {
			return model.Reply{}, err
		}
		if reply.FinishReason == model.FinishLength {
			return model.Reply{}, fmt.Errorf("model %s reached its output token limit", in.Model)
		}
		return reply, nil
	}
	return []tardigrade.Handler{
		Generate.Handle(func(x *tardigrade.Execution, in ModelInput) (tardigrade.Result[model.Reply], error) {
			reply, err := call(x, in)
			return tardigrade.Value(reply), err
		}, nil),
		Summarize.Handle(func(x *tardigrade.Execution, in ModelInput) (tardigrade.Result[model.Reply], error) {
			reply, err := call(x, in)
			if err == nil && strings.TrimSpace(reply.Text) == "" {
				err = errors.New("compaction returned an empty summary")
			}
			return tardigrade.Value(reply), err
		}, nil),
		ExecuteTool.Handle(func(x *tardigrade.Execution, in ExecuteInput) (tardigrade.Result[json.RawMessage], error) {
			if in.Error != "" {
				return tardigrade.Result[json.RawMessage]{}, errors.New(in.Error)
			}
			content := library.Content{{Type: model.PartText, Text: string(in.Value)}}
			if in.Value == nil {
				output, err := s.Tools.Run(library.Call{X: x, CallID: in.Call.CallID, Name: in.Call.Name}, in.Call.Input)
				if err != nil {
					return tardigrade.Result[json.RawMessage]{}, err
				}
				if output.Handle != nil {
					return tardigrade.Defer[json.RawMessage](*output.Handle, output.Options), nil
				}
				content = output.Content
			}
			encoded, err := json.Marshal(content)
			return tardigrade.Value(json.RawMessage(encoded)), err
		}, func(c tardigrade.Cleanup, in ExecuteInput) error {
			return s.Tools.Cancel(c, library.Call{CallID: in.Call.CallID, Name: in.Call.Name})
		}),
		AskPermission.Handle(func(x *tardigrade.Execution, in PermissionRequest) (tardigrade.Result[Decision], error) {
			if s.Permission == nil {
				return tardigrade.Value(Decision{Allowed: false, Reason: "no permission handler is configured"}), nil
			}
			decision, err := s.Permission(x, in)
			return tardigrade.Value(decision), err
		}, nil),
		AskBudget.Handle(func(x *tardigrade.Execution, in BudgetRequest) (tardigrade.Result[BudgetDecision], error) {
			if s.Budget == nil {
				return tardigrade.Value(BudgetDecision{Allowed: false, Reason: "no budget handler is configured"}), nil
			}
			decision, err := s.Budget(x, in)
			if err == nil && decision.Allowed && decision.Additional < 1 {
				err = errors.New("budget grants must be positive")
			}
			return tardigrade.Value(decision), err
		}, nil),
	}
}
