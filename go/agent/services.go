package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
)

// DefaultModelTimeout bounds each model call attempt.
const DefaultModelTimeout = 90 * time.Second

// Services implement the agent's acts on a host.
type Services struct {
	Model model.Model
	Tools library.Registry
	// Permission decides "ask" permission requests; nil denies them.
	Permission func(x *core.Execution, request PermissionRequest) (Decision, error)
	// Budget decides requests for more tool calls; nil denies them.
	Budget func(x *core.Execution, request BudgetRequest) (BudgetDecision, error)
	// AttemptTimeout bounds each model attempt; zero uses DefaultModelTimeout and core.NoDeadline disables it.
	AttemptTimeout time.Duration
	// RetryAttempts, RetryBase, and RetryMax shape model retries; zero fields use the model package defaults.
	RetryAttempts int
	RetryBase     time.Duration
	RetryMax      time.Duration
}

// ModelDelta is the execution update payload carrying streamed model text.
type ModelDelta struct {
	Type string `json:"type"`
	Kind string `json:"kind"`
	Text string `json:"text"`
}

// Acts returns the implementations of every act an agent proposes.
func Acts(s Services) []core.Implementation {
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
	call := func(x *core.Execution, in ModelInput) (model.Reply, error) {
		if s.Model == nil {
			return model.Reply{}, errors.New("no model service is configured")
		}
		policy := core.RetryPolicy{AttemptTimeout: s.AttemptTimeout, Decide: func(err error, attempt int) *core.RetryDecision {
			delay, ok := model.Backoff(err, attempt, s.RetryAttempts, s.RetryBase, s.RetryMax)
			if !ok {
				return nil
			}
			return &core.RetryDecision{Delay: delay, Reason: err.Error()}
		}}
		reply, err := core.Retry(x, policy, func(ctx context.Context) (model.Reply, error) {
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
	return []core.Implementation{
		Generate.Implement(func(x *core.Execution, in ModelInput) (core.Result[model.Reply], error) {
			reply, err := call(x, in)
			return core.Value(reply), err
		}, nil),
		Summarize.Implement(func(x *core.Execution, in ModelInput) (core.Result[model.Reply], error) {
			reply, err := call(x, in)
			if err == nil && strings.TrimSpace(reply.Text) == "" {
				err = errors.New("compaction returned an empty summary")
			}
			return core.Value(reply), err
		}, nil),
		ExecuteTool.Implement(func(x *core.Execution, in ExecuteInput) (core.Result[json.RawMessage], error) {
			if in.Error != "" {
				return core.Result[json.RawMessage]{}, errors.New(in.Error)
			}
			content := library.Content{{Type: "text", Text: string(in.Value)}}
			if in.Value == nil {
				output, err := s.Tools.Run(library.Call{X: x, CallID: in.Call.CallID, Name: in.Call.Name}, in.Call.Input)
				if err != nil {
					return core.Result[json.RawMessage]{}, err
				}
				if output.Handle != nil {
					return core.Defer[json.RawMessage](*output.Handle, output.Options), nil
				}
				content = output.Content
			}
			encoded, err := json.Marshal(content)
			return core.Value(json.RawMessage(encoded)), err
		}, func(c core.Cleanup, in ExecuteInput) error {
			return s.Tools.Cancel(c, library.Call{CallID: in.Call.CallID, Name: in.Call.Name})
		}),
		AskPermission.Implement(func(x *core.Execution, in PermissionRequest) (core.Result[Decision], error) {
			if s.Permission == nil {
				return core.Value(Decision{Allowed: false, Reason: "no permission handler is configured"}), nil
			}
			decision, err := s.Permission(x, in)
			return core.Value(decision), err
		}, nil),
		AskBudget.Implement(func(x *core.Execution, in BudgetRequest) (core.Result[BudgetDecision], error) {
			if s.Budget == nil {
				return core.Value(BudgetDecision{Allowed: false, Reason: "no budget handler is configured"}), nil
			}
			decision, err := s.Budget(x, in)
			if err == nil && decision.Allowed && decision.Additional < 1 {
				err = errors.New("budget grants must be positive")
			}
			return core.Value(decision), err
		}, nil),
	}
}
