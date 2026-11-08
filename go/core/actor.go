package core

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/rdeusser/tardigrade/go/reactive"
	"github.com/rdeusser/tardigrade/go/schema"
)

// MethodContext identifies one method call.
type MethodContext struct {
	ID     string
	Method string
}

// Method call statuses.
const (
	StatusPending   = "pending"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
)

// MethodResult is a terminal method outcome.
type MethodResult[O any] struct {
	Status string
	Output O
	Error  string
	Reason string
}

// Completed returns a completed method result.
func Completed[O any](output O) *MethodResult[O] {
	return &MethodResult[O]{Status: StatusCompleted, Output: output}
}

// FailedResult returns a failed method result.
func FailedResult[O any](message string) *MethodResult[O] {
	return &MethodResult[O]{Status: StatusFailed, Error: message}
}

// CancelledResult returns a cancelled method result.
func CancelledResult[O any](reason string) *MethodResult[O] {
	return &MethodResult[O]{Status: StatusCancelled, Reason: reason}
}

// MethodState is a method call's state as callers read it.
type MethodState struct {
	Status string          `json:"status"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
	Reason string          `json:"reason,omitempty"`
}

// MethodSpec defines a typed method. OnReceive maps a call to the domain event that records it; Result derives the reply from state and returns nil while pending; OnCancel, when set, maps a cancellation request to a domain event.
type MethodSpec[I, O any] struct {
	OnReceive func(in I, c MethodContext) Event
	Result    func(in I, g reactive.Getter, c MethodContext) *MethodResult[O]
	OnCancel  func(in I, c MethodContext, reason string) Event
}

// Method is a type-erased method definition.
type Method struct {
	inputSchema  json.RawMessage
	outputSchema json.RawMessage
	receive      func(input json.RawMessage, c MethodContext) (Event, error)
	result       func(input json.RawMessage, g reactive.Getter, c MethodContext) (*MethodState, error)
	cancel       func(input json.RawMessage, c MethodContext, reason string) (Event, error)
}

// NewMethod defines a method. Inputs are decoded strictly with schema.Decode.
func NewMethod[I, O any](spec MethodSpec[I, O]) Method {
	if spec.OnReceive == nil || spec.Result == nil {
		panic(fmt.Errorf("core: methods require OnReceive and Result"))
	}
	m := Method{
		inputSchema:  schema.Of[I](),
		outputSchema: schema.Of[O](),
		receive: func(input json.RawMessage, c MethodContext) (Event, error) {
			in, err := schema.Decode[I](input)
			if err != nil {
				return nil, err
			}
			return spec.OnReceive(in, c), nil
		},
		result: func(input json.RawMessage, g reactive.Getter, c MethodContext) (*MethodState, error) {
			in, err := schema.Decode[I](input)
			if err != nil {
				return nil, err
			}
			result := spec.Result(in, g, c)
			if result == nil {
				return nil, nil
			}
			state := &MethodState{Status: result.Status, Error: result.Error, Reason: result.Reason}
			if result.Status == StatusCompleted {
				if state.Output, err = json.Marshal(result.Output); err != nil {
					return nil, err
				}
			}
			return state, nil
		},
	}
	if spec.OnCancel != nil {
		m.cancel = func(input json.RawMessage, c MethodContext, reason string) (Event, error) {
			in, err := schema.Decode[I](input)
			if err != nil {
				return nil, err
			}
			return spec.OnCancel(in, c, reason), nil
		}
	}
	return m
}

// Cancellable reports whether the method accepts cancellation.
func (m Method) Cancellable() bool { return m.cancel != nil }

// InputSchema returns the method input's JSON Schema.
func (m Method) InputSchema() json.RawMessage { return m.inputSchema }

// OutputSchema returns the method output's JSON Schema.
func (m Method) OutputSchema() json.RawMessage { return m.outputSchema }

// Actor is a behavior definition: a root effect atom and its methods. One definition serves every thread; values live in each thread's registry.
type Actor struct {
	name    string
	root    reactive.Atom[proposer]
	methods map[string]Method
}

// DefineActor assembles an actor from its root effect atom and methods. Names must be nonempty and contain no slash.
func DefineActor[V any](name string, root reactive.Atom[Output[V]], methods map[string]Method) *Actor {
	if name == "" || strings.Contains(name, "/") {
		panic(fmt.Errorf("core: actor name must be nonempty and contain no slash"))
	}
	for method := range methods {
		if method == "" {
			panic(fmt.Errorf("core: method names must be nonempty"))
		}
	}
	return &Actor{
		name:    name,
		root:    reactive.New(func(g reactive.Getter) proposer { return reactive.Get(g, root) }),
		methods: methods,
	}
}

// Name returns the actor's name.
func (a *Actor) Name() string { return a.name }

// Method returns a method by name.
func (a *Actor) Method(name string) (Method, bool) {
	m, ok := a.methods[name]
	return m, ok
}

// MethodInfo summarizes a method for discovery.
type MethodInfo struct {
	Name         string          `json:"name"`
	Cancellable  bool            `json:"cancellable"`
	InputSchema  json.RawMessage `json:"inputSchema"`
	OutputSchema json.RawMessage `json:"outputSchema"`
}

// Methods lists the actor's methods sorted by name.
func (a *Actor) Methods() []MethodInfo {
	infos := make([]MethodInfo, 0, len(a.methods))
	for name, m := range a.methods {
		infos = append(infos, MethodInfo{Name: name, Cancellable: m.Cancellable(), InputSchema: m.inputSchema, OutputSchema: m.outputSchema})
	}
	sort.Slice(infos, func(i, j int) bool { return infos[i].Name < infos[j].Name })
	return infos
}
