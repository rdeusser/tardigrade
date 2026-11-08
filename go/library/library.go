// Package library declares tools for agents: typed methods grouped into libraries, with JSON Schemas derived from their Go types.
package library

import (
	"encoding/json"
	"fmt"
	"regexp"
	"time"

	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/schema"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// Mode is how a tool returns its result.
type Mode string

// Tool modes.
const (
	// Foreground tools return their result to the model directly.
	Foreground Mode = "foreground"
	// Background tools return a durable handle; the result reaches the agent as a later turn.
	Background Mode = "background"
)

// Annotations are MCP-compatible behaviour hints; adapters do not enforce permissions from them.
type Annotations struct {
	Title           string `json:"title,omitempty"`
	ReadOnlyHint    *bool  `json:"readOnlyHint,omitempty"`
	DestructiveHint *bool  `json:"destructiveHint,omitempty"`
	IdempotentHint  *bool  `json:"idempotentHint,omitempty"`
	OpenWorldHint   *bool  `json:"openWorldHint,omitempty"`
}

// Spec is a tool's contract as the model sees it.
type Spec struct {
	Name             string          `json:"name"`
	Method           string          `json:"method"`
	Description      string          `json:"description"`
	InputSchema      json.RawMessage `json:"inputSchema"`
	OutputSchema     json.RawMessage `json:"outputSchema,omitempty"`
	Annotations      *Annotations    `json:"annotations,omitempty"`
	Execution        Mode            `json:"execution"`
	PromiseTimeoutMs int64           `json:"promiseTimeoutMs,omitempty"`
}

// Call is the context of one tool invocation.
type Call struct {
	X      *tardigrade.Execution
	CallID string
	Name   string
}

// Content is a tool result made of content parts; other result types are rendered as text.
type Content []model.Part

// Output is a tool's result: content for the model, or a handle when the tool runs in the background.
type Output struct {
	Content Content
	Handle  *tardigrade.Handle
	Options tardigrade.PromiseOptions
}

// Method is one tool of a library.
type Method struct {
	Spec   Spec
	run    func(c Call, input json.RawMessage) (Output, error)
	cancel func(c tardigrade.Cleanup, call Call) error
}

// Library groups methods under a name.
type Library struct {
	Name        string
	Description string
	Methods     []Method
}

// Option adjusts a method.
type Option func(*Method)

// Named sets the tool name; the default is "<library>__<method>".
func Named(name string) Option { return func(m *Method) { m.Spec.Name = name } }

// Hints sets behaviour annotations.
func Hints(a Annotations) Option { return func(m *Method) { m.Spec.Annotations = &a } }

// PromiseTimeout bounds waiting for a background tool's result; omission inherits the runtime policy.
func PromiseTimeout(d time.Duration) Option {
	return func(m *Method) { m.Spec.PromiseTimeoutMs = d.Milliseconds() }
}

// OnCancel releases a background tool's external work after cancellation.
func OnCancel(cancel func(c tardigrade.Cleanup, call Call) error) Option {
	return func(m *Method) { m.cancel = cancel }
}

// Tool defines a foreground method whose result returns to the model directly.
func Tool[I, O any](name, description string, run func(c Call, in I) (O, error), options ...Option) Method {
	m := Method{Spec: Spec{Method: name, Description: description, InputSchema: schema.Of[I](), OutputSchema: schema.Of[O](), Execution: Foreground}}
	m.run = func(c Call, input json.RawMessage) (Output, error) {
		in, err := schema.Decode[I](input)
		if err != nil {
			return Output{}, err
		}
		out, err := run(c, in)
		if err != nil {
			return Output{}, err
		}
		content, err := render(out)
		return Output{Content: content}, err
	}
	for _, option := range options {
		option(&m)
	}
	return m
}

// Task defines a background method that returns a durable handle; its result reaches the agent as a later turn.
func Task[I any](name, description string, submit func(c Call, in I) (tardigrade.Handle, error), options ...Option) Method {
	m := Method{Spec: Spec{Method: name, Description: description, InputSchema: schema.Of[I](), Execution: Background}}
	m.run = func(c Call, input json.RawMessage) (Output, error) {
		in, err := schema.Decode[I](input)
		if err != nil {
			return Output{}, err
		}
		handle, err := submit(c, in)
		if err != nil {
			return Output{}, err
		}
		return Output{Handle: &handle, Options: tardigrade.PromiseOptions{Timeout: time.Duration(m.Spec.PromiseTimeoutMs) * time.Millisecond}}, nil
	}
	for _, option := range options {
		option(&m)
	}
	return m
}

func render(value any) (Content, error) {
	switch v := value.(type) {
	case Content:
		return v, nil
	case string:
		return Content{{Type: model.PartText, Text: v}}, nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return Content{{Type: model.PartText, Text: string(data)}}, nil
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)

// New defines a library and names its tools. Duplicate method or tool names panic.
func New(name, description string, methods ...Method) Library {
	if !identifier.MatchString(name) {
		panic(fmt.Errorf("library: invalid name %q", name))
	}
	seen := map[string]bool{}
	for i := range methods {
		m := &methods[i]
		if m.Spec.Name == "" {
			m.Spec.Name = name + "__" + m.Spec.Method
		}
		if !identifier.MatchString(m.Spec.Name) || seen[m.Spec.Name] {
			panic(fmt.Errorf("library %s: invalid or duplicate tool %q", name, m.Spec.Name))
		}
		seen[m.Spec.Name] = true
	}
	return Library{Name: name, Description: description, Methods: methods}
}

// Specs lists the tool specs of libraries.
func Specs(libraries ...Library) []Spec {
	var specs []Spec
	for _, l := range libraries {
		for _, m := range l.Methods {
			specs = append(specs, m.Spec)
		}
	}
	return specs
}

// Registry resolves tool names to methods across libraries.
type Registry map[string]Method

// NewRegistry indexes libraries by tool name; duplicate tool names panic.
func NewRegistry(libraries ...Library) Registry {
	registry := Registry{}
	for _, l := range libraries {
		for _, m := range l.Methods {
			if _, ok := registry[m.Spec.Name]; ok {
				panic(fmt.Errorf("library: tool %q is defined twice", m.Spec.Name))
			}
			registry[m.Spec.Name] = m
		}
	}
	return registry
}

// Run executes a tool call.
func (r Registry) Run(c Call, input json.RawMessage) (Output, error) {
	m, ok := r[c.Name]
	if !ok {
		return Output{}, fmt.Errorf("unknown tool: %s", c.Name)
	}
	return m.run(c, input)
}

// Cancel releases a cancelled tool call's external work.
func (r Registry) Cancel(c tardigrade.Cleanup, call Call) error {
	if m, ok := r[call.Name]; ok && m.cancel != nil {
		return m.cancel(c, call)
	}
	return nil
}
