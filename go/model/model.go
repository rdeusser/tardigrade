// Package model calls language model providers through one provider-neutral request and reply shape.
package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// Ref selects a model of a configured provider.
type Ref struct {
	Provider string `json:"provider"`
	ModelID  string `json:"model_id"`
}

func (r Ref) String() string { return r.Provider + "/" + r.ModelID }

// PartType is the kind of a content part.
type PartType string

// Content part types.
const (
	PartText PartType = "text"
	PartFile PartType = "file"
)

// Part is one piece of message content. Files carry their bytes inline.
type Part struct {
	Type      PartType `json:"type" enum:"text,file"`
	Text      string   `json:"text,omitempty"`
	MediaType string   `json:"mediaType,omitempty"`
	Data      []byte   `json:"data,omitempty"`
	Filename  string   `json:"filename,omitempty"`
}

// IsImage reports whether the part is an image file.
func (p Part) IsImage() bool {
	return p.Type == PartFile && strings.HasPrefix(p.MediaType, "image/")
}

// TextOf joins a content's text parts with sep.
func TextOf(parts []Part, sep string) string {
	text := ""
	for _, p := range parts {
		if p.Type == PartText {
			if text != "" {
				text += sep
			}
			text += p.Text
		}
	}
	return text
}

// ToolCall is a model's request to run a tool; ID is the provider's call identifier.
type ToolCall struct {
	ID    string          `json:"id"`
	Name  string          `json:"name"`
	Input json.RawMessage `json:"input"`
}

// Continuation is provider-native assistant content, such as signed reasoning, replayed verbatim to the same provider, protocol, and model.
type Continuation struct {
	Provider string          `json:"provider"`
	Protocol Protocol        `json:"protocol"`
	Model    string          `json:"model"`
	Payload  json.RawMessage `json:"payload"`
}

// Role is who a message comes from.
type Role string

// Message roles.
const (
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
	RoleTool      Role = "tool"
)

// Message is one conversation turn. RoleUser carries Text or Content; RoleAssistant carries Text, ToolCalls, and an optional Continuation; RoleTool carries a ToolCallID result as Text or Content with Error set on failure.
type Message struct {
	Role         Role          `json:"role" enum:"user,assistant,tool"`
	Text         string        `json:"text,omitempty"`
	Content      []Part        `json:"content,omitempty"`
	ToolCalls    []ToolCall    `json:"toolCalls,omitempty"`
	Reasoning    string        `json:"reasoning,omitempty"`
	Continuation *Continuation `json:"continuation,omitempty"`
	ToolCallID   string        `json:"toolCallId,omitempty"`
	Name         string        `json:"name,omitempty"`
	Error        bool          `json:"error,omitempty"`
}

// Tool describes a tool the model may call.
type Tool struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
}

// Request is one model call.
type Request struct {
	Model    Ref
	System   string
	Messages []Message
	Tools    []Tool
	// MaxOutputTokens bounds the reply; zero uses the provider configuration or DefaultMaxOutputTokens.
	MaxOutputTokens int
}

// Usage is provider-reported token usage; CacheRead and CacheWrite count tokens within Input.
type Usage struct {
	Input      int      `json:"input,omitempty"`
	Output     int      `json:"output,omitempty"`
	CacheRead  int      `json:"cacheRead,omitempty"`
	CacheWrite int      `json:"cacheWrite,omitempty"`
	USD        *float64 `json:"usd"`
}

// FinishReason is why a reply ended.
type FinishReason string

// Finish reasons.
const (
	FinishStop      FinishReason = "stop"
	FinishLength    FinishReason = "length"
	FinishToolCalls FinishReason = "tool_calls"
)

// Reply is a completed model call.
type Reply struct {
	Text         string        `json:"text"`
	Reasoning    string        `json:"reasoning,omitempty"`
	ToolCalls    []ToolCall    `json:"toolCalls"`
	Usage        Usage         `json:"usage"`
	FinishReason FinishReason  `json:"finishReason"`
	Continuation *Continuation `json:"continuation,omitempty"`
}

// DeltaType is the kind of streamed text.
type DeltaType string

// Delta types.
const (
	DeltaText      DeltaType = "text"
	DeltaReasoning DeltaType = "reasoning"
)

// Delta is streamed reply text.
type Delta struct {
	Type DeltaType `json:"type"`
	Text string    `json:"text"`
}

// Model generates replies. onDelta, when non-nil, receives streamed text as it arrives.
type Model interface {
	Generate(ctx context.Context, request Request, onDelta func(Delta)) (Reply, error)
}

// Protocol is a provider wire protocol.
type Protocol string

// Protocols the providers in this package speak.
const (
	AnthropicMessages     Protocol = "anthropic-messages"
	OpenAIChatCompletions Protocol = "openai-chat-completions"
)

// Defaults for provider calls.
const (
	DefaultMaxOutputTokens = 32_768
	DefaultFirstChunk      = 60 * time.Second
	DefaultIdle            = 60 * time.Second
)

// ProviderConfig connects one provider.
type ProviderConfig struct {
	BaseURL  string   `json:"baseUrl"`
	Protocol Protocol `json:"protocol"`
	// Env names environment variables holding the API key; the first nonempty one is used.
	Env []string `json:"env,omitempty"`
	// MaxTokensField names the chat-completions output bound field; empty uses "max_tokens".
	MaxTokensField string                 `json:"maxTokensField,omitempty"`
	Models         map[string]ModelConfig `json:"models,omitempty"`
}

// ModelConfig configures one model of a provider.
type ModelConfig struct {
	// Options are provider request fields merged into each request body.
	Options             map[string]json.RawMessage `json:"options,omitempty"`
	ContextWindowTokens int                        `json:"contextWindowTokens,omitempty"`
	MaxOutputTokens     int                        `json:"maxOutputTokens,omitempty"`
}

// Config selects a default model and connects providers. A provider named "openrouter" inherits the OpenRouter fields it leaves empty; every other provider sets its base URL and protocol.
type Config struct {
	Default   Ref                       `json:"default"`
	Providers map[string]ProviderConfig `json:"providers"`
	// FirstChunk and Idle bound streamed replies; zero uses the defaults and a negative value disables the bound.
	FirstChunk time.Duration `json:"-"`
	Idle       time.Duration `json:"-"`
}

// OpenRouter is the connection a provider named "openrouter" inherits: OpenRouter's chat completions endpoint, keyed by OPENROUTER_API_KEY.
var OpenRouter = ProviderConfig{BaseURL: "https://openrouter.ai/api/v1", Protocol: OpenAIChatCompletions, Env: []string{"OPENROUTER_API_KEY"}}

// ParseConfig decodes a configuration and fills the "openrouter" provider from OpenRouter. Every other provider must set a base URL and protocol.
func ParseConfig(data []byte) (Config, error) {
	var config Config
	if err := json.Unmarshal(data, &config); err != nil {
		return config, fmt.Errorf("model config: %w", err)
	}
	if config.Default.Provider == "" || config.Default.ModelID == "" {
		return config, errors.New("model config: default requires provider and model_id")
	}
	if config.Providers == nil {
		config.Providers = map[string]ProviderConfig{}
	}
	if p, ok := config.Providers["openrouter"]; ok || config.Default.Provider == "openrouter" {
		config.Providers["openrouter"] = withOpenRouter(p)
	}
	if _, ok := config.Providers[config.Default.Provider]; !ok {
		return config, fmt.Errorf("model config: default provider %q is not configured", config.Default.Provider)
	}
	for name, p := range config.Providers {
		if p.BaseURL == "" || (p.Protocol != AnthropicMessages && p.Protocol != OpenAIChatCompletions) {
			return config, fmt.Errorf("model config: provider %q needs baseUrl and protocol %q or %q", name, AnthropicMessages, OpenAIChatCompletions)
		}
	}
	return config, nil
}

func withOpenRouter(p ProviderConfig) ProviderConfig {
	if p.BaseURL == "" {
		p.BaseURL = OpenRouter.BaseURL
	}
	if p.Protocol == "" {
		p.Protocol = OpenRouter.Protocol
	}
	if len(p.Env) == 0 {
		p.Env = OpenRouter.Env
	}
	return p
}

// ContextWindow returns a model's configured context window, or zero when unset.
func (c Config) ContextWindow(ref Ref) int {
	return c.Providers[ref.Provider].Models[ref.ModelID].ContextWindowTokens
}

// Router implements Model by dispatching each request to its provider.
type Router struct {
	Config Config
	HTTP   *http.Client
	// Getenv reads credentials; nil uses os.Getenv.
	Getenv func(string) string
}

// Generate calls the provider the request's model names.
func (r *Router) Generate(ctx context.Context, request Request, onDelta func(Delta)) (Reply, error) {
	provider, ok := r.Config.Providers[request.Model.Provider]
	if !ok {
		return Reply{}, fmt.Errorf("model provider %q is not configured", request.Model.Provider)
	}
	getenv := r.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	key := ""
	for _, name := range provider.Env {
		if key = getenv(name); key != "" {
			break
		}
	}
	if key == "" && len(provider.Env) > 0 {
		return Reply{}, fmt.Errorf("model provider %q requires one of %v", request.Model.Provider, provider.Env)
	}
	settings := provider.Models[request.Model.ModelID]
	if request.MaxOutputTokens == 0 {
		request.MaxOutputTokens = settings.MaxOutputTokens
	}
	if request.MaxOutputTokens == 0 {
		request.MaxOutputTokens = DefaultMaxOutputTokens
	}
	client := r.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	call := call{http: client, baseURL: provider.BaseURL, key: key, provider: request.Model.Provider, protocol: provider.Protocol, options: settings.Options, maxTokensField: provider.MaxTokensField,
		firstChunk: bound(r.Config.FirstChunk, DefaultFirstChunk), idle: bound(r.Config.Idle, DefaultIdle)}
	switch provider.Protocol {
	case AnthropicMessages:
		return call.anthropic(ctx, request, onDelta)
	case OpenAIChatCompletions:
		return call.openai(ctx, request, onDelta)
	}
	return Reply{}, fmt.Errorf("model provider %q uses unsupported protocol %q", request.Model.Provider, provider.Protocol)
}

func bound(value, fallback time.Duration) time.Duration {
	if value == 0 {
		return fallback
	}
	return value
}

// HTTPError is a provider rejection.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("provider returned %d: %s", e.Status, e.Body)
}

// Retryable reports whether an error is worth another attempt: rate limits, server errors, overloads, and transport failures.
func Retryable(err error) bool {
	if errors.Is(err, context.Canceled) {
		return false
	}
	if status, ok := errors.AsType[*HTTPError](err); ok {
		return status.Status == http.StatusTooManyRequests || status.Status >= 500
	}
	if stream, ok := errors.AsType[*StreamError](err); ok {
		return stream.Retryable
	}
	return true
}

// StreamError is a failure reported inside a provider's event stream.
type StreamError struct {
	Message   string
	Retryable bool
}

func (e *StreamError) Error() string { return e.Message }

// Backoff policy defaults for model retries.
const (
	DefaultRetryAttempts = 4
	DefaultRetryBase     = time.Second
	DefaultRetryMax      = 30 * time.Second
)

// Backoff returns the delay before retry attempt (zero-based) under exponential backoff, or false once attempts are exhausted.
func Backoff(err error, attempt, attempts int, base, ceiling time.Duration) (time.Duration, bool) {
	if attempt >= attempts || !Retryable(err) {
		return 0, false
	}
	delay := base << attempt
	if delay > ceiling || delay <= 0 {
		delay = ceiling
	}
	return delay, true
}
