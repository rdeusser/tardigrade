package model

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

type call struct {
	http           *http.Client
	baseURL        string
	key            string
	provider       string
	protocol       string
	options        map[string]json.RawMessage
	maxTokensField string
	firstChunk     time.Duration
	idle           time.Duration
}

// MaxEventBytes bounds one line of a provider's event stream, and MaxErrorBytes the rejection body kept in HTTPError.
const (
	MaxEventBytes = 16 << 20
	MaxErrorBytes = 64 << 10
)

var errStreamStalled = errors.New("provider stream stalled")

// stream posts body and calls visit with each SSE event name and data, bounding the wait for the first and each later chunk.
func (c call) stream(ctx context.Context, path string, header http.Header, body map[string]any, visit func(event string, data []byte) error) error {
	for key, value := range c.options {
		body[key] = value
	}
	data, err := json.Marshal(body)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var mu sync.Mutex
	timer := time.AfterFunc(max(c.firstChunk, 0), func() { cancel(errStreamStalled) })
	if c.firstChunk < 0 {
		timer.Stop()
	}
	defer timer.Stop()
	touch := func() {
		mu.Lock()
		defer mu.Unlock()
		if c.idle > 0 {
			timer.Reset(c.idle)
		} else {
			timer.Stop()
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.baseURL, "/")+path, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header = header
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "text/event-stream")
	response, err := c.http.Do(request)
	if err != nil {
		return stalled(ctx, err)
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(response.Body, MaxErrorBytes))
		return &HTTPError{Status: response.StatusCode, Body: strings.TrimSpace(string(detail))}
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), MaxEventBytes)
	event := ""
	for scanner.Scan() {
		touch()
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if payload == "[DONE]" {
				return nil
			}
			if err := visit(event, []byte(payload)); err != nil {
				return err
			}
		case line == "":
			event = ""
		}
	}
	return stalled(ctx, scanner.Err())
}

func stalled(ctx context.Context, err error) error {
	if errors.Is(context.Cause(ctx), errStreamStalled) {
		return &StreamError{Message: "provider stream stalled", Retryable: true}
	}
	return err
}

func encodeData(part Part) string {
	return base64.StdEncoding.EncodeToString(part.Data)
}

// anthropic speaks the Anthropic Messages API.
func (c call) anthropic(ctx context.Context, request Request, onDelta func(Delta)) (Reply, error) {
	var messages []map[string]any
	push := func(role string, blocks ...map[string]any) {
		if len(blocks) == 0 {
			return
		}
		if n := len(messages); n > 0 && messages[n-1]["role"] == role {
			messages[n-1]["content"] = append(messages[n-1]["content"].([]any), anySlice(blocks)...)
			return
		}
		messages = append(messages, map[string]any{"role": role, "content": anySlice(blocks)})
	}
	for _, m := range request.Messages {
		switch m.Role {
		case "user":
			push("user", anthropicParts(m)...)
		case "assistant":
			if m.Continuation != nil && m.Continuation.Provider == c.provider && m.Continuation.Protocol == c.protocol && m.Continuation.Model == request.Model.ModelID {
				var blocks []map[string]any
				if err := json.Unmarshal(m.Continuation.Payload, &blocks); err == nil {
					push("assistant", blocks...)
					continue
				}
			}
			var blocks []map[string]any
			if m.Text != "" {
				blocks = append(blocks, map[string]any{"type": "text", "text": m.Text})
			}
			for _, call := range m.ToolCalls {
				blocks = append(blocks, map[string]any{"type": "tool_use", "id": call.ID, "name": call.Name, "input": rawOrEmpty(call.Input)})
			}
			push("assistant", blocks...)
		case "tool":
			result := map[string]any{"type": "tool_result", "tool_use_id": m.ToolCallID, "is_error": m.Error}
			if len(m.Content) > 0 {
				result["content"] = anySlice(anthropicParts(m))
			} else {
				result["content"] = m.Text
			}
			push("user", result)
		}
	}
	body := map[string]any{"model": request.Model.ModelID, "max_tokens": request.MaxOutputTokens, "messages": messages, "stream": true}
	if request.System != "" {
		body["system"] = request.System
	}
	if len(request.Tools) > 0 {
		tools := make([]map[string]any, len(request.Tools))
		for i, t := range request.Tools {
			tools[i] = map[string]any{"name": t.Name, "description": t.Description, "input_schema": t.InputSchema}
		}
		body["tools"] = tools
	}
	header := http.Header{}
	header.Set("x-api-key", c.key)
	header.Set("anthropic-version", "2023-06-01")
	type block struct {
		Type      string          `json:"type"`
		Text      string          `json:"text,omitempty"`
		Thinking  string          `json:"thinking,omitempty"`
		Signature string          `json:"signature,omitempty"`
		Data      string          `json:"data,omitempty"`
		ID        string          `json:"id,omitempty"`
		Name      string          `json:"name,omitempty"`
		Input     json.RawMessage `json:"input,omitempty"`
		partial   strings.Builder
	}
	var blocks []*block
	var reply Reply
	err := c.stream(ctx, "/v1/messages", header, body, func(_ string, data []byte) error {
		var event struct {
			Type         string `json:"type"`
			Index        int    `json:"index"`
			ContentBlock *block `json:"content_block"`
			Delta        struct {
				Type        string `json:"type"`
				Text        string `json:"text"`
				PartialJSON string `json:"partial_json"`
				Thinking    string `json:"thinking"`
				Signature   string `json:"signature"`
				StopReason  string `json:"stop_reason"`
			} `json:"delta"`
			Message struct {
				Usage anthropicUsage `json:"usage"`
			} `json:"message"`
			Usage anthropicUsage `json:"usage"`
			Error struct {
				Type    string `json:"type"`
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			return err
		}
		switch event.Type {
		case "message_start":
			event.Message.Usage.add(&reply.Usage)
		case "content_block_start":
			for len(blocks) <= event.Index {
				blocks = append(blocks, &block{})
			}
			if event.ContentBlock != nil {
				blocks[event.Index] = event.ContentBlock
			}
		case "content_block_delta":
			if event.Index >= len(blocks) {
				return fmt.Errorf("anthropic: delta for unknown block %d", event.Index)
			}
			b := blocks[event.Index]
			switch event.Delta.Type {
			case "text_delta":
				b.Text += event.Delta.Text
				emit(onDelta, "text", event.Delta.Text)
			case "thinking_delta":
				b.Thinking += event.Delta.Thinking
				emit(onDelta, "reasoning", event.Delta.Thinking)
			case "signature_delta":
				b.Signature += event.Delta.Signature
			case "input_json_delta":
				b.partial.WriteString(event.Delta.PartialJSON)
			}
		case "content_block_stop":
			if event.Index < len(blocks) && blocks[event.Index].Type == "tool_use" {
				b := blocks[event.Index]
				if b.partial.Len() > 0 {
					b.Input = json.RawMessage(b.partial.String())
				}
				if len(b.Input) == 0 {
					b.Input = json.RawMessage("{}")
				}
			}
		case "message_delta":
			reply.FinishReason = map[string]string{"max_tokens": FinishLength, "tool_use": FinishToolCalls}[event.Delta.StopReason]
			if event.Usage.OutputTokens > 0 {
				reply.Usage.Output = event.Usage.OutputTokens
			}
		case "error":
			return &StreamError{Message: event.Error.Type + ": " + event.Error.Message, Retryable: event.Error.Type == "overloaded_error" || event.Error.Type == "api_error"}
		}
		return nil
	})
	if err != nil {
		return Reply{}, err
	}
	if reply.FinishReason == "" {
		reply.FinishReason = FinishStop
	}
	signed := false
	for _, b := range blocks {
		switch b.Type {
		case "text":
			reply.Text += b.Text
		case "thinking":
			reply.Reasoning += b.Thinking
			signed = true
		case "redacted_thinking":
			signed = true
		case "tool_use":
			reply.ToolCalls = append(reply.ToolCalls, ToolCall{ID: b.ID, Name: b.Name, Input: b.Input})
		}
	}
	if signed {
		payload, err := json.Marshal(blocks)
		if err != nil {
			return Reply{}, err
		}
		reply.Continuation = &Continuation{Provider: c.provider, Protocol: c.protocol, Model: request.Model.ModelID, Payload: payload}
	}
	return reply, nil
}

type anthropicUsage struct {
	InputTokens              int `json:"input_tokens"`
	OutputTokens             int `json:"output_tokens"`
	CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     int `json:"cache_read_input_tokens"`
}

func (u anthropicUsage) add(usage *Usage) {
	usage.Input = u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens
	usage.CacheRead, usage.CacheWrite = u.CacheReadInputTokens, u.CacheCreationInputTokens
	if u.OutputTokens > 0 {
		usage.Output = u.OutputTokens
	}
}

func anthropicParts(m Message) []map[string]any {
	if len(m.Content) == 0 {
		if m.Text == "" {
			return nil
		}
		return []map[string]any{{"type": "text", "text": m.Text}}
	}
	var blocks []map[string]any
	for _, p := range m.Content {
		switch {
		case p.Type == "text" && p.Text != "":
			blocks = append(blocks, map[string]any{"type": "text", "text": p.Text})
		case p.Type == "file" && strings.HasPrefix(p.MediaType, "image/"):
			blocks = append(blocks, map[string]any{"type": "image", "source": map[string]any{"type": "base64", "media_type": p.MediaType, "data": encodeData(p)}})
		case p.Type == "file" && p.MediaType == "application/pdf":
			blocks = append(blocks, map[string]any{"type": "document", "source": map[string]any{"type": "base64", "media_type": p.MediaType, "data": encodeData(p)}})
		case p.Type == "file":
			blocks = append(blocks, map[string]any{"type": "text", "text": fmt.Sprintf("[file %s (%s) omitted]", p.Filename, p.MediaType)})
		}
	}
	return blocks
}

// openai speaks the OpenAI Chat Completions API and its compatible providers.
func (c call) openai(ctx context.Context, request Request, onDelta func(Delta)) (Reply, error) {
	messages := []map[string]any{}
	if request.System != "" {
		messages = append(messages, map[string]any{"role": "system", "content": request.System})
	}
	for _, m := range request.Messages {
		switch m.Role {
		case "user":
			messages = append(messages, map[string]any{"role": "user", "content": openaiContent(m)})
		case "assistant":
			message := map[string]any{"role": "assistant", "content": m.Text}
			if len(m.ToolCalls) > 0 {
				calls := make([]map[string]any, len(m.ToolCalls))
				for i, call := range m.ToolCalls {
					calls[i] = map[string]any{"id": call.ID, "type": "function", "function": map[string]any{"name": call.Name, "arguments": string(rawOrEmpty(call.Input))}}
				}
				message["tool_calls"] = calls
			}
			messages = append(messages, message)
		case "tool":
			text := m.Text
			if text == "" {
				text = TextOf(m.Content, "\n")
			}
			messages = append(messages, map[string]any{"role": "tool", "tool_call_id": m.ToolCallID, "content": text})
			if images := imageParts(m.Content); len(images) > 0 {
				messages = append(messages, map[string]any{"role": "user", "content": append([]map[string]any{{"type": "text", "text": "Files returned by tool call " + m.ToolCallID + ":"}}, images...)})
			}
		}
	}
	field := c.maxTokensField
	if field == "" {
		field = "max_tokens"
	}
	body := map[string]any{"model": request.Model.ModelID, "messages": messages, "stream": true, "stream_options": map[string]any{"include_usage": true}, field: request.MaxOutputTokens}
	if len(request.Tools) > 0 {
		tools := make([]map[string]any, len(request.Tools))
		for i, t := range request.Tools {
			tools[i] = map[string]any{"type": "function", "function": map[string]any{"name": t.Name, "description": t.Description, "parameters": t.InputSchema}}
		}
		body["tools"] = tools
	}
	header := http.Header{}
	if c.key != "" {
		header.Set("Authorization", "Bearer "+c.key)
	}
	type partial struct {
		id, name string
		args     strings.Builder
	}
	var calls []*partial
	var reply Reply
	err := c.stream(ctx, "/chat/completions", header, body, func(_ string, data []byte) error {
		var chunk struct {
			Choices []struct {
				Delta struct {
					Content          string `json:"content"`
					Reasoning        string `json:"reasoning"`
					ReasoningContent string `json:"reasoning_content"`
					ToolCalls        []struct {
						Index    int    `json:"index"`
						ID       string `json:"id"`
						Function struct {
							Name      string `json:"name"`
							Arguments string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
				FinishReason string `json:"finish_reason"`
			} `json:"choices"`
			Usage *struct {
				PromptTokens        int      `json:"prompt_tokens"`
				CompletionTokens    int      `json:"completion_tokens"`
				Cost                *float64 `json:"cost"`
				PromptTokensDetails struct {
					CachedTokens int `json:"cached_tokens"`
				} `json:"prompt_tokens_details"`
			} `json:"usage"`
			Error *struct {
				Message string `json:"message"`
				Code    any    `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(data, &chunk); err != nil {
			return err
		}
		if chunk.Error != nil {
			return &StreamError{Message: chunk.Error.Message, Retryable: true}
		}
		if chunk.Usage != nil {
			reply.Usage = Usage{Input: chunk.Usage.PromptTokens, Output: chunk.Usage.CompletionTokens, CacheRead: chunk.Usage.PromptTokensDetails.CachedTokens, USD: chunk.Usage.Cost}
		}
		for _, choice := range chunk.Choices {
			if choice.Delta.Content != "" {
				reply.Text += choice.Delta.Content
				emit(onDelta, "text", choice.Delta.Content)
			}
			if reasoning := choice.Delta.Reasoning + choice.Delta.ReasoningContent; reasoning != "" {
				reply.Reasoning += reasoning
				emit(onDelta, "reasoning", reasoning)
			}
			for _, tc := range choice.Delta.ToolCalls {
				for len(calls) <= tc.Index {
					calls = append(calls, &partial{})
				}
				p := calls[tc.Index]
				if tc.ID != "" {
					p.id = tc.ID
				}
				if tc.Function.Name != "" {
					p.name = tc.Function.Name
				}
				p.args.WriteString(tc.Function.Arguments)
			}
			switch choice.FinishReason {
			case "length":
				reply.FinishReason = FinishLength
			case "tool_calls":
				reply.FinishReason = FinishToolCalls
			case "stop":
				reply.FinishReason = FinishStop
			}
		}
		return nil
	})
	if err != nil {
		return Reply{}, err
	}
	if reply.FinishReason == "" {
		reply.FinishReason = FinishStop
	}
	for _, p := range calls {
		input := json.RawMessage(p.args.String())
		if len(bytes.TrimSpace(input)) == 0 {
			input = json.RawMessage("{}")
		}
		if !json.Valid(input) {
			return Reply{}, fmt.Errorf("tool call %s returned invalid JSON arguments", p.name)
		}
		reply.ToolCalls = append(reply.ToolCalls, ToolCall{ID: p.id, Name: p.name, Input: input})
	}
	return reply, nil
}

func openaiContent(m Message) any {
	if len(m.Content) == 0 {
		return m.Text
	}
	var parts []map[string]any
	for _, p := range m.Content {
		switch {
		case p.Type == "text":
			parts = append(parts, map[string]any{"type": "text", "text": p.Text})
		case p.Type == "file" && strings.HasPrefix(p.MediaType, "image/"):
			parts = append(parts, imagePart(p))
		case p.Type == "file":
			parts = append(parts, map[string]any{"type": "text", "text": fmt.Sprintf("[file %s (%s) omitted]", p.Filename, p.MediaType)})
		}
	}
	return parts
}

func imagePart(p Part) map[string]any {
	return map[string]any{"type": "image_url", "image_url": map[string]any{"url": "data:" + p.MediaType + ";base64," + encodeData(p)}}
}

func imageParts(content []Part) []map[string]any {
	var images []map[string]any
	for _, p := range content {
		if p.Type == "file" && strings.HasPrefix(p.MediaType, "image/") {
			images = append(images, imagePart(p))
		}
	}
	return images
}

func emit(onDelta func(Delta), kind, text string) {
	if onDelta != nil && text != "" {
		onDelta(Delta{Type: kind, Text: text})
	}
}

func rawOrEmpty(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return json.RawMessage("{}")
	}
	return raw
}

func anySlice(blocks []map[string]any) []any {
	out := make([]any, len(blocks))
	for i, b := range blocks {
		out[i] = b
	}
	return out
}
