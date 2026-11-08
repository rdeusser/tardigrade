package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sse(t *testing.T, check func(body map[string]any, r *http.Request), events ...string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(data, &body); err != nil {
			t.Errorf("request body: %v", err)
		}
		check(body, r)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, e := range events {
			fmt.Fprint(w, e)
		}
	}))
}

func router(baseURL, protocol string) *Router {
	return &Router{Config: Config{Providers: map[string]ProviderConfig{"p": {BaseURL: baseURL, Protocol: protocol, Env: []string{"KEY"}}}},
		Getenv: func(string) string { return "secret" }}
}

var conversation = []Message{
	{Role: "user", Text: "weather?"},
	{Role: "assistant", Text: "checking", ToolCalls: []ToolCall{{ID: "call_1", Name: "lookup", Input: json.RawMessage(`{"city":"Oslo"}`)}}},
	{Role: "tool", ToolCallID: "call_1", Name: "lookup", Text: "rain"},
}

func TestAnthropicStreamsTextToolsAndUsage(t *testing.T) {
	server := sse(t, func(body map[string]any, r *http.Request) {
		if r.Header.Get("x-api-key") != "secret" || body["system"] != "be brief" {
			t.Errorf("headers or system missing: %v", body)
		}
		messages := body["messages"].([]any)
		if len(messages) != 3 || messages[2].(map[string]any)["role"] != "user" {
			t.Errorf("tool result must travel as a user message: %v", messages)
		}
	},
		"event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":5}}}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"thinking\",\"thinking\":\"\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"hmm\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"signature_delta\",\"signature\":\"sig\"}}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Rainy.\"}}\n\n",
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_1\",\"name\":\"lookup\",\"input\":{}}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"city\\\":\"}}\n\n",
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"Bergen\\\"}\"}}\n\n",
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":2}\n\n",
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":7}}\n\n",
	)
	defer server.Close()
	var deltas []string
	reply, err := router(server.URL, AnthropicMessages).Generate(context.Background(), Request{Model: Ref{Provider: "p", ModelID: "m"}, System: "be brief", Messages: conversation},
		func(d Delta) { deltas = append(deltas, d.Type+":"+d.Text) })
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "Rainy." || reply.Reasoning != "hmm" || reply.FinishReason != FinishToolCalls {
		t.Fatalf("reply = %+v", reply)
	}
	if len(reply.ToolCalls) != 1 || string(reply.ToolCalls[0].Input) != `{"city":"Bergen"}` {
		t.Fatalf("tool calls = %+v", reply.ToolCalls)
	}
	if reply.Usage.Input != 15 || reply.Usage.CacheRead != 5 || reply.Usage.Output != 7 {
		t.Fatalf("usage = %+v", reply.Usage)
	}
	if reply.Continuation == nil || !strings.Contains(string(reply.Continuation.Payload), `"signature":"sig"`) {
		t.Fatalf("continuation = %+v", reply.Continuation)
	}
	if strings.Join(deltas, ",") != "reasoning:hmm,text:Rainy." {
		t.Fatalf("deltas = %v", deltas)
	}
}

func TestOpenAIStreamsToolCallsAcrossChunks(t *testing.T) {
	server := sse(t, func(body map[string]any, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing bearer token")
		}
		messages := body["messages"].([]any)
		if messages[0].(map[string]any)["role"] != "system" || messages[3].(map[string]any)["tool_call_id"] != "call_1" {
			t.Errorf("messages = %v", messages)
		}
	},
		"data: {\"choices\":[{\"delta\":{\"content\":\"Let me \"}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"content\":\"check.\",\"tool_calls\":[{\"index\":0,\"id\":\"call_2\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\"ci\"}}]}}]}\n\n",
		"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"arguments\":\"ty\\\":\\\"Rome\\\"}\"}}]},\"finish_reason\":\"tool_calls\"}]}\n\n",
		"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":20,\"completion_tokens\":4,\"cost\":0.001}}\n\n",
		"data: [DONE]\n\n",
	)
	defer server.Close()
	reply, err := router(server.URL, OpenAIChatCompletions).Generate(context.Background(), Request{Model: Ref{Provider: "p", ModelID: "m"}, System: "sys", Messages: conversation}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Text != "Let me check." || reply.FinishReason != FinishToolCalls || len(reply.ToolCalls) != 1 || string(reply.ToolCalls[0].Input) != `{"city":"Rome"}` {
		t.Fatalf("reply = %+v", reply)
	}
	if reply.Usage.Input != 20 || reply.Usage.USD == nil || *reply.Usage.USD != 0.001 {
		t.Fatalf("usage = %+v", reply.Usage)
	}
}

func TestProviderRejectionIsRetryableByStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error":"slow down"}`, http.StatusTooManyRequests)
	}))
	defer server.Close()
	_, err := router(server.URL, OpenAIChatCompletions).Generate(context.Background(), Request{Model: Ref{Provider: "p", ModelID: "m"}}, nil)
	var status *HTTPError
	if !errors.As(err, &status) || !Retryable(err) {
		t.Fatalf("err = %v", err)
	}
	if delay, ok := Backoff(err, 2, DefaultRetryAttempts, DefaultRetryBase, DefaultRetryMax); !ok || delay != 4*time.Second {
		t.Fatalf("backoff = %v, %v", delay, ok)
	}
}

func TestStalledStreamTimesOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer server.Close()
	r := router(server.URL, OpenAIChatCompletions)
	r.Config.FirstChunk = 50 * time.Millisecond
	_, err := r.Generate(context.Background(), Request{Model: Ref{Provider: "p", ModelID: "m"}}, nil)
	var stream *StreamError
	if !errors.As(err, &stream) || !stream.Retryable {
		t.Fatalf("err = %v", err)
	}
}

func TestParseConfigAppliesPresets(t *testing.T) {
	config, err := ParseConfig([]byte(`{"default":{"provider":"openrouter","model_id":"anthropic/claude-sonnet-4.6"},"providers":{"openrouter":{"models":{"anthropic/claude-sonnet-4.6":{"contextWindowTokens":200000}}}}}`))
	if err != nil {
		t.Fatal(err)
	}
	provider := config.Providers["openrouter"]
	if provider.BaseURL != "https://openrouter.ai/api/v1" || provider.Protocol != OpenAIChatCompletions {
		t.Fatalf("provider = %+v", provider)
	}
	if config.ContextWindow(config.Default) != 200000 {
		t.Fatal("context window missing")
	}
}
