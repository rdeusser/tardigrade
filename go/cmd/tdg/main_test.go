package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

type buffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *buffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *buffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func TestServeAndCallAgent(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hello \"}}]}\n\n")
		fmt.Fprint(w, "data: {\"choices\":[{\"delta\":{\"content\":\"there\"},\"finish_reason\":\"stop\"}]}\n\n")
		fmt.Fprint(w, "data: [DONE]\n\n")
	}))
	defer provider.Close()
	dir := t.TempDir()
	t.Setenv("TARDIGRADE_CONFIG", fmt.Sprintf(`{"models":{"default":{"provider":"fake","model_id":"m"},"providers":{"fake":{"baseUrl":%q,"protocol":"openai-chat-completions","models":{"m":{"contextWindowTokens":10000}}}}}}`, provider.URL))
	t.Setenv("TARDIGRADE_TOKEN", "secret")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := &buffer{}
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"serve", "--addr", "127.0.0.1:0", "--data", dir + "/data", "--workspace", dir + "/workspace"}, nil, served)
	}()
	address := regexp.MustCompile(`http://\S+`)
	deadline := time.Now().Add(10 * time.Second)
	for address.FindString(served.String()) == "" {
		if time.Now().After(deadline) {
			t.Fatalf("server did not start: %q", served.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	url := address.FindString(served.String())
	tdg := func(args ...string) string {
		t.Helper()
		var out bytes.Buffer
		if err := run(ctx, append(args, "--url", url), strings.NewReader(""), &out); err != nil {
			t.Fatalf("tdg %v: %v", args, err)
		}
		return out.String()
	}
	if actors := tdg("actors"); !strings.Contains(actors, `"name": "message"`) {
		t.Fatalf("actors = %s", actors)
	}
	tdg("thread", "create", "t1")
	result := tdg("call", "t1", "message", `{"text":"hi"}`, "--id", "c1", "--wait")
	if !strings.Contains(result, `"status": "CALL_STATUS_COMPLETED"`) || !strings.Contains(result, `hello there`) {
		t.Fatalf("call = %s", result)
	}
	if state := tdg("state", "t1", "c1"); !strings.Contains(state, `"method": "message"`) {
		t.Fatalf("state = %s", state)
	}
	if events := tdg("events", "t1"); !strings.Contains(events, "TurnFinished") {
		t.Fatalf("events = %s", events)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
