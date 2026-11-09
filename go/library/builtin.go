package library

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// FilesystemPolicy bounds workspace commands and reads.
type FilesystemPolicy struct {
	CommandTimeout time.Duration
	MaxOutputChars int
	MaxReadChars   int
	// MaxFileBytes bounds binary files returned whole by read.
	MaxFileBytes int
}

// DefaultFilesystemPolicy is the policy LocalWorkspace applies to zero fields.
var DefaultFilesystemPolicy = FilesystemPolicy{CommandTimeout: 30 * time.Second, MaxOutputChars: 32_768, MaxReadChars: 32_768, MaxFileBytes: 5 << 20}

// Workspace supplies files and a shell to the filesystem tools.
type Workspace interface {
	Bash(ctx context.Context, command string) (string, error)
	Read(path string, offset, limit int) (Content, error)
	Write(path, content string) error
	Edit(path, oldText, newText string) error
}

// LocalWorkspace runs commands and file operations inside a directory.
type LocalWorkspace struct {
	Dir    string
	Policy FilesystemPolicy
}

func (w LocalWorkspace) policy() FilesystemPolicy {
	p := w.Policy
	d := DefaultFilesystemPolicy
	if p.CommandTimeout == 0 {
		p.CommandTimeout = d.CommandTimeout
	}
	if p.MaxOutputChars == 0 {
		p.MaxOutputChars = d.MaxOutputChars
	}
	if p.MaxReadChars == 0 {
		p.MaxReadChars = d.MaxReadChars
	}
	if p.MaxFileBytes == 0 {
		p.MaxFileBytes = d.MaxFileBytes
	}
	return p
}

// resolve confines a workspace path to the directory.
func (w LocalWorkspace) resolve(path string) (string, error) {
	root, err := filepath.Abs(w.Dir)
	if err != nil {
		return "", err
	}
	full := filepath.Join(root, path)
	if filepath.IsAbs(path) {
		full = filepath.Clean(path)
	}
	if full != root && !strings.HasPrefix(full, root+string(filepath.Separator)) {
		return "", fmt.Errorf("path %q is outside the workspace", path)
	}
	return full, nil
}

// Truncate marks omitted text with the applied character bound.
func Truncate(text string, limit int) string {
	if utf8.RuneCountInString(text) <= limit {
		return text
	}
	runes := []rune(text)
	return fmt.Sprintf("%s\n[truncated: %d characters, limit %d]", string(runes[:limit]), len(runes), limit)
}

func (w LocalWorkspace) Bash(ctx context.Context, command string) (string, error) {
	policy := w.policy()
	ctx, cancel := context.WithTimeout(ctx, policy.CommandTimeout)
	defer cancel()
	if err := os.MkdirAll(w.Dir, 0o755); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, "bash", "-c", command)
	cmd.Dir = w.Dir
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	err := cmd.Run()
	text := Truncate(output.String(), policy.MaxOutputChars)
	var exit *exec.ExitError
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		return text, fmt.Errorf("command timed out after %s\n%s", policy.CommandTimeout, text)
	case errors.As(err, &exit):
		return fmt.Sprintf("%s\n[exit code %d]", text, exit.ExitCode()), nil
	}
	return text, err
}

func (w LocalWorkspace) Read(path string, offset, limit int) (Content, error) {
	full, err := w.resolve(path)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return nil, err
	}
	policy := w.policy()
	if !utf8.Valid(data) {
		if len(data) > policy.MaxFileBytes {
			return nil, fmt.Errorf("file is %d bytes, limit %d", len(data), policy.MaxFileBytes)
		}
		mediaType := mime.TypeByExtension(filepath.Ext(full))
		if mediaType == "" {
			mediaType = http.DetectContentType(data)
		}
		return Content{{Type: model.PartFile, MediaType: strings.Split(mediaType, ";")[0], Data: data, Filename: filepath.Base(full)}}, nil
	}
	lines := strings.Split(string(data), "\n")
	if offset < 1 {
		offset = 1
	}
	if offset > len(lines) {
		return Content{{Type: model.PartText, Text: fmt.Sprintf("[offset %d is beyond the file's %d lines]", offset, len(lines))}}, nil
	}
	end := len(lines)
	if limit > 0 && offset-1+limit < end {
		end = offset - 1 + limit
	}
	text := strings.Join(lines[offset-1:end], "\n")
	if utf8.RuneCountInString(text) > policy.MaxReadChars {
		text = Truncate(text, policy.MaxReadChars) + "\nRead again with a later offset or a smaller limit."
	} else if end < len(lines) {
		text += fmt.Sprintf("\n[%d more lines; continue with offset %d]", len(lines)-end, end+1)
	}
	return Content{{Type: model.PartText, Text: text}}, nil
}

func (w LocalWorkspace) Write(path, content string) error {
	full, err := w.resolve(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		return err
	}
	return os.WriteFile(full, []byte(content), 0o644)
}

// EditText replaces exactly one nonempty literal occurrence of oldText.
func EditText(text, oldText, newText string) (string, error) {
	if oldText == "" {
		return "", errors.New("oldText must not be empty")
	}
	if strings.Count(text, oldText) != 1 {
		return "", errors.New("expected oldText exactly once in the file")
	}
	return strings.Replace(text, oldText, newText, 1), nil
}

func (w LocalWorkspace) Edit(path, oldText, newText string) error {
	full, err := w.resolve(path)
	if err != nil {
		return err
	}
	data, err := os.ReadFile(full)
	if err != nil {
		return err
	}
	edited, err := EditText(string(data), oldText, newText)
	if err != nil {
		return err
	}
	return os.WriteFile(full, []byte(edited), 0o644)
}

// Filesystem declares bash, read, write, and edit tools over a workspace.
func Filesystem(w Workspace) Library {
	type bash struct {
		Command string `json:"command"`
	}
	type read struct {
		Path   string `json:"path"`
		Offset int    `json:"offset,omitempty" desc:"One-based starting line"`
		Limit  int    `json:"limit,omitempty" desc:"Maximum line count"`
	}
	type write struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}
	type edit struct {
		Path    string `json:"path"`
		OldText string `json:"oldText"`
		NewText string `json:"newText"`
	}
	return New("filesystem", "Files and a shell in the agent's workspace",
		Tool("bash", "Run a shell command in the workspace and return stdout, stderr, and any nonzero exit code", func(c Call, in bash) (string, error) {
			return w.Bash(c.X.Context(), in.Command)
		}, Named("bash")),
		Tool("read", "Read workspace text or file content. offset is a one-based starting line and limit the maximum line count. Binary files are returned whole.", func(_ Call, in read) (Content, error) {
			return w.Read(in.Path, in.Offset, in.Limit)
		}, Named("read"), Hints(Annotations{ReadOnlyHint: new(true), OpenWorldHint: new(false)})),
		Tool("write", "Create or overwrite a file in the workspace", func(_ Call, in write) (string, error) {
			return "Wrote " + in.Path, w.Write(in.Path, in.Content)
		}, Named("write")),
		Tool("edit", "Replace one exact nonempty occurrence of oldText in a workspace file", func(_ Call, in edit) (string, error) {
			return "Edited " + in.Path, w.Edit(in.Path, in.OldText, in.NewText)
		}, Named("edit")),
	)
}

// DefaultFetchMaxBytes bounds response bodies fetch reads.
const DefaultFetchMaxBytes = 8 << 20

// Fetch declares a tool reading HTTP and HTTPS resources; maxBytes zero uses DefaultFetchMaxBytes.
func Fetch(client *http.Client, maxBytes int64) Library {
	if client == nil {
		client = http.DefaultClient
	}
	if maxBytes == 0 {
		maxBytes = DefaultFetchMaxBytes
	}
	type input struct {
		URL string `json:"url"`
	}
	type output struct {
		URL       string `json:"url"`
		Status    int    `json:"status"`
		OK        bool   `json:"ok"`
		Body      string `json:"body"`
		Truncated bool   `json:"truncated,omitempty"`
	}
	return New("fetch", "Read HTTP and HTTPS resources.",
		Tool("get", "GET a URL and return its response body.", func(c Call, in input) (output, error) {
			target, err := url.Parse(in.URL)
			if err != nil || (target.Scheme != "http" && target.Scheme != "https") {
				return output{}, errors.New("expected an HTTP or HTTPS URL")
			}
			request, err := http.NewRequestWithContext(c.X.Context(), http.MethodGet, target.String(), nil)
			if err != nil {
				return output{}, err
			}
			response, err := client.Do(request)
			if err != nil {
				return output{}, err
			}
			defer response.Body.Close()
			body, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
			if err != nil {
				return output{}, err
			}
			truncated := int64(len(body)) > maxBytes
			if truncated {
				body = body[:maxBytes]
			}
			return output{URL: response.Request.URL.String(), Status: response.StatusCode, OK: response.StatusCode < 300, Body: string(body), Truncated: truncated}, nil
		}, Named("fetch_url"), Hints(Annotations{ReadOnlyHint: new(true), OpenWorldHint: new(true)})),
	)
}

// DefaultNotesMaxChars bounds each notes read.
const DefaultNotesMaxChars = 32_768

// Notes declares named text values kept in memory for the lifetime of the library value; maxChars zero uses DefaultNotesMaxChars.
func Notes(maxChars int) Library {
	if maxChars == 0 {
		maxChars = DefaultNotesMaxChars
	}
	var mu sync.Mutex
	values := map[string]string{}
	type write struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	type read struct {
		Key    string `json:"key"`
		Offset int    `json:"offset,omitempty"`
	}
	type readOutput struct {
		Key        string `json:"key"`
		Value      string `json:"value"`
		Offset     int    `json:"offset"`
		TotalChars int    `json:"totalChars"`
		Truncated  bool   `json:"truncated"`
	}
	return New("workspace", "Read and write named text values in the agent workspace.",
		Tool("write", "Store a text value by key, replacing any existing value.", func(_ Call, in write) (map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			values[in.Key] = in.Value
			return map[string]any{"key": in.Key, "chars": utf8.RuneCountInString(in.Value)}, nil
		}, Named("write_text")),
		Tool("read", fmt.Sprintf("Read up to %d characters from a key, optionally starting at offset.", maxChars), func(_ Call, in read) (readOutput, error) {
			mu.Lock()
			value, ok := values[in.Key]
			mu.Unlock()
			if !ok {
				return readOutput{}, fmt.Errorf("no value for key %q", in.Key)
			}
			runes := []rune(value)
			start := min(max(in.Offset, 0), len(runes))
			end := min(start+maxChars, len(runes))
			return readOutput{Key: in.Key, Value: string(runes[start:end]), Offset: start, TotalChars: len(runes), Truncated: end < len(runes)}, nil
		}, Named("read_text"), Hints(Annotations{ReadOnlyHint: new(true)})),
		Tool("list", "List stored keys.", func(Call, struct{}) ([]string, error) {
			mu.Lock()
			defer mu.Unlock()
			keys := make([]string, 0, len(values))
			for k := range values {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			return keys, nil
		}, Named("list_texts"), Hints(Annotations{ReadOnlyHint: new(true)})),
	)
}

// Promise identifies a background tool's pending result.
type Promise struct {
	Ref    tardigrade.Ref    `json:"ref"`
	Handle tardigrade.Handle `json:"handle"`
}

// Alarm declares reminders delivered as clock promises.
func Alarm() Library {
	type set struct {
		Message      string  `json:"message"`
		AfterSeconds float64 `json:"afterSeconds,omitempty"`
		At           string  `json:"at,omitempty" desc:"RFC 3339 time with a timezone"`
	}
	type cancel struct {
		Promise Promise `json:"promise"`
	}
	return New("alarm", "Schedule and cancel reminders.",
		Task("set_alarm", "Schedule a reminder and return its durable handle immediately. Supply message and either afterSeconds or an RFC 3339 time with a timezone in at. The reminder arrives in the inbox when it is due.", func(c Call, in set) (tardigrade.Handle, error) {
			if strings.TrimSpace(in.Message) == "" {
				return tardigrade.Handle{}, errors.New("alarm message must not be empty")
			}
			var at time.Time
			switch {
			case in.At != "" && in.AfterSeconds == 0:
				parsed, err := time.Parse(time.RFC3339, in.At)
				if err != nil {
					return tardigrade.Handle{}, errors.New("at must be an RFC 3339 time with a timezone")
				}
				at = parsed
			case in.At == "" && in.AfterSeconds > 0:
				at = time.Now().Add(time.Duration(in.AfterSeconds * float64(time.Second)))
			default:
				return tardigrade.Handle{}, errors.New("supply either a positive afterSeconds or at")
			}
			value, _ := json.Marshal(map[string]any{"alarmId": "alarm:" + c.CallID, "at": at.UnixMilli(), "message": in.Message})
			return tardigrade.Handle{Executor: tardigrade.ExecutorClock, ID: "alarm:" + c.CallID, At: at.UnixMilli(), Value: value}, nil
		}, Hints(Annotations{ReadOnlyHint: new(false), DestructiveHint: new(false)})),
		Tool("cancel_alarm", "Cancel a pending reminder using the complete promise set_alarm returned.", func(c Call, in cancel) (map[string]any, error) {
			if in.Promise.Handle.Executor != tardigrade.ExecutorClock {
				return nil, errors.New("expected a clock promise")
			}
			if state := tardigrade.State(c.X, tardigrade.PromiseOf(in.Promise.Ref)); state.Finished {
				return map[string]any{"cancelled": false, "reason": "the reminder already finished"}, nil
			}
			return map[string]any{"cancelled": true}, c.X.Cancel(in.Promise.Ref, "alarm cancelled")
		}, Hints(Annotations{DestructiveHint: new(true), IdempotentHint: new(true)})),
	)
}

// Delegator runs method calls on child threads, which may belong to another actor, as host.Host does.
type Delegator interface {
	Delegate(parent tardigrade.ThreadCoordinate, ref tardigrade.Ref, actor, method string, input any) (tardigrade.Handle, error)
	CancelDelegation(handle tardigrade.Handle, reason string) error
}

// Agents declares delegation of work to child threads of actor through its "message" method; input builds that method's input from the delegated text.
func Agents(d Delegator, actor string, input func(message string) any) Library {
	type run struct {
		Message string `json:"message"`
	}
	type cancel struct {
		Handle tardigrade.Handle `json:"handle"`
	}
	return New("agents", "Delegate work to child agents.",
		Task("run", "Start a child agent with a message. Returns a durable handle immediately; its result arrives in the inbox.", func(c Call, in run) (tardigrade.Handle, error) {
			return d.Delegate(c.X.Address(), c.X.Ref(), actor, "message", input(in.Message))
		}, Named("delegate_task"), Hints(Annotations{ReadOnlyHint: new(false), OpenWorldHint: new(true)}), OnCancel(func(c tardigrade.Cleanup, _ Call) error {
			if c.Handle == nil {
				return nil
			}
			return d.CancelDelegation(*c.Handle, c.Reason)
		})),
		Tool("cancel", "Cancel a child using its execution handle. Its promise finishes as cancelled if the child is still running.", func(_ Call, in cancel) (map[string]any, error) {
			return map[string]any{"handle": in.Handle, "cancellationRequested": true}, d.CancelDelegation(in.Handle, "cancelled by parent")
		}, Named("cancel_agent"), Hints(Annotations{DestructiveHint: new(true), IdempotentHint: new(true)})),
	)
}
