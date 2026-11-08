package library

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLocalWorkspaceConfinesPaths(t *testing.T) {
	w := LocalWorkspace{Dir: t.TempDir()}
	for _, path := range []string{"../escape.txt", "/etc/passwd", "a/../../b"} {
		if err := w.Write(path, "x"); err == nil || !strings.Contains(err.Error(), "outside the workspace") {
			t.Fatalf("write %q = %v", path, err)
		}
	}
	if err := w.Write("notes/a.txt", "one\ntwo\nthree"); err != nil {
		t.Fatal(err)
	}
	content, err := w.Read("notes/a.txt", 2, 1)
	if err != nil || content[0].Text != "two\n[1 more lines; continue with offset 3]" {
		t.Fatalf("read = %+v, %v", content, err)
	}
}

func TestEditRequiresExactlyOneMatch(t *testing.T) {
	w := LocalWorkspace{Dir: t.TempDir()}
	_ = w.Write("f", "a b a")
	if err := w.Edit("f", "a", "c"); err == nil {
		t.Fatal("ambiguous edit applied")
	}
	if err := w.Edit("f", "b", "c"); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(filepath.Join(w.Dir, "f"))
	if string(data) != "a c a" {
		t.Fatalf("file = %q", data)
	}
}

func TestBashReportsExitCodeAndBoundsOutput(t *testing.T) {
	w := LocalWorkspace{Dir: t.TempDir(), Policy: FilesystemPolicy{MaxOutputChars: 5}}
	out, err := w.Bash(context.Background(), "printf 'abcdefgh'; exit 3")
	if err != nil || !strings.Contains(out, "abcde\n[truncated: 8 characters, limit 5]") || !strings.Contains(out, "[exit code 3]") {
		t.Fatalf("bash = %q, %v", out, err)
	}
}

func TestRegistryRunsToolsWithSchemas(t *testing.T) {
	notes := Notes(3)
	registry := NewRegistry(notes)
	if _, err := registry.Run(Call{Name: "write_text"}, json.RawMessage(`{"key":"k","value":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	out, err := registry.Run(Call{Name: "read_text"}, json.RawMessage(`{"key":"k"}`))
	if err != nil || !strings.Contains(out.Content[0].Text, `"value":"hel"`) || !strings.Contains(out.Content[0].Text, `"truncated":true`) {
		t.Fatalf("read = %+v, %v", out, err)
	}
	if _, err := registry.Run(Call{Name: "write_text"}, json.RawMessage(`{"key":"k","extra":1}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
	var schema map[string]any
	_ = json.Unmarshal(registry["write_text"].Spec.InputSchema, &schema)
	if schema["required"] == nil || schema["additionalProperties"] != false {
		t.Fatalf("schema = %v", schema)
	}
}
