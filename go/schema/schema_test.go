package schema

import (
	"encoding/json"
	"errors"
	"testing"
)

type sample struct {
	Path   string   `json:"path" desc:"File path"`
	Limit  *int     `json:"limit,omitempty"`
	Tags   []string `json:"tags,omitempty"`
	Mode   string   `json:"mode" enum:"read,write"`
	hidden int
}

func (s sample) Validate() error {
	if s.Path == "" {
		return errors.New("path is required")
	}
	return nil
}

func TestOfDescribesStruct(t *testing.T) {
	var got map[string]any
	if err := json.Unmarshal(Of[sample](), &got); err != nil {
		t.Fatal(err)
	}
	properties := got["properties"].(map[string]any)
	if properties["path"].(map[string]any)["description"] != "File path" {
		t.Fatalf("missing description: %v", properties["path"])
	}
	required := got["required"].([]any)
	if len(required) != 2 || required[0] != "mode" || required[1] != "path" {
		t.Fatalf("required = %v", required)
	}
	if got["additionalProperties"] != false {
		t.Fatal("objects must be closed")
	}
}

func TestDecodeRejectsUnknownFieldsAndRunsValidate(t *testing.T) {
	if _, err := Decode[sample]([]byte(`{"path":"a","mode":"read","extra":1}`)); err == nil {
		t.Fatal("unknown field accepted")
	}
	if _, err := Decode[sample]([]byte(`{"path":"","mode":"read"}`)); err == nil {
		t.Fatal("Validate not applied")
	}
	value, err := Decode[sample]([]byte(`{"path":"a","mode":"read"}`))
	if err != nil || value.Path != "a" {
		t.Fatalf("decode = %v, %v", value, err)
	}
}
