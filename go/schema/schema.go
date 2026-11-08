// Package schema derives JSON Schemas from Go types and decodes JSON strictly at trust boundaries.
//
// Go's static types replace runtime validation inside the process. Values crossing a boundary (HTTP bodies, tool inputs, model output) are decoded with Decode, which rejects unknown fields and calls a Validate method when the type defines one.
package schema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

// Validator is implemented by types with constraints beyond their Go shape.
type Validator interface {
	Validate() error
}

// Describer is implemented by types that supply their own JSON Schema, such as tagged unions.
type Describer interface {
	JSONSchema() map[string]any
}

// Decode parses data into a T, rejecting unknown fields and trailing data, then applies Validate.
func Decode[T any](data []byte) (T, error) {
	var value T
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return value, fmt.Errorf("invalid %s: %w", typeName[T](), err)
	}
	if decoder.More() {
		return value, fmt.Errorf("invalid %s: trailing data", typeName[T]())
	}
	if err := validate(&value); err != nil {
		return value, fmt.Errorf("invalid %s: %w", typeName[T](), err)
	}
	return value, nil
}

func validate(value any) error {
	if v, ok := value.(Validator); ok {
		return v.Validate()
	}
	if v, ok := reflect.ValueOf(value).Elem().Interface().(Validator); ok {
		return v.Validate()
	}
	return nil
}

func typeName[T any]() string {
	t := reflect.TypeOf((*T)(nil)).Elem()
	if t.Name() == "" {
		return "input"
	}
	return t.Name()
}

// Of returns the JSON Schema describing T's JSON encoding.
func Of[T any]() json.RawMessage {
	raw, err := json.Marshal(For(reflect.TypeOf((*T)(nil)).Elem()))
	if err != nil {
		panic(err)
	}
	return raw
}

// For returns the JSON Schema describing a type's JSON encoding.
func For(t reflect.Type) map[string]any {
	return build(t, map[reflect.Type]bool{})
}

var (
	rawMessage = reflect.TypeOf(json.RawMessage{})
	describer  = reflect.TypeOf((*Describer)(nil)).Elem()
)

func build(t reflect.Type, visiting map[reflect.Type]bool) map[string]any {
	if t.Implements(describer) {
		return reflect.Zero(t).Interface().(Describer).JSONSchema()
	}
	if t == rawMessage {
		return map[string]any{}
	}
	switch t.Kind() {
	case reflect.Pointer:
		return build(t.Elem(), visiting)
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Interface:
		return map[string]any{}
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return map[string]any{"type": "string", "contentEncoding": "base64"}
		}
		return map[string]any{"type": "array", "items": build(t.Elem(), visiting)}
	case reflect.Map:
		return map[string]any{"type": "object", "additionalProperties": build(t.Elem(), visiting)}
	case reflect.Struct:
		if visiting[t] {
			return map[string]any{}
		}
		visiting[t] = true
		defer delete(visiting, t)
		properties := map[string]any{}
		required := []string{}
		fields(t, visiting, properties, &required)
		sort.Strings(required)
		object := map[string]any{"type": "object", "properties": properties, "additionalProperties": false}
		if len(required) > 0 {
			object["required"] = required
		}
		return object
	}
	return map[string]any{}
}

func fields(t reflect.Type, visiting map[reflect.Type]bool, properties map[string]any, required *[]string) {
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		tag := field.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		if field.Anonymous && name == "" && field.Type.Kind() == reflect.Struct {
			fields(field.Type, visiting, properties, required)
			continue
		}
		if name == "" {
			name = field.Name
		}
		property := build(field.Type, visiting)
		if description := field.Tag.Get("desc"); description != "" {
			property = withKey(property, "description", description)
		}
		if values := field.Tag.Get("enum"); values != "" {
			property = withKey(property, "enum", strings.Split(values, ","))
		}
		properties[name] = property
		if !strings.Contains(options, "omitempty") && !strings.Contains(options, "omitzero") && field.Type.Kind() != reflect.Pointer {
			*required = append(*required, name)
		}
	}
}

func withKey(schema map[string]any, key string, value any) map[string]any {
	copied := make(map[string]any, len(schema)+1)
	for k, v := range schema {
		copied[k] = v
	}
	copied[key] = value
	return copied
}
