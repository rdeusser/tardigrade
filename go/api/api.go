// Package api defines the JSON shapes of the HTTP method API shared by hosts and clients.
package api

import (
	"encoding/json"

	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// Row is one journal record as the events routes return it; Seq is one-based.
type Row struct {
	Seq        int64           `json:"seq"`
	Event      json.RawMessage `json:"event"`
	RecordedAt int64           `json:"recordedAt,omitempty"`
}

// Error is a declared request failure.
type Error struct {
	Status  int    `json:"-"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Message }

// Metadata names the mounted actor and the API generation.
type Metadata struct {
	Name string `json:"name"`
	API  string `json:"api"`
}

// CallState is a method call's state with its identity.
type CallState struct {
	ID     string                `json:"id"`
	Method string                `json:"method"`
	Status tardigrade.CallStatus `json:"status"`
	Output json.RawMessage       `json:"output,omitempty"`
	Error  string                `json:"error,omitempty"`
	Reason string                `json:"reason,omitempty"`
}
