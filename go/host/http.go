package host

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"

	"github.com/rdeusser/tardigrade/go/api"
	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/schema"
)

// DefaultBodyBytes bounds request bodies the handler reads.
const DefaultBodyBytes = 8 << 20

// HTTPOptions configure the HTTP handler.
type HTTPOptions struct {
	// Token, when set, requires "Authorization: Bearer <Token>" on every route except /healthz.
	Token string
	// BodyBytes bounds request bodies; zero uses DefaultBodyBytes.
	BodyBytes int64
	// Sender identifies HTTP callers in message metadata; the zero value uses {external, http}.
	Sender core.Sender
}

// Handler serves the method API: thread allocation, method calls and their state, cancellation, the journal, and SSE streams of records and execution updates.
func (h *Host) Handler(opts HTTPOptions) http.Handler {
	if opts.BodyBytes == 0 {
		opts.BodyBytes = DefaultBodyBytes
	}
	if opts.Sender == (core.Sender{}) {
		opts.Sender = core.Sender{Kind: "external", ID: "http"}
	}
	mux := http.NewServeMux()
	route := func(pattern string, handle func(w http.ResponseWriter, r *http.Request) error) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			if opts.Token != "" && r.Header.Get("Authorization") != "Bearer "+opts.Token {
				writeError(w, &api.Error{Status: http.StatusUnauthorized, Code: "unauthorized", Message: "Unauthorized"})
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, opts.BodyBytes)
			if err := handle(w, r); err != nil {
				writeError(w, err)
			}
		})
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	route("GET /v1/metadata", func(w http.ResponseWriter, _ *http.Request) error {
		return writeJSON(w, http.StatusOK, api.Metadata{Name: h.opts.Actor.Name(), API: "atoms"})
	})
	route("GET /v1/methods", func(w http.ResponseWriter, _ *http.Request) error {
		return writeJSON(w, http.StatusOK, h.opts.Actor.Methods())
	})
	route("GET /v1/actors/{instance}/threads", func(w http.ResponseWriter, r *http.Request) error {
		threads, err := h.Threads(r.PathValue("instance"))
		if err != nil {
			return err
		}
		if threads == nil {
			threads = []core.ThreadInfo{}
		}
		return writeJSON(w, http.StatusOK, threads)
	})
	route("POST /v1/actors/{instance}/threads", func(w http.ResponseWriter, r *http.Request) error {
		input, err := decodeBody[struct {
			Name   string `json:"name,omitempty"`
			Parent string `json:"parent,omitempty"`
		}](r)
		if err != nil {
			return err
		}
		address, err := h.CreateThread(r.PathValue("instance"), input.Name, input.Parent)
		if err != nil {
			return err
		}
		return writeJSON(w, http.StatusOK, address)
	})
	thread := func(r *http.Request) (*core.Runtime, error) {
		return h.Thread(r.PathValue("instance"), r.PathValue("thread"))
	}
	route("POST /v1/actors/{instance}/threads/{thread}/methods/{method}", func(w http.ResponseWriter, r *http.Request) error {
		id := r.Header.Get("Idempotency-Key")
		if id == "" {
			return &api.Error{Status: http.StatusBadRequest, Code: "missing_idempotency_key", Message: "Idempotency-Key is required"}
		}
		rt, err := thread(r)
		if err != nil {
			return err
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return &api.Error{Status: http.StatusBadRequest, Code: "invalid_request", Message: err.Error()}
		}
		method := r.PathValue("method")
		if err := rt.Invoke(method, id, opts.Sender, body); err != nil {
			return err
		}
		w.Header().Set("Location", r.URL.Path+"/calls/"+id)
		return writeJSON(w, http.StatusAccepted, map[string]string{"actor": r.PathValue("instance"), "thread": r.PathValue("thread"), "method": method, "id": id})
	})
	route("GET /v1/actors/{instance}/threads/{thread}/methods/{method}/calls/{call}", func(w http.ResponseWriter, r *http.Request) error {
		rt, err := thread(r)
		if err != nil {
			return err
		}
		state, err := rt.Call(r.PathValue("method"), r.PathValue("call"))
		if err != nil {
			return err
		}
		return writeJSON(w, http.StatusOK, api.CallState{ID: r.PathValue("call"), Method: r.PathValue("method"), Status: state.Status, Output: state.Output, Error: state.Error, Reason: state.Reason})
	})
	route("PUT /v1/actors/{instance}/threads/{thread}/methods/{method}/calls/{call}/cancellation", func(w http.ResponseWriter, r *http.Request) error {
		input, err := decodeBody[struct {
			Reason string `json:"reason"`
		}](r)
		if err != nil {
			return err
		}
		rt, err := thread(r)
		if err != nil {
			return err
		}
		if err := rt.CancelCall(r.PathValue("method"), r.PathValue("call"), input.Reason, opts.Sender); err != nil {
			return err
		}
		return writeJSON(w, http.StatusAccepted, map[string]string{"actor": r.PathValue("instance"), "thread": r.PathValue("thread"), "method": r.PathValue("method"), "call": r.PathValue("call"), "status": "requested"})
	})
	route("GET /v1/actors/{instance}/threads/{thread}/events", func(w http.ResponseWriter, r *http.Request) error {
		rt, err := thread(r)
		if err != nil {
			return err
		}
		after, err := cursor(r.URL.Query().Get("after"))
		if err != nil {
			return err
		}
		rows, err := rows(rt, after)
		if err != nil {
			return err
		}
		return writeJSON(w, http.StatusOK, rows)
	})
	route("GET /v1/actors/{instance}/threads/{thread}/events/stream", func(w http.ResponseWriter, r *http.Request) error {
		rt, err := thread(r)
		if err != nil {
			return err
		}
		after, err := cursor(r.Header.Get("Last-Event-ID"))
		if err != nil {
			return err
		}
		if query := r.URL.Query().Get("after"); query != "" {
			if after, err = cursor(query); err != nil {
				return err
			}
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			return errors.New("streaming is unsupported")
		}
		startStream(w)
		for {
			changed := rt.Changed()
			batch, err := rows(rt, after)
			if err != nil {
				return nil
			}
			for _, row := range batch {
				data, _ := json.Marshal(row)
				fmt.Fprintf(w, "id: %d\nevent: record\ndata: %s\n\n", row.Seq, data)
				after = row.Seq
			}
			flusher.Flush()
			select {
			case <-changed:
			case <-r.Context().Done():
				return nil
			}
		}
	})
	route("GET /v1/actors/{instance}/threads/{thread}/execution/stream", func(w http.ResponseWriter, r *http.Request) error {
		rt, err := thread(r)
		if err != nil {
			return err
		}
		flusher, ok := w.(http.Flusher)
		if !ok {
			return errors.New("streaming is unsupported")
		}
		updates, stop := h.Subscribe(rt.Address())
		defer stop()
		startStream(w)
		flusher.Flush()
		for {
			select {
			case update := <-updates:
				var payload struct {
					Type string `json:"type"`
				}
				_ = json.Unmarshal(update.Payload, &payload)
				if payload.Type == "" {
					payload.Type = "update"
				}
				data, _ := json.Marshal(update)
				fmt.Fprintf(w, "event: %s\ndata: %s\n\n", payload.Type, data)
				flusher.Flush()
			case <-r.Context().Done():
				return nil
			}
		}
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, &api.Error{Status: http.StatusNotFound, Code: "route_not_found", Message: "Not found"})
	})
	return mux
}

func rows(rt *core.Runtime, after int64) ([]api.Row, error) {
	records, err := rt.Records(after, rt.Length())
	if err != nil {
		return nil, err
	}
	out := make([]api.Row, 0, len(records))
	for i, record := range records {
		event, err := core.EncodeEvent(record.Event)
		if err != nil {
			return nil, err
		}
		out = append(out, api.Row{Seq: after + int64(i) + 1, Event: event, RecordedAt: record.RecordedAt})
	}
	return out, nil
}

func cursor(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < 0 {
		return 0, &api.Error{Status: http.StatusBadRequest, Code: "invalid_cursor", Message: "cursor must be a nonnegative integer"}
	}
	return n, nil
}

func startStream(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
}

func decodeBody[T any](r *http.Request) (T, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var zero T
		return zero, &api.Error{Status: http.StatusBadRequest, Code: "invalid_request", Message: err.Error()}
	}
	if len(body) == 0 {
		body = []byte("{}")
	}
	value, err := schema.Decode[T](body)
	if err != nil {
		return value, &api.Error{Status: http.StatusBadRequest, Code: "invalid_request", Message: err.Error()}
	}
	return value, nil
}

func writeJSON(w http.ResponseWriter, status int, value any) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	return json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, err error) {
	var declared *api.Error
	var invalid *core.InvalidInputError
	var stopped *core.StoppedError
	switch {
	case errors.As(err, &declared):
	case errors.As(err, &invalid):
		declared = &api.Error{Status: http.StatusBadRequest, Code: "invalid_request", Message: invalid.Error()}
	case errors.As(err, &stopped):
		declared = &api.Error{Status: http.StatusConflict, Code: "actor_stopped", Message: stopped.Error()}
	case errors.Is(err, ErrUnknownThread):
		declared = &api.Error{Status: http.StatusNotFound, Code: "thread_not_found", Message: err.Error()}
	case errors.Is(err, core.ErrUnknownMethod):
		declared = &api.Error{Status: http.StatusNotFound, Code: "method_not_found", Message: err.Error()}
	case errors.Is(err, core.ErrUnknownCall):
		declared = &api.Error{Status: http.StatusNotFound, Code: "call_not_found", Message: err.Error()}
	case errors.Is(err, core.ErrNotCancellable):
		declared = &api.Error{Status: http.StatusBadRequest, Code: "cancellation_unsupported", Message: err.Error()}
	case errors.Is(err, core.ErrMessageConflict):
		declared = &api.Error{Status: http.StatusConflict, Code: "message_conflict", Message: err.Error()}
	default:
		log.Printf("tardigrade: request failed: %v", err)
		declared = &api.Error{Status: http.StatusInternalServerError, Code: "internal", Message: "Internal error; inspect the host logs"}
	}
	_ = writeJSON(w, declared.Status, declared)
}
