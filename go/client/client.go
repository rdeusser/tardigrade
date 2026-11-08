// Package client calls a hosted actor's HTTP method API.
package client

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/rdeusser/tardigrade/go/api"
	"github.com/rdeusser/tardigrade/go/core"
)

// DefaultPollInterval spaces Await's state reads.
const DefaultPollInterval = 250 * time.Millisecond

// MaxEventBytes bounds one line of a host's event stream.
const MaxEventBytes = 16 << 20

// Client calls one host. The zero HTTP uses http.DefaultClient.
type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
}

// New returns a client for the host at baseURL.
func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token}
}

func (c *Client) http() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

func (c *Client) do(ctx context.Context, method, path string, body any, header http.Header, out any) error {
	var reader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reader = bytes.NewReader(data)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, reader)
	if err != nil {
		return err
	}
	for k, v := range header {
		request.Header[k] = v
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	response, err := c.http().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode >= 300 {
		failure := &api.Error{Status: response.StatusCode}
		if json.Unmarshal(data, failure) != nil || failure.Message == "" {
			failure.Message = fmt.Sprintf("%s %s: %s", method, path, response.Status)
		}
		return failure
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func threadPath(t core.ThreadCoordinate) string {
	return "/v1/actors/" + url.PathEscape(t.Instance) + "/threads/" + url.PathEscape(t.Thread)
}

// Metadata reads the mounted actor's name.
func (c *Client) Metadata(ctx context.Context) (api.Metadata, error) {
	var out api.Metadata
	return out, c.do(ctx, http.MethodGet, "/v1/metadata", nil, nil, &out)
}

// Methods lists the actor's methods with their schemas.
func (c *Client) Methods(ctx context.Context) ([]core.MethodInfo, error) {
	var out []core.MethodInfo
	return out, c.do(ctx, http.MethodGet, "/v1/methods", nil, nil, &out)
}

// CreateThread allocates a thread; an empty name is generated and a nonempty parent creates a child.
func (c *Client) CreateThread(ctx context.Context, instance, name, parent string) (core.ThreadCoordinate, error) {
	var out core.ThreadCoordinate
	body := map[string]string{}
	if name != "" {
		body["name"] = name
	}
	if parent != "" {
		body["parent"] = parent
	}
	return out, c.do(ctx, http.MethodPost, "/v1/actors/"+url.PathEscape(instance)+"/threads", body, nil, &out)
}

// Threads lists an instance's threads.
func (c *Client) Threads(ctx context.Context, instance string) ([]core.ThreadInfo, error) {
	var out []core.ThreadInfo
	return out, c.do(ctx, http.MethodGet, "/v1/actors/"+url.PathEscape(instance)+"/threads", nil, nil, &out)
}

// Invoke calls a method under a caller-chosen ID; repeating the same call is absorbed.
func (c *Client) Invoke(ctx context.Context, thread core.ThreadCoordinate, method, id string, input any) error {
	header := http.Header{"Idempotency-Key": {id}}
	return c.do(ctx, http.MethodPost, threadPath(thread)+"/methods/"+url.PathEscape(method), input, header, nil)
}

// Call reads a method call's state.
func (c *Client) Call(ctx context.Context, thread core.ThreadCoordinate, method, id string) (api.CallState, error) {
	var out api.CallState
	return out, c.do(ctx, http.MethodGet, threadPath(thread)+"/methods/"+url.PathEscape(method)+"/calls/"+url.PathEscape(id), nil, nil, &out)
}

// Await polls a call every interval until it is no longer pending; a zero interval uses DefaultPollInterval.
func (c *Client) Await(ctx context.Context, thread core.ThreadCoordinate, method, id string, interval time.Duration) (api.CallState, error) {
	if interval == 0 {
		interval = DefaultPollInterval
	}
	for {
		state, err := c.Call(ctx, thread, method, id)
		if err != nil || state.Status != core.StatusPending {
			return state, err
		}
		select {
		case <-time.After(interval):
		case <-ctx.Done():
			return state, ctx.Err()
		}
	}
}

// CancelCall asks a cancellable call to stop.
func (c *Client) CancelCall(ctx context.Context, thread core.ThreadCoordinate, method, id, reason string) error {
	return c.do(ctx, http.MethodPut, threadPath(thread)+"/methods/"+url.PathEscape(method)+"/calls/"+url.PathEscape(id)+"/cancellation", map[string]string{"reason": reason}, nil, nil)
}

// Events reads journal rows after a one-based cursor.
func (c *Client) Events(ctx context.Context, thread core.ThreadCoordinate, after int64) ([]api.Row, error) {
	var out []api.Row
	return out, c.do(ctx, http.MethodGet, threadPath(thread)+"/events?after="+strconv.FormatInt(after, 10), nil, nil, &out)
}

// Follow streams journal rows after a cursor until ctx ends or visit returns an error.
func (c *Client) Follow(ctx context.Context, thread core.ThreadCoordinate, after int64, visit func(api.Row) error) error {
	return c.stream(ctx, threadPath(thread)+"/events/stream?after="+strconv.FormatInt(after, 10), func(data []byte) error {
		var row api.Row
		if err := json.Unmarshal(data, &row); err != nil {
			return err
		}
		return visit(row)
	})
}

// Updates streams transient execution updates produced after the connection opens.
func (c *Client) Updates(ctx context.Context, thread core.ThreadCoordinate, visit func(core.Update) error) error {
	return c.stream(ctx, threadPath(thread)+"/execution/stream", func(data []byte) error {
		var update core.Update
		if err := json.Unmarshal(data, &update); err != nil {
			return err
		}
		return visit(update)
	})
}

func (c *Client) stream(ctx context.Context, path string, visit func([]byte) error) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "text/event-stream")
	if c.Token != "" {
		request.Header.Set("Authorization", "Bearer "+c.Token)
	}
	response, err := c.http().Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode >= 300 {
		return &api.Error{Status: response.StatusCode, Message: "stream " + path + ": " + response.Status}
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, 64<<10), MaxEventBytes)
	for scanner.Scan() {
		line := scanner.Text()
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			if err := visit([]byte(data)); err != nil {
				return err
			}
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return scanner.Err()
}
