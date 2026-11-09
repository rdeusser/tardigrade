// Command tdg serves the batteries-included agent and drives any Tardigrade host through the HostService API.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	tardigradev1 "github.com/rdeusser/tardigrade/go/api/tardigrade/v1"
	"github.com/rdeusser/tardigrade/go/api/tardigrade/v1/tardigradev1connect"
)

const usage = `tdg drives Tardigrade actors.

Usage:
  tdg serve [--addr :4242] [--data DIR] [--workspace DIR]   host the agent (TARDIGRADE_CONFIG)
  tdg chat [--thread NAME]                                  converse with a hosted agent
  tdg init DIR [--template counter|agent]                   write a Go project
  tdg actors                                                list actors, methods, and schemas
  tdg threads                                               list threads
  tdg thread create [NAME]                                  allocate a thread
  tdg call THREAD METHOD JSON [--id ID] [--wait]            call a method
  tdg state THREAD ID                                       read a call
  tdg cancel THREAD ID REASON                               cancel a call
  tdg events THREAD [--after N] [--follow]                  read the journal

Client commands read --url (TARDIGRADE_URL, default http://localhost:4242),
--token (TARDIGRADE_TOKEN), --actor (TARDIGRADE_ACTOR, default the host's only
actor), and --instance (default main).
`

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "tdg:", err)
		os.Exit(1)
	}
}

func env(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// bearer sends a bearer token with every request.
type bearer struct {
	token string
	next  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(r)
}

type remote struct {
	client   tardigradev1connect.HostServiceClient
	actor    string
	instance string
}

func dial(flags *flag.FlagSet) func() *remote {
	url := flags.String("url", env("TARDIGRADE_URL", "http://localhost:4242"), "host URL")
	token := flags.String("token", os.Getenv("TARDIGRADE_TOKEN"), "bearer token")
	actor := flags.String("actor", os.Getenv("TARDIGRADE_ACTOR"), "actor; the host's only actor when empty")
	instance := flags.String("instance", "main", "actor instance")
	return func() *remote {
		client := http.DefaultClient
		if *token != "" {
			client = &http.Client{Transport: bearer{*token, http.DefaultTransport}}
		}
		return &remote{client: tardigradev1connect.NewHostServiceClient(client, *url), actor: *actor, instance: *instance}
	}
}

// ref addresses a thread of the selected actor. Without --actor, the host must serve exactly one actor.
func (r *remote) ref(ctx context.Context, thread string) (*tardigradev1.ThreadRef, error) {
	if r.actor == "" {
		res, err := r.client.ListActors(ctx, connect.NewRequest(&tardigradev1.ListActorsRequest{}))
		if err != nil {
			return nil, err
		}
		if len(res.Msg.Actors) != 1 {
			return nil, fmt.Errorf("the host serves %d actors; choose one with --actor", len(res.Msg.Actors))
		}
		r.actor = res.Msg.Actors[0].Name
	}
	return &tardigradev1.ThreadRef{Actor: r.actor, Instance: r.instance, Thread: thread}, nil
}

// await follows a call until it finishes and returns its final state.
func (r *remote) await(ctx context.Context, thread *tardigradev1.ThreadRef, id string) (*tardigradev1.Call, error) {
	stream, err := r.client.WatchCall(ctx, connect.NewRequest(&tardigradev1.WatchCallRequest{Thread: thread, CallId: id}))
	if err != nil {
		return nil, err
	}
	defer stream.Close()
	var last *tardigradev1.Call
	for stream.Receive() {
		last = stream.Msg().Call
	}
	return last, stream.Err()
}

// parse accepts flags before and after positional arguments.
func parse(flags *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := flags.Parse(args); err != nil {
			return nil, err
		}
		args = flags.Args()
		if len(args) == 0 {
			return positional, nil
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
}

// compact renders a message as single-line JSON.
func compact(m proto.Message) []byte {
	data, _ := protojson.Marshal(m)
	var out bytes.Buffer
	_ = json.Compact(&out, data)
	return out.Bytes()
}

func printJSON(out io.Writer, m proto.Message) error {
	var indented bytes.Buffer
	if err := json.Indent(&indented, compact(m), "", "  "); err != nil {
		return err
	}
	_, err := fmt.Fprintln(out, indented.String())
	return err
}

func newID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func run(ctx context.Context, args []string, in io.Reader, out io.Writer) error {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(out, usage)
		return nil
	}
	command, args := args[0], args[1:]
	flags := flag.NewFlagSet("tdg "+command, flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	switch command {
	case "serve":
		return serve(ctx, flags, args, out)
	case "init":
		return initProject(flags, args, out)
	}
	remoteOf := dial(flags)
	switch command {
	case "actors":
		if _, err := parse(flags, args); err != nil {
			return err
		}
		res, err := remoteOf().client.ListActors(ctx, connect.NewRequest(&tardigradev1.ListActorsRequest{}))
		if err != nil {
			return err
		}
		return printJSON(out, res.Msg)
	case "threads":
		if _, err := parse(flags, args); err != nil {
			return err
		}
		r := remoteOf()
		res, err := r.client.ListThreads(ctx, connect.NewRequest(&tardigradev1.ListThreadsRequest{Actor: r.actor, Instance: r.instance}))
		if err != nil {
			return err
		}
		return printJSON(out, res.Msg)
	case "thread":
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		if len(rest) == 0 || rest[0] != "create" {
			return errors.New("usage: tdg thread create [NAME]")
		}
		name := ""
		if len(rest) > 1 {
			name = rest[1]
		}
		thread, err := remoteOf().create(ctx, name)
		if err != nil {
			return err
		}
		return printJSON(out, thread)
	case "call":
		id := flags.String("id", "", "call ID; generated when empty")
		wait := flags.Bool("wait", false, "wait for the call to finish")
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		input := &structpb.Value{}
		if len(rest) != 3 || input.UnmarshalJSON([]byte(rest[2])) != nil {
			return errors.New("usage: tdg call THREAD METHOD JSON [--id ID] [--wait]")
		}
		if *id == "" {
			*id = newID()
		}
		r := remoteOf()
		thread, err := r.ref(ctx, rest[0])
		if err != nil {
			return err
		}
		res, err := r.client.StartCall(ctx, connect.NewRequest(&tardigradev1.StartCallRequest{Thread: thread, Method: rest[1], CallId: *id, Input: input}))
		if err != nil {
			return err
		}
		call := res.Msg.Call
		if *wait {
			if call, err = r.await(ctx, thread, *id); err != nil {
				return err
			}
		}
		return printJSON(out, call)
	case "state", "cancel":
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		if command == "state" && len(rest) != 2 || command == "cancel" && len(rest) != 3 {
			return errors.New("usage: tdg state THREAD ID | tdg cancel THREAD ID REASON")
		}
		r := remoteOf()
		thread, err := r.ref(ctx, rest[0])
		if err != nil {
			return err
		}
		if command == "cancel" {
			res, err := r.client.CancelCall(ctx, connect.NewRequest(&tardigradev1.CancelCallRequest{Thread: thread, CallId: rest[1], Reason: rest[2]}))
			if err != nil {
				return err
			}
			return printJSON(out, res.Msg.Call)
		}
		res, err := r.client.GetCall(ctx, connect.NewRequest(&tardigradev1.GetCallRequest{Thread: thread, CallId: rest[1]}))
		if err != nil {
			return err
		}
		return printJSON(out, res.Msg.Call)
	case "events":
		after := flags.Int64("after", 0, "one-based journal position")
		follow := flags.Bool("follow", false, "stream new records")
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		if len(rest) != 1 {
			return errors.New("usage: tdg events THREAD [--after N] [--follow]")
		}
		r := remoteOf()
		thread, err := r.ref(ctx, rest[0])
		if err != nil {
			return err
		}
		if *follow {
			stream, err := r.client.WatchRecords(ctx, connect.NewRequest(&tardigradev1.WatchRecordsRequest{Thread: thread, After: *after}))
			if err != nil {
				return err
			}
			defer stream.Close()
			for stream.Receive() {
				fmt.Fprintf(out, "%d\t%s\n", stream.Msg().Record.Seq, compact(stream.Msg().Record.Event))
			}
			if err := stream.Err(); err != nil && ctx.Err() == nil {
				return err
			}
			return nil
		}
		res, err := r.client.ListRecords(ctx, connect.NewRequest(&tardigradev1.ListRecordsRequest{Thread: thread, After: *after}))
		if err != nil {
			return err
		}
		for _, record := range res.Msg.Records {
			fmt.Fprintf(out, "%d\t%s\n", record.Seq, compact(record.Event))
		}
		return nil
	case "chat":
		name := flags.String("thread", "", "thread to continue; a new thread when empty")
		if _, err := parse(flags, args); err != nil {
			return err
		}
		return chat(ctx, remoteOf(), *name, in, out)
	}
	return fmt.Errorf("unknown command %q\n\n%s", command, usage)
}

// create allocates a thread of the selected actor.
func (r *remote) create(ctx context.Context, name string) (*tardigradev1.Thread, error) {
	ref, err := r.ref(ctx, name)
	if err != nil {
		return nil, err
	}
	res, err := r.client.CreateThread(ctx, connect.NewRequest(&tardigradev1.CreateThreadRequest{Actor: ref.Actor, Instance: ref.Instance, Name: name}))
	if err != nil {
		return nil, err
	}
	return res.Msg.Thread, nil
}

func chat(ctx context.Context, r *remote, name string, in io.Reader, out io.Writer) error {
	created, err := r.create(ctx, name)
	if err != nil {
		return err
	}
	thread := created.Ref
	fmt.Fprintf(out, "thread %s (Ctrl-D to exit)\n", thread.Thread)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	streamed := make(chan bool, 1)
	updates, err := r.client.WatchUpdates(streamCtx, connect.NewRequest(&tardigradev1.WatchUpdatesRequest{Thread: thread}))
	if err != nil {
		return err
	}
	defer updates.Close()
	go func() {
		for updates.Receive() {
			delta := updates.Msg().Update.Payload.GetStructValue().GetFields()
			if delta["type"].GetStringValue() == "model.delta" && delta["kind"].GetStringValue() == "text" {
				fmt.Fprint(out, delta["text"].GetStringValue())
				select {
				case streamed <- true:
				default:
				}
			}
		}
	}()
	lines := bufio.NewScanner(in)
	for {
		fmt.Fprint(out, "> ")
		if !lines.Scan() {
			fmt.Fprintln(out)
			return lines.Err()
		}
		text := strings.TrimSpace(lines.Text())
		if text == "" {
			continue
		}
		select {
		case <-streamed:
		default:
		}
		id := newID()
		input, _ := structpb.NewValue(map[string]any{"text": text})
		if _, err := r.client.StartCall(ctx, connect.NewRequest(&tardigradev1.StartCallRequest{Thread: thread, Method: "message", CallId: id, Input: input})); err != nil {
			return err
		}
		call, err := r.await(ctx, thread, id)
		if err != nil {
			return err
		}
		wasStreamed := false
		select {
		case wasStreamed = <-streamed:
		default:
		}
		switch call.Status {
		case tardigradev1.CallStatus_CALL_STATUS_COMPLETED:
			if !wasStreamed {
				fmt.Fprint(out, call.GetOutput().GetStructValue().GetFields()["text"].GetStringValue())
			}
			fmt.Fprintln(out)
		case tardigradev1.CallStatus_CALL_STATUS_FAILED:
			fmt.Fprintf(out, "\n[failed] %s\n", call.GetError())
		default:
			fmt.Fprintf(out, "\n[cancelled] %s\n", call.GetCancelReason())
		}
	}
}
