// Command tdg serves the batteries-included agent and drives any Tardigrade host over HTTP.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/rdeusser/tardigrade/go/api"
	"github.com/rdeusser/tardigrade/go/client"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

const usage = `tdg drives Tardigrade actors.

Usage:
  tdg serve [--addr :4242] [--data DIR] [--workspace DIR]   host the agent (TARDIGRADE_CONFIG)
  tdg chat [--thread NAME]                                  converse with a hosted agent
  tdg init DIR [--template counter|agent]                   write a Go project
  tdg methods                                               list methods and schemas
  tdg threads                                               list threads
  tdg thread create [NAME] [--parent THREAD]                allocate a thread
  tdg call THREAD METHOD JSON [--id ID] [--wait]            call a method
  tdg state THREAD METHOD ID                                read a call
  tdg cancel THREAD METHOD ID REASON                        cancel a call
  tdg events THREAD [--after N] [--follow]                  read the journal
  tdg models [--provider ID]                                list catalog context windows

Client commands read --url (TARDIGRADE_URL, default http://localhost:4242),
--token (TARDIGRADE_TOKEN), and --instance (default main).
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

type remote struct {
	client   *client.Client
	instance string
}

func connect(flags *flag.FlagSet) func() remote {
	url := flags.String("url", env("TARDIGRADE_URL", "http://localhost:4242"), "host URL")
	token := flags.String("token", os.Getenv("TARDIGRADE_TOKEN"), "bearer token")
	instance := flags.String("instance", "main", "actor instance")
	return func() remote { return remote{client: client.New(*url, *token), instance: *instance} }
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

func (r remote) thread(name string) tardigrade.ThreadCoordinate {
	return tardigrade.ThreadCoordinate{Instance: r.instance, Thread: name}
}

func printJSON(out io.Writer, value any) error {
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
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
	case "models":
		provider := flags.String("provider", "", "provider ID")
		url := flags.String("catalog", env("TARDIGRADE_MODEL_CATALOG_URL", model.DefaultCatalogURL), "catalog URL")
		if _, err := parse(flags, args); err != nil {
			return err
		}
		catalog, err := model.FetchCatalog(ctx, nil, *url)
		if err != nil {
			return err
		}
		for id, p := range catalog {
			if *provider != "" && id != *provider {
				continue
			}
			for modelID, m := range p.Models {
				fmt.Fprintf(out, "%s\t%s\t%d\n", id, modelID, m.Limit.Context)
			}
		}
		return nil
	}
	remoteOf := connect(flags)
	switch command {
	case "methods":
		if _, err := parse(flags, args); err != nil {
			return err
		}
		methods, err := remoteOf().client.Methods(ctx)
		if err != nil {
			return err
		}
		return printJSON(out, methods)
	case "threads":
		if _, err := parse(flags, args); err != nil {
			return err
		}
		r := remoteOf()
		threads, err := r.client.Threads(ctx, r.instance)
		if err != nil {
			return err
		}
		return printJSON(out, threads)
	case "thread":
		parent := flags.String("parent", "", "parent thread")
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		if len(rest) == 0 || rest[0] != "create" {
			return errors.New("usage: tdg thread create [NAME] [--parent THREAD]")
		}
		name := ""
		if len(rest) > 1 {
			name = rest[1]
		}
		r := remoteOf()
		thread, err := r.client.CreateThread(ctx, r.instance, name, *parent)
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
		if len(rest) != 3 || !json.Valid([]byte(rest[2])) {
			return errors.New("usage: tdg call THREAD METHOD JSON [--id ID] [--wait]")
		}
		if *id == "" {
			*id = newID()
		}
		r := remoteOf()
		thread := r.thread(rest[0])
		if err := r.client.Invoke(ctx, thread, rest[1], *id, json.RawMessage(rest[2])); err != nil {
			return err
		}
		if !*wait {
			return printJSON(out, map[string]any{"id": *id, "status": tardigrade.StatusPending})
		}
		state, err := r.client.Await(ctx, thread, rest[1], *id, 0)
		if err != nil {
			return err
		}
		return printJSON(out, state)
	case "state":
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		if len(rest) != 3 {
			return errors.New("usage: tdg state THREAD METHOD ID")
		}
		r := remoteOf()
		state, err := r.client.Call(ctx, r.thread(rest[0]), rest[1], rest[2])
		if err != nil {
			return err
		}
		return printJSON(out, state)
	case "cancel":
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		if len(rest) != 4 {
			return errors.New("usage: tdg cancel THREAD METHOD ID REASON")
		}
		r := remoteOf()
		return r.client.CancelCall(ctx, r.thread(rest[0]), rest[1], rest[2], rest[3])
	case "events":
		after := flags.Int64("after", 0, "one-based cursor")
		follow := flags.Bool("follow", false, "stream new records")
		rest, err := parse(flags, args)
		if err != nil {
			return err
		}
		if len(rest) != 1 {
			return errors.New("usage: tdg events THREAD [--after N] [--follow]")
		}
		r := remoteOf()
		thread := r.thread(rest[0])
		if *follow {
			return r.client.Follow(ctx, thread, *after, func(row api.Row) error {
				fmt.Fprintf(out, "%d\t%s\n", row.Seq, row.Event)
				return nil
			})
		}
		rows, err := r.client.Events(ctx, thread, *after)
		if err != nil {
			return err
		}
		for _, row := range rows {
			fmt.Fprintf(out, "%d\t%s\n", row.Seq, row.Event)
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

func chat(ctx context.Context, r remote, name string, in io.Reader, out io.Writer) error {
	thread, err := r.client.CreateThread(ctx, r.instance, name, "")
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "thread %s (Ctrl-D to exit)\n", thread.Thread)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	streamed := make(chan bool, 1)
	go func() {
		_ = r.client.Updates(streamCtx, thread, func(u tardigrade.Update) error {
			var delta struct {
				Type string `json:"type"`
				Kind string `json:"kind"`
				Text string `json:"text"`
			}
			if json.Unmarshal(u.Payload, &delta) == nil && delta.Type == "model.delta" && delta.Kind == "text" {
				fmt.Fprint(out, delta.Text)
				select {
				case streamed <- true:
				default:
				}
			}
			return nil
		})
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
		if err := r.client.Invoke(ctx, thread, "message", id, map[string]string{"text": text}); err != nil {
			return err
		}
		state, err := r.client.Await(ctx, thread, "message", id, 100*time.Millisecond)
		if err != nil {
			return err
		}
		wasStreamed := false
		select {
		case wasStreamed = <-streamed:
		default:
		}
		switch state.Status {
		case tardigrade.StatusCompleted:
			var reply struct {
				Text string `json:"text"`
			}
			_ = json.Unmarshal(state.Output, &reply)
			if !wasStreamed {
				fmt.Fprint(out, reply.Text)
			}
			fmt.Fprintln(out)
		case tardigrade.StatusFailed:
			fmt.Fprintf(out, "\n[failed] %s\n", state.Error)
		default:
			fmt.Fprintf(out, "\n[%s] %s\n", state.Status, state.Reason)
		}
	}
}
