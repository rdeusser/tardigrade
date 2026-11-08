package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/rdeusser/tardigrade/go/agent"
	"github.com/rdeusser/tardigrade/go/host"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/sqlite"
	"github.com/rdeusser/tardigrade/go/tardigrade"
)

// DefaultSystem is the agent prompt tdg serve uses without --system.
const DefaultSystem = "You are a friendly assistant."

// loadConfig reads TARDIGRADE_CONFIG, or the file TARDIGRADE_CONFIG_PATH names, as {"models": ...} or a bare model configuration.
func loadConfig() (model.Config, error) {
	data := []byte(os.Getenv("TARDIGRADE_CONFIG"))
	if len(data) == 0 {
		path := os.Getenv("TARDIGRADE_CONFIG_PATH")
		if path == "" {
			return model.Config{}, errors.New("set TARDIGRADE_CONFIG or TARDIGRADE_CONFIG_PATH to a model configuration")
		}
		var err error
		if data, err = os.ReadFile(path); err != nil {
			return model.Config{}, err
		}
	}
	var wrapped struct {
		Models json.RawMessage `json:"models"`
	}
	if json.Unmarshal(data, &wrapped) == nil && len(wrapped.Models) > 0 {
		data = wrapped.Models
	}
	return model.ParseConfig(data)
}

// libraries returns the tools of the served agent; workspace and delegator may be nil when only specs are needed.
func libraries(workspace library.Workspace, delegator library.Delegator) []library.Library {
	return []library.Library{
		library.Filesystem(workspace),
		library.Fetch(nil, 0),
		library.Notes(0),
		library.Alarm(),
		library.Agents(delegator, func(message string) any { return agent.MessageInput{Text: message} }),
	}
}

func serve(ctx context.Context, flags *flag.FlagSet, args []string, out io.Writer) error {
	addr := flags.String("addr", ":"+env("PORT", "4242"), "listen address")
	data := flags.String("data", env("TARDIGRADE_ACTOR_DATA", ".tardigrade/data"), "storage directory")
	workspace := flags.String("workspace", env("TARDIGRADE_WORKSPACE", ".tardigrade/workspace"), "root of per-thread workspaces")
	name := flags.String("name", env("TARDIGRADE_ACTOR", "tardie"), "actor name")
	system := flags.String("system", env("TARDIGRADE_SYSTEM", DefaultSystem), "system prompt")
	if _, err := parse(flags, args); err != nil {
		return err
	}
	config, err := loadConfig()
	if err != nil {
		return err
	}
	window := config.ContextWindow(config.Default)
	if window == 0 {
		lookup, cancel := context.WithTimeout(ctx, 10*time.Second)
		catalog, err := model.FetchCatalog(lookup, nil, env("TARDIGRADE_MODEL_CATALOG_URL", model.DefaultCatalogURL))
		cancel()
		if err == nil {
			window = catalog.ContextWindow(config.Default)
		}
		if window == 0 {
			return fmt.Errorf("context window of %s is unknown; set providers.%s.models.%q.contextWindowTokens", config.Default, config.Default.Provider, config.Default.ModelID)
		}
	}
	store, err := sqlite.Open(filepath.Join(*data, "host.sqlite"))
	if err != nil {
		return err
	}
	defer store.Close()
	actor := agent.New(agent.Options{Name: *name, System: *system, Model: config.Default, ContextWindowTokens: window, Tools: library.Specs(libraries(nil, nil)...)})
	router := &model.Router{Config: config}
	h, err := host.New(host.Options{Actor: actor, Store: store, Handlers: func(h *host.Host, address tardigrade.ThreadCoordinate) []tardigrade.Handler {
		dir := library.LocalWorkspace{Dir: filepath.Join(*workspace, address.Instance, address.Thread)}
		return agent.Handlers(agent.Services{Model: router, Tools: library.NewRegistry(libraries(dir, h)...)})
	}})
	if err != nil {
		return err
	}
	defer h.Close()
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	server := &http.Server{Handler: h.Handler(host.HTTPOptions{Token: os.Getenv("TARDIGRADE_TOKEN")}), ReadHeaderTimeout: 10 * time.Second}
	fmt.Fprintf(out, "%s serving %s on http://%s\n", *name, config.Default, listener.Addr())
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
