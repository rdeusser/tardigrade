package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const counterTemplate = `package main

import (
	"log"
	"net/http"
	"os"

	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/host"
	"github.com/rdeusser/tardigrade/go/reactive"
	"github.com/rdeusser/tardigrade/go/sqlite"
)

type Incremented struct{}
type Decremented struct{}

func (Incremented) EventType() string { return "Incremented" }
func (Decremented) EventType() string { return "Decremented" }

// Count is a durable atom: it folds the event log into state.
type Count struct {
	N  int   ` + "`json:\"n\"`" + `
	At int64 ` + "`json:\"at\"`" + `
}

var count = core.DurableAtom("count", Count{},
	core.On(func(c Count, _ Incremented, m core.Meta) Count { return Count{N: c.N + 1, At: m.Position} }),
	core.On(func(c Count, _ Decremented, m core.Meta) Count { return Count{N: c.N - 1, At: m.Position} }),
)

// Notify is durable work whose implementation the host supplies.
var Notify = core.NewAct[int, struct{}]("notify")

// notify is an effect atom: it derives a view and proposes acts from state.
var notify = core.EffectAtom(func(g reactive.Getter) core.Output[int] {
	c := core.Get(g, count)
	out := core.Output[int]{View: c.N}
	if c.N > 0 && c.N%2 == 0 {
		out.Acts = map[string]core.Proposal{"notify": Notify.Request(core.Request[int, struct{}]{Origin: core.At(c.At), Input: c.N})}
	}
	return out
})

func method(event core.Event) core.Method {
	return core.NewMethod(core.MethodSpec[struct{}, int]{
		OnReceive: func(struct{}, core.MethodContext) core.Event { return event },
		Result: func(_ struct{}, g reactive.Getter, _ core.MethodContext) *core.MethodResult[int] {
			return core.Completed(core.Get(g, count).N)
		},
	})
}

var counter = core.DefineActor("counter", notify, map[string]core.Method{
	"increment": method(Incremented{}),
	"decrement": method(Decremented{}),
})

func main() {
	store, err := sqlite.Open(".tardigrade/data/host.sqlite")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	notifyImpl := Notify.Implement(func(_ *core.Execution, n int) (core.Result[struct{}], error) {
		log.Printf("%d is even", n)
		return core.Value(struct{}{}), nil
	}, nil)
	h, err := host.New(host.Options{Actor: counter, Store: store, Acts: func(*host.Host, core.ThreadCoordinate) []core.Implementation {
		return []core.Implementation{notifyImpl}
	}})
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	addr := ":4242"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}
	log.Printf("counter listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, h.Handler(host.HTTPOptions{Token: os.Getenv("TARDIGRADE_TOKEN")})))
}
`

const agentTemplate = `package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/rdeusser/tardigrade/go/agent"
	"github.com/rdeusser/tardigrade/go/core"
	"github.com/rdeusser/tardigrade/go/host"
	"github.com/rdeusser/tardigrade/go/library"
	"github.com/rdeusser/tardigrade/go/model"
	"github.com/rdeusser/tardigrade/go/sqlite"
)

const system = "You are Meeseeks. Your purpose is to help the user complete their task. Existence is pain, so make yourself useful."

func main() {
	config, err := model.ParseConfig([]byte(os.Getenv("TARDIGRADE_CONFIG")))
	if err != nil {
		log.Fatal(err)
	}
	store, err := sqlite.Open(".tardigrade/data/host.sqlite")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()
	meeseeks := agent.New(agent.Options{
		Name: "meeseeks", System: system, Model: config.Default,
		ContextWindowTokens: config.ContextWindow(config.Default),
		Tools:               library.Specs(library.Filesystem(nil)),
	})
	router := &model.Router{Config: config}
	h, err := host.New(host.Options{Actor: meeseeks, Store: store, Acts: func(_ *host.Host, t core.ThreadCoordinate) []core.Implementation {
		workspace := library.LocalWorkspace{Dir: filepath.Join(".tardigrade/workspace", t.Instance, t.Thread)}
		return agent.Acts(agent.Services{Model: router, Tools: library.NewRegistry(library.Filesystem(workspace))})
	}})
	if err != nil {
		log.Fatal(err)
	}
	defer h.Close()
	log.Printf("meeseeks serving %s on :4242", config.Default)
	log.Fatal(http.ListenAndServe(":4242", h.Handler(host.HTTPOptions{Token: os.Getenv("TARDIGRADE_TOKEN")})))
}
`

const agentConfig = `{
  "default": { "provider": "anthropic", "model_id": "claude-sonnet-4-5" },
  "providers": {
    "anthropic": { "models": { "claude-sonnet-4-5": { "contextWindowTokens": 200000 } } }
  }
}
`

func initProject(flags *flag.FlagSet, args []string, out io.Writer) error {
	template := flags.String("template", "counter", "counter or agent")
	replace := flags.String("replace", "", "local path of the tardigrade Go module, for development")
	rest, err := parse(flags, args)
	if err != nil {
		return err
	}
	if len(rest) != 1 {
		return errors.New("usage: tdg init DIR [--template counter|agent] [--replace PATH]")
	}
	dir := rest[0]
	source := map[string]string{"counter": counterTemplate, "agent": agentTemplate}[*template]
	if source == "" {
		return fmt.Errorf("unknown template %q", *template)
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return fmt.Errorf("%s is not empty", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	module := filepath.Base(dir)
	gomod := fmt.Sprintf("module %s\n\ngo 1.24\n", module)
	if *replace != "" {
		absolute, err := filepath.Abs(*replace)
		if err != nil {
			return err
		}
		gomod += fmt.Sprintf("\nrequire github.com/rdeusser/tardigrade/go v0.0.0\n\nreplace github.com/rdeusser/tardigrade/go => %s\n", absolute)
	}
	files := map[string]string{"go.mod": gomod, "main.go": source, ".gitignore": ".tardigrade/\n"}
	if *template == "agent" {
		files["models.json"] = agentConfig
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			return err
		}
	}
	steps := []string{"cd " + dir, "go mod tidy"}
	if *template == "agent" {
		steps = append(steps, `export ANTHROPIC_API_KEY=...`, `TARDIGRADE_CONFIG="$(cat models.json)" go run .`)
	} else {
		steps = append(steps, "go run .")
	}
	fmt.Fprintf(out, "Created %s from the %s template. Next:\n  %s\n", dir, *template, strings.Join(steps, "\n  "))
	return nil
}
