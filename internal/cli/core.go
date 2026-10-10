package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/urmzd/legatus/pkg/controlplane"
	"github.com/urmzd/legatus/pkg/metrics"
	"github.com/urmzd/legatus/pkg/node/inproc"
	"github.com/urmzd/legatus/pkg/tool"
	"github.com/urmzd/legatus/pkg/workspace"
	"github.com/urmzd/duraturo"
	"github.com/urmzd/duraturo/pkg/ledger"
	dqueue "github.com/urmzd/duraturo/pkg/queue"
	drun "github.com/urmzd/duraturo/pkg/run"
	"github.com/urmzd/duraturo/pkg/worker"

	"github.com/urmzd/mandatum/internal/server"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/cron"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/registry"
	"github.com/urmzd/mandatum/pkg/router"
	"github.com/urmzd/mandatum/pkg/runtime"
	"github.com/urmzd/mandatum/pkg/topic"
)

// Environment variables the composition root reads. Everything that names an
// external system is an environment variable rather than a flag, because it is
// deployment configuration rather than a per-invocation choice, and because a
// credential in a flag ends up in shell history and in ps output.
const (
	// EnvRedis points the bus at Redis Streams. Unset keeps the in-memory
	// broker, which is a complete implementation of the same interface and
	// exactly as correct — for one process.
	EnvRedis = "MANDATUM_REDIS_URL"

	// EnvWorkspace is the directory agent artifacts are written under. Unset
	// takes a directory under the user's cache.
	EnvWorkspace = "MANDATUM_WORKSPACE"
)

// coreConfig is what a running mandatum needs that is not a command-line
// concern. It is filled from flags and the environment by the commands, and
// nothing in this file reads either: a composition root that reaches for
// os.Getenv cannot be constructed twice in one process, and a test is exactly
// the second time.
type coreConfig struct {
	// Tenant is the authorization scope this process serves. It is stamped
	// onto every tag a connector raises, mirroring what the transport does
	// with a credential: a trigger has no credential to derive a scope from,
	// so the deployment's own scope is the only honest answer.
	Tenant string

	// Redis is a redis:// URL. Empty selects the in-memory bus.
	Redis string

	// Workspace is where the sandbox writes agent artifacts.
	Workspace string

	// Schedules are the cron connector's standing requests. Empty registers no
	// cron connector at all, rather than one with nothing to do.
	Schedules []cron.Schedule

	// Concurrency is how many runs a worker executes at once. Zero means one.
	Concurrency int

	// Log receives operational output. Nil discards it.
	Log *slog.Logger

	// Metrics receives counters. Nil records nothing.
	Metrics metrics.Recorder
}

// core is mandatum assembled: every long-lived component this process owns, in
// the order data flows through them.
//
//	registry  →  runtime  →  bus  →  router  →  connector sinks
//	                ↑                              ↓
//	            duraturo worker              a surface somewhere
//
// Nothing here is a stub standing in for a deployment. The in-memory registry,
// bus, ledger and queue are complete implementations of the same interfaces the
// distributed backends implement; swapping one is a line in newCore, not a
// change to anything below it.
type core struct {
	cfg coreConfig
	log *slog.Logger

	store   *registry.Memory
	broker  bus.Bus
	sinks   *connector.Registry
	deliver *router.Router
	rt      *runtime.Runtime
	ledger  *ledger.Memory
	queue   *dqueue.Memory

	// webhooks are the connectors that serve their own HTTP, mounted under
	// /connectors/{name}. A trigger authenticates its own requests, so the
	// server never sees an unverified body.
	webhooks map[string]http.Handler

	closers []func() error
}

// newCore assembles the system. It is deliberately linear: read it top to
// bottom and you have read how mandatum fits together.
func newCore(cfg coreConfig) (*core, error) {
	log := cfg.Log
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	rec := cfg.Metrics
	if rec == nil {
		rec = metrics.Nop()
	}
	c := &core{cfg: cfg, log: log, webhooks: map[string]http.Handler{}}

	// The broker. Runs publish here; the router, every subscriber, and every
	// SSE client read from here. Nothing else connects them.
	broker, closeBus, err := newBus(cfg.Redis)
	if err != nil {
		return nil, err
	}
	c.broker = broker
	if closeBus != nil {
		c.closers = append(c.closers, closeBus)
	}

	// The control plane: append-only agent definitions, scoped by tenant.
	c.store = registry.NewMemory()

	// The mesh. Roles are derived from the interfaces each connector
	// implements, never declared, so registering is the whole wiring.
	sinks, err := buildConnectors(cfg, c.store)
	if err != nil {
		return nil, err
	}
	c.sinks = sinks
	for _, t := range sinks.Triggers() {
		if h, ok := t.(http.Handler); ok {
			c.webhooks[t.Name()] = h
		}
	}

	// The router is just another bus subscriber. That indirection is why
	// origin and destination stay independent: nothing on the path from
	// trigger to sink holds both ends at once.
	c.deliver = router.New(broker, sinks, router.WithLogger(log))

	// The agent side. A turn runs as a legatus task under the NGAC policy
	// compiled from the pinned revision's Access grant, so what an agent may
	// touch is enforced rather than documented.
	exec, err := newExecutor(cfg, c.store, sinks, log, rec)
	if err != nil {
		return nil, err
	}

	// Durable execution: the ledger is truth, the queue is disposable flow.
	c.ledger = ledger.NewMemory()
	c.queue = dqueue.NewMemory()

	rt, err := runtime.New(
		duraturo.New(c.ledger, c.queue),
		registry.NewSpecs(c.store),
		broker,
		exec,
		runtime.WithLogger(log),
	)
	if err != nil {
		return nil, fmt.Errorf("cli: runtime: %w", err)
	}
	c.rt = rt
	return c, nil
}

// close releases what the core owns. It is separate from the run loops because
// a Redis client outlives the context that stopped reading from it.
func (c *core) close() error {
	var err error
	for _, fn := range c.closers {
		err = errors.Join(err, fn())
	}
	return err
}

// newBus selects a broker. The in-memory one is the default because a single
// process needs nothing else and a default that requires infrastructure is a
// default nobody can run.
func newBus(redisURL string) (bus.Bus, func() error, error) {
	if strings.TrimSpace(redisURL) == "" {
		return bus.NewMemory(), nil, nil
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, nil, usagef("$%s is not a redis URL: %v", EnvRedis, err)
	}
	client := redis.NewClient(opts)
	return bus.NewRedis(client), client.Close, nil
}

// newExecutor builds the sandbox a turn runs in.
//
// The tool registry and the workspace are shared with the control plane rather
// than owned by the sandbox: legatus resolves a tool by name on the node that
// executes it, and the node is created from this factory. The policy that
// scopes what that tool can see comes from the pinned revision, one deployment
// per (agent, revision), which is what stops a revised Access grant from
// widening a run already in flight.
func newExecutor(cfg coreConfig, store registry.Store, sinks *connector.Registry, log *slog.Logger, rec metrics.Recorder) (runtime.Executor, error) {
	dir := strings.TrimSpace(cfg.Workspace)
	if dir == "" {
		base, err := os.UserCacheDir()
		if err != nil {
			base = os.TempDir()
		}
		dir = filepath.Join(base, "mandatum", "workspace")
	}
	ws, err := workspace.NewLocal(dir)
	if err != nil {
		return nil, fmt.Errorf("cli: workspace %q: %w", dir, err)
	}
	tools := tool.NewRegistry()
	plane := controlplane.NewMemory(inproc.NewFactory(tools, ws), rec)

	exec, err := runtime.NewSandbox(plane, tools, registry.NewSpecs(store),
		// A spec's tool names resolve against the actions every registered
		// connector advertises, so granting "github.comment" in a spec is the
		// whole of wiring an agent to GitHub's write side.
		runtime.WithCatalog(runtime.Connectors(sinks)),
		runtime.WithSandboxLogger(log),
	)
	if err != nil {
		return nil, fmt.Errorf("cli: sandbox: %w", err)
	}
	return exec, nil
}

// Invoke accepts a tag. It satisfies server.Invoker, and it is the one place
// where accepting work and knowing where to deliver it meet.
//
// The order matters: routes are registered with the router BEFORE the run is
// accepted, because accept publishes the run's first event and an event that
// arrives for a run the router has no routes for has nowhere to go. RunID is a
// pure function of the tag id, so the routes can be registered before the run
// exists.
func (c *core) Invoke(ctx context.Context, tag envelope.Tag) (server.Accepted, error) {
	if err := c.deliver.Register(runtime.RunID(tag.ID), tag.Routes()); err != nil {
		return server.Accepted{}, fmt.Errorf("cli: register routes: %w", err)
	}
	acc, err := c.rt.Accept(ctx, tag)
	if err != nil {
		return server.Accepted{}, err
	}
	return server.Accepted{
		RunID: acc.RunID,
		Rev:   acc.Rev,
		Topic: acc.Topic,
		At:    acc.AcceptedAt,
	}, nil
}

// Run reads a durable run record. It satisfies server.RunReader.
//
// It is a projection of the ledger, so reading one never touches a worker and
// polling it is cheap. A run belonging to another tenant is reported as not
// found rather than as a permission error: saying "exists, but not yours"
// confirms existence across the boundary.
func (c *core) Run(ctx context.Context, tenant, runID string) (server.Run, error) {
	rec, err := c.ledger.GetRun(ctx, runID)
	if err != nil {
		if errors.Is(err, drun.ErrNotFound) {
			return server.Run{}, fmt.Errorf("%w: run %q", server.ErrNotFound, runID)
		}
		return server.Run{}, fmt.Errorf("cli: read run %q: %w", runID, err)
	}

	var in runtime.Input
	if err := json.Unmarshal(rec.Input, &in); err != nil {
		return server.Run{}, fmt.Errorf("cli: decode run %q input: %w", runID, err)
	}
	if in.Tag.Tenant != tenant {
		return server.Run{}, fmt.Errorf("%w: run %q", server.ErrNotFound, runID)
	}

	out := server.Run{
		ID:        rec.ID,
		Tenant:    in.Tag.Tenant,
		Agent:     in.Tag.Agent,
		Rev:       in.Rev,
		Origin:    in.Tag.Origin,
		Status:    statusOf(rec.Status),
		Tag:       in.Tag,
		Error:     rec.Error,
		CreatedAt: rec.CreatedAt,
		EndedAt:   rec.CompletedAt,
	}
	if t, err := topic.Run(in.Tag.Agent, rec.ID); err == nil {
		out.Topic = t
	}
	// LastSeq is the run's own answer, not the ledger's: it is the sequence of
	// the completed event, which only a finished run has.
	if rec.Status == drun.StatusSucceeded && len(rec.Output) > 0 {
		var result runtime.Output
		if err := json.Unmarshal(rec.Output, &result); err == nil {
			out.LastSeq = result.LastSeq
		}
	}
	return out, nil
}

// statusOf maps a ledger status onto the edge's. duraturo has no "running"
// state — assignment is flow state and lives in the queue, never in the ledger
// — so a pending run is reported as accepted, which is the only thing the
// ledger actually knows about it.
func statusOf(s drun.RunStatus) server.Status {
	switch s {
	case drun.StatusSucceeded:
		return server.StatusCompleted
	case drun.StatusFailed:
		return server.StatusFailed
	default:
		return server.StatusAccepted
	}
}

// startDelivery runs the router: the half of mandatum that reads the bus and
// renders events onto surfaces. It is separable from execution on purpose —
// see work.go.
func (c *core) startDelivery(ctx context.Context, g *group) {
	g.run("router", func() error { return c.deliver.Run(ctx) })
}

// startExecution runs the durable worker and every trigger.
//
// The worker is a pull loop on a goroutine, not a service to deploy: it leases
// runs from the queue this process owns and executes them through the activity
// registry the runtime built. The triggers are the inbound half, and they feed
// one channel that this process drains into Invoke, so a connector never learns
// what a runtime is.
func (c *core) startExecution(ctx context.Context, g *group) {
	g.run("worker", func() error {
		return worker.New(c.ledger, c.queue,
			worker.WithRegistry(c.rt.Registry()),
			worker.WithConcurrency(max(c.cfg.Concurrency, 1)),
			worker.WithLogger(c.log),
		).Run(ctx)
	})

	triggers := c.sinks.Triggers()
	if len(triggers) == 0 {
		return
	}
	tags := make(chan envelope.Tag)
	for _, t := range triggers {
		g.run("trigger "+t.Name(), func() error {
			if err := t.Ingest(ctx, tags); err != nil && !errors.Is(err, context.Canceled) {
				return fmt.Errorf("cli: trigger %s: %w", t.Name(), err)
			}
			return nil
		})
	}
	g.run("tags", func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case tag := <-tags:
				// The deployment's scope, stamped exactly where the transport
				// stamps a credential's. A trigger has no credential.
				if tag.Tenant == "" {
					tag.Tenant = c.cfg.Tenant
				}
				acc, err := c.Invoke(ctx, tag)
				if err != nil {
					c.log.Error("cli: accept tag", "tag", tag.ID, "agent", tag.Agent, "error", err)
					continue
				}
				c.log.Info("cli: accepted", "run", acc.RunID, "agent", tag.Agent,
					"origin", tag.Origin, "rev", acc.Rev)
			}
		}
	})
}

// loadSchedules reads the cron connector's standing requests from a JSON file:
// an array of schedules, validated here so a typo fails at startup rather than
// at 2am.
func loadSchedules(path string) ([]cron.Schedule, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("cli: read schedules: %w", err)
	}
	var out []cron.Schedule
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("cli: parse schedules %q: %w", path, err)
	}
	for i, s := range out {
		if err := s.Validate(); err != nil {
			return nil, fmt.Errorf("cli: schedule %d in %q: %w", i, path, err)
		}
	}
	return out, nil
}

// group runs the long-lived loops of a command and collects the first failure.
//
// It is fifteen lines rather than a dependency because that is all this needs:
// every loop here ends on one shared context, and a caller wants "the reason we
// stopped", not a cancellation tree. A cancelled context is not a failure and
// is never reported as one.
type group struct {
	wg  sync.WaitGroup
	mu  sync.Mutex
	err error
}

func (g *group) run(name string, fn func() error) {
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		err := fn()
		if err == nil || errors.Is(err, context.Canceled) {
			return
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.err == nil {
			g.err = fmt.Errorf("%s: %w", name, err)
		}
	}()
}

// wait blocks until every loop has returned, or until grace expires. The
// timeout exists because a connector's flush can outlive the context that told
// it to stop, and a CLI that never exits is worse than one that reports a
// straggler.
func (g *group) wait(grace time.Duration) error {
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(grace):
		return errors.New("cli: timed out waiting for background loops to stop")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.err
}
