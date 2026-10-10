package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/urmzd/dispatch/pkg/metrics"

	"github.com/urmzd/mandatum/internal/server"
	"github.com/urmzd/mandatum/pkg/registry"
)

// DefaultTenant is the authorization scope a single-tenant deployment runs
// under when $MANDATUM_TENANT is unset. It is a real scope rather than a blank
// one, so that turning multi-tenancy on later is a configuration change and not
// a data migration.
const DefaultTenant = "default"

// serveGrace bounds how long the background loops are given to stop after the
// HTTP server has drained.
const serveGrace = 20 * time.Second

func newServeCmd() *cobra.Command {
	var (
		addr        string
		schedules   string
		concurrency int
		tenant      string
		noWorker    bool
		verbose     bool
	)
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the whole system: control plane, bus, runtime, router, connectors",
		Long: "serve runs mandatum in one process: the agent registry, the broker, the durable\n" +
			"runtime and its worker, the delivery router, and every connector the environment\n" +
			"configures. It is the composition root, and the only command that owns state.\n\n" +
			"With no configuration it uses the in-memory registry, bus, ledger and queue.\n" +
			"Those are complete implementations, not stubs; they simply do not outlive the\n" +
			"process. Set $" + EnvRedis + " to put the bus on Redis Streams.",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ui, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			return runServe(cmd.Context(), ui, serveOptions{
				addr:        addr,
				schedules:   schedules,
				concurrency: concurrency,
				tenant:      tenant,
				noWorker:    noWorker,
				verbose:     verbose,
			})
		},
	}
	cmd.Flags().StringVar(&addr, "addr", DefaultAddr, "Listen address")
	cmd.Flags().StringVar(&schedules, "schedules", "", "JSON file of cron schedules to run")
	cmd.Flags().IntVar(&concurrency, "concurrency", 1, "Runs executed at once")
	cmd.Flags().StringVar(&tenant, "tenant", envOr(EnvTenant, DefaultTenant),
		"Authorization scope this deployment serves ($"+EnvTenant+")")
	cmd.Flags().BoolVar(&noWorker, "no-worker", false,
		"Accept and stream, but execute nothing; run `mandatum work` elsewhere")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Log at debug level")
	return cmd
}

type serveOptions struct {
	addr        string
	schedules   string
	concurrency int
	tenant      string
	noWorker    bool
	verbose     bool
}

// runServe builds the system, binds a listener to it, and serves until the
// context is cancelled.
//
// The order is: build everything, start the background loops, then bind. A
// listener that is accepting before the router is reading would accept a tag
// whose events have nowhere to go, which is exactly the kind of gap that only
// shows up under load.
func runServe(ctx context.Context, ui *ui, opts serveOptions) error {
	log := newLogger(ui, opts.verbose)
	rec := metrics.NewMemory()

	scheds, err := loadSchedules(opts.schedules)
	if err != nil {
		return err
	}

	c, err := newCore(coreConfig{
		Tenant:      opts.tenant,
		Redis:       os.Getenv(EnvRedis),
		Workspace:   os.Getenv(EnvWorkspace),
		Schedules:   scheds,
		Concurrency: opts.concurrency,
		Log:         log,
		Metrics:     rec,
	})
	if err != nil {
		return err
	}
	defer func() { _ = c.close() }()

	// The credential. A server with no authenticator has no tenant, and no
	// tenant means every subscriber sees every event, so there is no "off"
	// here: an unset token is generated and printed, which is the only default
	// that is both runnable and safe.
	token := strings.TrimSpace(os.Getenv(EnvToken))
	generated := token == ""
	if generated {
		token, err = newToken()
		if err != nil {
			return err
		}
	}
	auth, err := server.NewTokens(map[string]server.Identity{
		token: {Tenant: opts.tenant, Subject: "mandatum"},
	})
	if err != nil {
		return fmt.Errorf("cli: credentials: %w", err)
	}

	srv, err := server.New(server.Config{
		Auth: auth,
		// The registry speaks agentspec types; the edge speaks its own. The
		// adapter is the one place that translation lives, so neither side
		// grows a dependency on the other's vocabulary.
		Store:   registry.ServerStore(c.store),
		Bus:     c.broker,
		Invoker: c,
		Runs:    c,
		Metrics: rec,
		Logger:  log,
	})
	if err != nil {
		return fmt.Errorf("cli: server: %w", err)
	}

	// Connectors that receive their own webhooks serve them themselves: each
	// verifies its own signature, so no unverified body ever reaches a handler
	// here. They are mounted beside the API rather than inside it because the
	// path a third party POSTs to is that connector's contract, not ours.
	mux := http.NewServeMux()
	mux.Handle("/", srv.Handler())
	for name, h := range c.webhooks {
		mux.Handle("/connectors/"+name+"/", http.StripPrefix("/connectors/"+name, h))
		mux.Handle("/connectors/"+name, h)
	}

	ln, err := net.Listen("tcp", opts.addr)
	if err != nil {
		return fmt.Errorf("%w: listen on %s: %w", errUnavailable, opts.addr, err)
	}

	var g group
	c.startDelivery(ctx, &g)
	if !opts.noWorker {
		c.startExecution(ctx, &g)
	}

	announce(ui, c, opts, ln.Addr().String(), token, generated)

	g.run("http", func() error {
		hs := &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
		}
		errs := make(chan error, 1)
		go func() { errs <- hs.Serve(ln) }()
		select {
		case err := <-errs:
			if err == nil || err == http.ErrServerClosed {
				return nil
			}
			return fmt.Errorf("cli: serve: %w", err)
		case <-ctx.Done():
		}
		// Streams are told to end first, then in-flight requests are given a
		// moment: waiting for the HTTP server first would wait forever,
		// because an SSE stream has no natural end.
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), server.DefaultShutdownGrace)
		defer cancel()
		_ = srv.Shutdown(shutdown)
		err := hs.Shutdown(shutdown)
		<-errs
		if err != nil {
			return fmt.Errorf("cli: shutdown: %w", err)
		}
		return nil
	})

	<-ctx.Done()
	ui.logf("\n%s draining\n", ui.dim("mandatum"))
	return g.wait(serveGrace)
}

// announce reports what was built, on stderr, so that `mandatum serve | tee` and
// a piped --format json both stay clean. Everything printed here is something
// an operator has to know and cannot derive: the address, the credential, and
// which optional halves are actually running.
func announce(ui *ui, c *core, opts serveOptions, addr, token string, generated bool) {
	ui.logf("%s listening on %s\n", ui.bold("mandatum"), ui.cyan("http://"+addr))
	ui.logf("  tenant     %s\n", opts.tenant)
	bus := "memory"
	if os.Getenv(EnvRedis) != "" {
		bus = "redis"
	}
	ui.logf("  bus        %s\n", bus)
	ui.logf("  worker     %s\n", onOff(!opts.noWorker))
	names := c.sinks.Names()
	if len(names) == 0 {
		ui.logf("  connectors %s\n", ui.dim("none configured; events stream to subscribers only"))
	} else {
		ui.logf("  connectors %s\n", strings.Join(names, ", "))
	}
	for name := range c.webhooks {
		ui.logf("             http://%s/connectors/%s\n", addr, name)
	}
	if generated {
		ui.logf("\n  %s no $%s was set, so one was generated for this process:\n", ui.yellow("credential:"), EnvToken)
		ui.logf("    export %s=%s\n", EnvToken, token)
	}
	ui.logf("\n")
}

func onOff(on bool) string {
	if on {
		return "on"
	}
	return "off"
}

// newToken mints a bootstrap credential. It is 32 bytes from crypto/rand
// because it is a bearer token: anything derived from the clock or the process
// would be guessable by whoever is watching the clock.
func newToken() (string, error) {
	var b [32]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("cli: generate credential: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// newLogger sends operational logging to stderr, where diagnostics belong, so
// that stdout stays a contract even while the server is logging a reconnect.
func newLogger(ui *ui, verbose bool) *slog.Logger {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(ui.err, &slog.HandlerOptions{Level: level}))
}
