package cli

import (
	"context"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

func newWorkCmd() *cobra.Command {
	var (
		tenant  string
		verbose bool
	)
	cmd := &cobra.Command{
		Use:   "work",
		Short: "Run the delivery half: read the bus, render onto connector sinks",
		Long: "work runs the router and nothing else: it subscribes to the bus and delivers\n" +
			"each event to the sinks the run's routes selected. It accepts no tags, executes\n" +
			"no agents and serves no HTTP, so it can be scaled independently of the process\n" +
			"that runs them.\n\n" +
			"It needs a shared bus, which means $" + EnvRedis + ": the in-memory broker is\n" +
			"private to the process that created it, so a second process reading it would\n" +
			"read an empty one. That is not a limitation of the router; it is what \"in\n" +
			"memory\" means, and refusing here is better than idling forever on a bus that\n" +
			"nobody is publishing to.",
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ui, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			return runWork(cmd.Context(), ui, tenant, verbose)
		},
	}
	cmd.Flags().StringVar(&tenant, "tenant", envOr(EnvTenant, DefaultTenant),
		"Authorization scope this deployment serves ($"+EnvTenant+")")
	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Log at debug level")
	return cmd
}

func runWork(ctx context.Context, ui *ui, tenant string, verbose bool) error {
	redisURL := strings.TrimSpace(os.Getenv(EnvRedis))
	if redisURL == "" {
		return usagef("work needs $%s: the in-memory bus is private to the process that created it, so there would be nothing to read", EnvRedis)
	}

	c, err := newCore(coreConfig{
		Tenant:    tenant,
		Redis:     redisURL,
		Workspace: os.Getenv(EnvWorkspace),
		Log:       newLogger(ui, verbose),
	})
	if err != nil {
		return err
	}
	defer func() { _ = c.close() }()

	names := c.sinks.Names()
	if len(names) == 0 {
		return usagef("no connectors are configured, so there is nowhere to deliver to; see `mandatum serve --help` for the environment variables that enable one")
	}
	ui.logf("%s delivering to %s\n", ui.bold("mandatum work"), strings.Join(names, ", "))

	var g group
	c.startDelivery(ctx, &g)

	<-ctx.Done()
	ui.logf("\n%s draining\n", ui.dim("mandatum work"))
	return g.wait(serveGrace)
}
