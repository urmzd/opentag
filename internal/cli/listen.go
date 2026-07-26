package cli

import (
	"errors"
	"io"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	opentagv1 "github.com/urmzd/opentag/gen/opentag/v1"
)

// newListenCmd builds `opentag listen`, the bus made visible.
//
// This is the command that shows opentag is a broker rather than a request/
// response API: it subscribes to a topic nobody invoked, and receives every run
// beneath it, including runs that start after the subscription is open. One
// terminal on `opentag listen agent:docs-bot` sees every tag anyone raises
// against that agent, from any connector.
func newListenCmd() *cobra.Command {
	var (
		kinds  []string
		rev    int32
		origin string
		runID  string
		from   uint64
	)

	cmd := &cobra.Command{
		Use:   "listen <topic>",
		Short: "Subscribe to a topic and print its events",
		Long: "Subscribe to a topic and print events until interrupted.\n\n" +
			"Topics are hierarchical and matched by prefix, so a subscription to an agent\n" +
			"receives every run beneath it:\n\n" +
			"  agent                    every agent\n" +
			"  agent:docs-bot           every run of one agent\n" +
			"  agent:docs-bot:run_01J   one run\n\n" +
			"The stream is long-lived and does not end when a run completes, because a\n" +
			"topic outlives the runs on it. Use --from to resume after a sequence number.",
		Example: "  opentag listen agent:docs-bot\n" +
			"  opentag listen agent --kinds lifecycle.completed\n" +
			"  opentag listen agent:docs-bot --kinds delta.citation --format json\n" +
			"  opentag listen agent:docs-bot --rev 7",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			t, err := validateTopic(args[0])
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}

			req := &opentagv1.SubscribeRequest{
				Subscription: &opentagv1.Subscription{
					Topic: t.String(),
					Filter: &opentagv1.Filter{
						Kinds:  kinds,
						Rev:    rev,
						Origin: origin,
						RunId:  runID,
					},
					From: from,
				},
			}

			stream, err := cl.bus.Subscribe(cmd.Context(), connect.NewRequest(req))
			if err != nil {
				return connectErr("subscribe", err)
			}
			defer func() { _ = stream.Close() }()

			u.logf("%s %s\n", u.dim("listening on"), u.bold(t.String()))

			for stream.Receive() {
				e := stream.Msg()
				if u.format == FormatJSON {
					if err := u.jsonLine(toEventJSON(e)); err != nil {
						return err
					}
					continue
				}
				renderEventText(u, e, false)
			}
			if err := stream.Err(); err != nil {
				// A closed stream on interrupt is how this command is meant to
				// end. Reporting it as a failure would make every Ctrl-C look
				// like the server misbehaved.
				if errors.Is(err, io.EOF) || errors.Is(err, cmd.Context().Err()) {
					return nil
				}
				// The Connect code says whether this is worth retrying, and a
				// subscription that dies at 3am should say why in one word
				// rather than only in an error string a supervisor cannot read.
				u.logf("%s %s\n", u.dim("stream ended:"), codeName(err))
				return connectErr("stream", err)
			}
			return nil
		},
	}

	cmd.Flags().StringSliceVar(&kinds, "kinds", nil,
		"Only these event kinds; families match, e.g. 'delta' or 'lifecycle.completed'")
	cmd.Flags().Int32Var(&rev, "rev", 0,
		"Only this pinned agent revision (0 means any)")
	cmd.Flags().StringVar(&origin, "origin", "",
		"Only runs raised by this origin, e.g. slack, github, cron")
	cmd.Flags().StringVar(&runID, "run", "",
		"Only this run")
	cmd.Flags().Uint64Var(&from, "from", 0,
		"Resume after this sequence number within each run")
	return cmd
}
