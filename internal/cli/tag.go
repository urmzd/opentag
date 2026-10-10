package cli

import (
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
	"google.golang.org/protobuf/types/known/timestamppb"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/topic"
)

// newTagCmd builds `mandatum tag`, the command that hands an agent its commission.
//
// It is the mesh in one line: tag an agent here, and say where the answer
// should go. With no --deliver the answer streams to this terminal and nowhere
// else; with one it also lands in a GitHub issue or a Jira ticket, and the
// terminal is then just one more observer of a run that was always going to be
// delivered somewhere else.
func newTagCmd() *cobra.Command {
	var (
		deliver []string
		kinds   []string
		meta    []string
		origin  string
		detach  bool
	)

	cmd := &cobra.Command{
		Use:   "tag <agent> <text>",
		Short: "Tag an agent and stream its answer",
		Long: "Tag an agent and stream its events until the run ends.\n\n" +
			"With --deliver the run also delivers to those addresses, which is the point of\n" +
			"the mesh: where a tag is raised and where its answer lands are independent.\n" +
			"Each --deliver may carry a kind filter after '|', so one destination can take\n" +
			"the whole stream while another takes only the outcome.",
		Example: "  mandatum tag docs-bot \"what is RAG?\"\n" +
			"  mandatum tag review-bot \"review this\" --deliver github://urmzd/mandatum/issues/42\n" +
			"  mandatum tag nightly \"summarise\" \\\n" +
			"    --deliver 'slack://T01/C02?thread=1699.001|delta' \\\n" +
			"    --deliver 'webhook://acme/deploys|lifecycle.completed'",
		Args: cobra.MinimumNArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}

			routes, err := parseRoutes(deliver)
			if err != nil {
				return err
			}
			metaMap, err := parseKeyValues(meta)
			if err != nil {
				return err
			}

			tag := &mandatumv1.Tag{
				// The id is the run's idempotency key. A terminal invocation
				// has no provider event to borrow one from, so it gets a fresh
				// one per invocation: re-running the command is a new question,
				// not a redelivery of the old one.
				Id:      newTagID(),
				Agent:   args[0],
				Origin:  origin,
				Text:    strings.Join(args[1:], " "),
				Deliver: routes,
				Meta:    metaMap,
				At:      timestamppb.New(time.Now()),
			}

			if detach {
				res, err := cl.invoke.Invoke(cmd.Context(), connect.NewRequest(tag))
				if err != nil {
					return connectErr("invoke", err)
				}
				msg := res.Msg
				if u.format == FormatJSON {
					return u.json(map[string]any{
						"run_id": msg.GetRunId(),
						"rev":    msg.GetRev(),
						"topic":  msg.GetTopic(),
					})
				}
				u.printf("%s\n", msg.GetRunId())
				u.logf("%s rev %d, topic %s\n", u.dim("accepted"), msg.GetRev(), msg.GetTopic())
				return nil
			}

			stream, err := cl.invoke.InvokeStream(cmd.Context(), connect.NewRequest(tag))
			if err != nil {
				return connectErr("invoke", err)
			}
			defer func() { _ = stream.Close() }()

			wanted := kindFilter(kinds)
			answered, ended := false, false
			for stream.Receive() {
				e := stream.Msg()
				if !wanted(e.GetKind()) {
					continue
				}
				if u.format == FormatJSON {
					if err := u.jsonLine(toEventJSON(e)); err != nil {
						return err
					}
				} else {
					renderEventText(u, e, true)
					answered = true
				}
				if terminal(e) {
					ended = true
					break
				}
			}
			if err := stream.Err(); err != nil {
				return connectErr("stream", err)
			}
			// The stream closing without a terminal event means the run is
			// still going and this client stopped watching it. Saying so
			// matters: the answer above is a prefix, not the whole thing, and
			// the run can still be followed by id.
			if !ended {
				u.warnf("stream ended before the run did; follow it with: mandatum listen agent:%s:%s\n",
					tag.Agent, tag.Id)
			}
			// The answer arrived as unterminated fragments so it would read as
			// one paragraph; close it here rather than leaving the shell prompt
			// glued to the last token.
			if u.format == FormatText && answered {
				u.printf("\n")
			}
			return nil
		},
	}

	cmd.Flags().StringArrayVar(&deliver, "deliver", nil,
		"Deliver to an address, optionally '|kind[,kind]' filtered (repeatable)")
	cmd.Flags().StringSliceVar(&kinds, "kinds", nil,
		"Only print these event kinds; families match, e.g. 'delta'")
	cmd.Flags().StringArrayVar(&meta, "meta", nil,
		"Context for the agent's tools as key=value (repeatable)")
	cmd.Flags().StringVar(&origin, "origin", "cli",
		"Origin recorded on the tag")
	cmd.Flags().BoolVar(&detach, "detach", false,
		"Accept the run and print its id without streaming")
	return cmd
}

// kindsSeparator divides an address from its kind filter in a --deliver value.
//
// It is "|" rather than the more obvious "=" because "=" is ambiguous inside a
// URI: slack://T01/C02?thread=1699.001 already contains one, and no rule for
// choosing which "=" is the separator survives contact with a thread timestamp
// that itself contains dots and digits. "|" is excluded from the URI character
// set entirely, so splitting on the first one can never be wrong. A shell needs
// the value quoted either way, because of the "?".
const kindsSeparator = "|"

// parseRoutes turns --deliver values into routes.
//
// The syntax is "<address>" or "<address>|<kind>[,<kind>...]". An empty kind
// list means every kind, which is the same default a Route with no Kinds has.
func parseRoutes(specs []string) ([]*mandatumv1.Route, error) {
	var out []*mandatumv1.Route
	for _, spec := range specs {
		raw, kinds, _ := strings.Cut(spec, kindsSeparator)
		addr, err := address.Parse(strings.TrimSpace(raw))
		if err != nil {
			return nil, usagef("--deliver %q: %v", spec, err)
		}
		route := &mandatumv1.Route{Target: toProtoAddress(addr)}
		for _, k := range strings.Split(kinds, ",") {
			if k = strings.TrimSpace(k); k != "" {
				route.Kinds = append(route.Kinds, k)
			}
		}
		out = append(out, route)
	}
	return out, nil
}

func toProtoAddress(a address.Address) *mandatumv1.Address {
	return &mandatumv1.Address{
		Connector: a.Connector,
		Workspace: a.Workspace,
		Path:      a.Path,
		Params:    a.Params,
	}
}

// kindFilter builds a client-side predicate over event kinds. It matches a
// family or an exact kind, the same rule envelope.Kind.Matches applies on the
// server, so --kinds means the same thing wherever it is enforced.
func kindFilter(kinds []string) func(string) bool {
	if len(kinds) == 0 {
		return func(string) bool { return true }
	}
	return func(kind string) bool {
		for _, sel := range kinds {
			sel = strings.TrimSpace(sel)
			if sel == "" || sel == kind {
				return true
			}
			if family, _, ok := strings.Cut(kind, "."); ok && sel == family {
				return true
			}
		}
		return false
	}
}

func parseKeyValues(pairs []string) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, nil
	}
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		k, v, ok := strings.Cut(p, "=")
		if !ok || strings.TrimSpace(k) == "" {
			return nil, usagef("--meta %q is not key=value", p)
		}
		out[strings.TrimSpace(k)] = v
	}
	return out, nil
}

// newTagID mints an idempotency key for a terminal invocation. It is derived
// from the clock and a counter rather than from the text, because typing the
// same question twice is two questions.
func newTagID() string {
	return fmt.Sprintf("cli-%d", time.Now().UnixNano())
}

// validateTopic is shared by listen and any future topic-taking command, so
// that a bad topic fails as a usage error before a connection is opened.
func validateTopic(s string) (topic.Topic, error) {
	t, err := topic.Parse(strings.TrimSpace(s))
	if err != nil {
		return topic.Topic{}, usagef("%v", err)
	}
	return t, nil
}
