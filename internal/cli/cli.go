// Package cli is the opentag command tree and the process's composition root.
//
// It owns one concern: turning a command line and an environment into a running
// system, and turning that system's answers into bytes on a terminal. Every
// decision about what opentag IS lives in pkg/ and internal/server; every
// decision about how a person or a script drives it lives here.
//
// # Where to start reading
//
// core.go. It is the composition root: the one place where the bus, the agent
// registry, the router, the durable runtime, the connectors and the transport
// are assembled into a system, in the order the data flows through them. Reading
// it top to bottom is the fastest way to understand how opentag fits together,
// and it is deliberately linear — no helper indirection, no options struct
// three levels deep — because a composition root that needs a diagram has
// failed at its only job.
//
// Everything else in this package is a thin shell over it: serve.go binds a
// listener to it, work.go runs half of it, and the client commands (agent, tag,
// listen) do not build it at all — they speak to a server someone else is
// running.
//
// # Output discipline
//
// Results go to stdout, diagnostics go to stderr, and the two are never
// interleaved. --format json makes stdout a contract; text is for humans and is
// coloured only when stdout is a terminal and NO_COLOR is unset. See format.go.
//
// # Exit codes
//
// A script needs to distinguish "the agent does not exist" from "the server is
// down" without parsing English, so failures are classified into the codes
// declared below rather than all collapsing onto 1.
package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"
)

// Exit codes. They are part of the interface: a caller may branch on them, so
// a code never changes meaning between releases, and a new failure class takes
// a new number rather than borrowing one.
const (
	// ExitOK reports success.
	ExitOK = 0
	// ExitError reports a failure with no more specific classification: an
	// internal server error, a broken pipe, a malformed response.
	ExitError = 1
	// ExitUsage reports a bad invocation: an unknown flag, a missing argument,
	// an unparseable value. Nothing was attempted.
	ExitUsage = 2
	// ExitUnauthenticated reports a missing or rejected credential.
	ExitUnauthenticated = 3
	// ExitNotFound reports an agent, revision or run that does not exist, or
	// that the caller's tenant may not see.
	ExitNotFound = 4
	// ExitConflict reports a name already taken or a revision that moved on
	// since the caller read it. Retrying unchanged will fail the same way.
	ExitConflict = 5
	// ExitUnavailable reports a dependency that could not be reached: no
	// server on the address, a Redis that is down. Retrying unchanged is the
	// correct response.
	ExitUnavailable = 6
	// ExitInterrupted reports a run ended by SIGINT or SIGTERM. It follows the
	// shell convention of 128 plus the signal number for SIGINT.
	ExitInterrupted = 130
)

// errUsage marks an error the user can fix by re-typing the command. It is a
// sentinel rather than a cobra concept because the same classification has to
// work for a value that only fails validation deep inside a command.
var errUsage = errors.New("usage")

// errUnavailable marks a dependency that could not be reached: an address
// already bound, a Redis that is down. It exists because the server-side
// commands have no Connect code to classify on, and "retry this unchanged" is
// exactly the distinction a supervisor needs to make.
var errUnavailable = errors.New("unavailable")

// Version metadata injected at build time via -ldflags.
type Version struct {
	Version string
	Commit  string
	Date    string
}

// global flags shared by every command. They are package-level because cobra
// binds persistent flags once and every RunE reads them; the alternative is
// threading a config struct through constructors that have nothing else to say.
var (
	formatFlag string
	serverFlag string
	tokenFlag  string
)

// Environment variables the client commands read, so that a shell can be
// configured once instead of every command carrying two flags.
const (
	// EnvServer is the default for --server.
	EnvServer = "OPENTAG_SERVER"
	// EnvToken is the default for --token. A credential belongs in the
	// environment rather than in shell history, which is why it has one.
	EnvToken = "OPENTAG_TOKEN"
	// EnvTenant is the tenant `serve` issues its bootstrap credential for.
	EnvTenant = "OPENTAG_TENANT"
)

// DefaultServer is where the client commands look for a server, and what
// `serve` binds to.
const DefaultServer = "http://localhost:8383"

// DefaultAddr is `serve`'s listen address. It matches DefaultServer so that
// running `opentag serve` in one shell and `opentag tag ...` in another needs
// no configuration at all.
const DefaultAddr = ":8383"

// Execute runs the opentag CLI and returns its exit code.
//
// It returns a code rather than calling os.Exit so that main stays a one-liner
// and so a test can drive the whole tree in-process. Cobra's own error printing
// is off: an error is classified once, here, and printed to stderr in one shape.
func Execute(v Version) int {
	root := newRootCmd(v)

	// Signals cancel the command's context rather than killing the process, so
	// a serve draining its streams and a tag mid-answer both get to finish the
	// sentence they were in the middle of.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := root.ExecuteContext(ctx)
	if err == nil {
		return ExitOK
	}
	// A cancelled context here means the signal arrived, not that anything
	// failed. Reporting it as an error would make every Ctrl-C look like a bug.
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "interrupted")
		return ExitInterrupted
	}
	fmt.Fprintln(os.Stderr, "error:", err)
	return exitCodeFor(err)
}

func newRootCmd(v Version) *cobra.Command {
	root := &cobra.Command{
		Use:   "opentag",
		Short: "Tag an agent from anywhere; stream its events everywhere",
		Long: "opentag is an agent platform and a low-latency pub/sub bus.\n\n" +
			"Tag an agent from Slack, GitHub, Jira, a schedule or a webhook; the platform\n" +
			"runs it durably, streams its events to any subscriber, and delivers them to any\n" +
			"destination. Origin and destination are independent: a tag raised in GitHub can\n" +
			"answer into Jira. That is what makes it a mesh rather than a set of integrations.",
		Version:       v.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.PersistentFlags().StringVar(&formatFlag, "format", string(FormatText),
		"Output format: text|json")
	root.PersistentFlags().StringVar(&serverFlag, "server", envOr(EnvServer, DefaultServer),
		"opentag server base URL ($"+EnvServer+")")
	root.PersistentFlags().StringVar(&tokenFlag, "token", os.Getenv(EnvToken),
		"Bearer credential ($"+EnvToken+")")

	root.AddCommand(
		newServeCmd(),
		newWorkCmd(),
		newAgentCmd(),
		newTagCmd(),
		newListenCmd(),
		newVersionCmd(v),
		newUpdateCmd(v),
	)
	return root
}

// resolveUI builds the output pair for a command. It runs at the top of every
// RunE rather than at construction so that --format is read after cobra has
// parsed it, and so a bad value fails as a usage error before any work starts.
func resolveUI(cmd *cobra.Command) (*ui, error) {
	f, err := ParseFormat(formatFlag)
	if err != nil {
		return nil, err
	}
	return newUI(cmd.OutOrStdout(), cmd.ErrOrStderr(), f), nil
}

// exitCodeFor classifies a failure.
//
// Connect codes are the primary signal for the client commands, because the
// server has already done this classification once and re-deriving it from an
// error string would be a second, quietly diverging answer. Everything else
// falls back to ExitError, which is the honest code for "something went wrong
// and nothing here knows what kind of wrong".
func exitCodeFor(err error) int {
	if errors.Is(err, errUsage) {
		return ExitUsage
	}
	if errors.Is(err, errUnavailable) {
		return ExitUnavailable
	}
	if errors.Is(err, context.Canceled) {
		return ExitInterrupted
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return ExitUnauthenticated
	case connect.CodeNotFound:
		return ExitNotFound
	case connect.CodeAlreadyExists, connect.CodeAborted, connect.CodeFailedPrecondition:
		return ExitConflict
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded:
		return ExitUnavailable
	case connect.CodeInvalidArgument, connect.CodeOutOfRange:
		return ExitUsage
	default:
		return ExitError
	}
}

// envOr reads an environment variable with a fallback.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// usagef builds an error the user can fix by re-typing the command.
func usagef(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errUsage, fmt.Sprintf(format, args...))
}
