package cli

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
)

// newAgentCmd builds `mandatum agent`, the control plane in a terminal.
//
// Creating an agent is submitting a document, which is what makes the platform
// self-serve: no redeploy, no code, no restart. The subcommands mirror the
// service exactly, including the distinction the service insists on between
// create and revise, so that no invocation is ever ambiguous about whether it
// added an agent or changed one.
func newAgentCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "agent",
		Short: "Create and inspect agents",
		Long: "Manage agent definitions on a running server.\n\n" +
			"A definition is immutable: revise appends revision N+1 and never modifies what\n" +
			"is already there, because runs accepted under a revision keep replaying under\n" +
			"it. That is why history is readable forever and why delete retires a name\n" +
			"without erasing what ran.",
	}
	cmd.AddCommand(
		newAgentCreateCmd(),
		newAgentReviseCmd(),
		newAgentGetCmd(),
		newAgentListCmd(),
		newAgentHistoryCmd(),
		newAgentDeleteCmd(),
	)
	return cmd
}

// specFile is the on-disk form of an agent definition.
//
// It is JSON rather than YAML deliberately: the encoder is in the standard
// library, so the CLI carries no parser dependency for a file most users will
// generate from a template or a script rather than hand-write.
type specFile struct {
	Name         string            `json:"name"`
	Description  string            `json:"description,omitempty"`
	Model        string            `json:"model,omitempty"`
	Provider     string            `json:"provider,omitempty"`
	SystemPrompt string            `json:"system_prompt"`
	Tools        []string          `json:"tools,omitempty"`
	Sources      []specFileSource  `json:"sources,omitempty"`
	Access       specFileAccess    `json:"access,omitempty"`
	Meta         map[string]string `json:"meta,omitempty"`
}

type specFileSource struct {
	Name    string            `json:"name,omitempty"`
	URI     string            `json:"uri"`
	Options map[string]string `json:"options,omitempty"`
}

type specFileAccess struct {
	Spawn          []string `json:"spawn,omitempty"`
	WorkspaceAreas []string `json:"workspace_areas,omitempty"`
}

func loadSpec(path string) (*mandatumv1.AgentSpec, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read spec: %w", err)
	}
	var f specFile
	dec := json.NewDecoder(bytes.NewReader(b))
	// An unknown field is almost always a typo in a hand-written spec, and a
	// silently ignored "sytem_prompt" would produce an agent with no
	// instructions and no complaint.
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return nil, usagef("%s is not a valid agent spec: %v", path, err)
	}

	spec := &mandatumv1.AgentSpec{
		Name:         f.Name,
		Description:  f.Description,
		Model:        f.Model,
		Provider:     f.Provider,
		SystemPrompt: f.SystemPrompt,
		Tools:        f.Tools,
		Access: &mandatumv1.Access{
			Spawn:          f.Access.Spawn,
			WorkspaceAreas: f.Access.WorkspaceAreas,
		},
	}
	for _, s := range f.Sources {
		spec.Sources = append(spec.Sources, &mandatumv1.Source{
			Name:    s.Name,
			Uri:     s.URI,
			Options: s.Options,
		})
	}
	return spec, nil
}

func newAgentCreateCmd() *cobra.Command {
	var file string
	cmd := &cobra.Command{
		Use:     "create -f <spec.json>",
		Short:   "Create an agent as revision 1",
		Example: "  mandatum agent create -f docs-bot.json",
		RunE: func(cmd *cobra.Command, _ []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			if file == "" {
				return usagef("--file is required")
			}
			spec, err := loadSpec(file)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}
			res, err := cl.agents.CreateAgent(cmd.Context(),
				connect.NewRequest(&mandatumv1.CreateAgentRequest{Spec: spec}))
			if err != nil {
				return connectErr("create agent", err)
			}
			return printRevision(u, res.Msg.GetRevision())
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "Agent spec JSON file")
	return cmd
}

func newAgentReviseCmd() *cobra.Command {
	var (
		file        string
		expectedRev int32
	)
	cmd := &cobra.Command{
		Use:   "revise -f <spec.json>",
		Short: "Append the next revision of an agent",
		Long: "Append a new revision. The previous revision is left untouched.\n\n" +
			"--expect makes the append conditional on the current revision, so two editors\n" +
			"racing cannot silently overwrite each other: the loser is told the revision\n" +
			"moved instead of quietly winning.",
		Example: "  mandatum agent revise -f docs-bot.json\n" +
			"  mandatum agent revise -f docs-bot.json --expect 6",
		RunE: func(cmd *cobra.Command, _ []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			if file == "" {
				return usagef("--file is required")
			}
			spec, err := loadSpec(file)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}
			res, err := cl.agents.ReviseAgent(cmd.Context(),
				connect.NewRequest(&mandatumv1.ReviseAgentRequest{Spec: spec, ExpectedRev: expectedRev}))
			if err != nil {
				return connectErr("revise agent", err)
			}
			return printRevision(u, res.Msg.GetRevision())
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "Agent spec JSON file")
	cmd.Flags().Int32Var(&expectedRev, "expect", 0,
		"Only append if this is the current revision (0 disables the check)")
	return cmd
}

func newAgentGetCmd() *cobra.Command {
	var rev int32
	cmd := &cobra.Command{
		Use:   "get <name>",
		Short: "Read one revision of an agent",
		Long: "Read an agent definition, defaulting to the latest revision.\n\n" +
			"A pinned revision stays readable after the agent is deleted, because a run\n" +
			"that executed under it must remain explicable.",
		Example: "  mandatum agent get docs-bot\n  mandatum agent get docs-bot --rev 6",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}
			res, err := cl.agents.GetAgent(cmd.Context(),
				connect.NewRequest(&mandatumv1.GetAgentRequest{Name: args[0], Rev: rev}))
			if err != nil {
				return connectErr("get agent", err)
			}
			return printRevisionFull(u, res.Msg.GetRevision())
		},
	}
	cmd.Flags().Int32Var(&rev, "rev", 0, "Revision to read (0 means latest)")
	return cmd
}

func newAgentListCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List agents at their latest revision",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}

			// Pages are followed to exhaustion here rather than exposed as a
			// flag: a person listing agents wants the list, and a script piping
			// json wants all of it too.
			var all []*mandatumv1.Revision
			token := ""
			for {
				res, err := cl.agents.ListAgents(cmd.Context(),
					connect.NewRequest(&mandatumv1.ListAgentsRequest{PageToken: token}))
				if err != nil {
					return connectErr("list agents", err)
				}
				all = append(all, res.Msg.GetAgents()...)
				token = res.Msg.GetNextPageToken()
				if token == "" {
					break
				}
			}

			if u.format == FormatJSON {
				out := make([]map[string]any, 0, len(all))
				for _, r := range all {
					out = append(out, revisionMap(r))
				}
				return u.json(out)
			}
			if len(all) == 0 {
				u.logf("no agents\n")
				return nil
			}
			rows := make([][]string, 0, len(all))
			for _, r := range all {
				rows = append(rows, []string{
					r.GetSpec().GetName(),
					strconv.Itoa(int(r.GetRev())),
					r.GetSpec().GetProvider(),
					r.GetSpec().GetModel(),
					r.GetSpec().GetDescription(),
				})
			}
			u.table([]string{"NAME", "REV", "PROVIDER", "MODEL", "DESCRIPTION"}, rows)
			return nil
		},
	}
	return cmd
}

func newAgentHistoryCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history <name>",
		Short: "Show every revision of an agent, oldest first",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}
			res, err := cl.agents.GetAgentHistory(cmd.Context(),
				connect.NewRequest(&mandatumv1.GetAgentHistoryRequest{Name: args[0]}))
			if err != nil {
				return connectErr("agent history", err)
			}
			revs := res.Msg.GetRevisions()

			if u.format == FormatJSON {
				out := make([]map[string]any, 0, len(revs))
				for _, r := range revs {
					out = append(out, revisionMap(r))
				}
				return u.json(out)
			}
			rows := make([][]string, 0, len(revs))
			for _, r := range revs {
				rows = append(rows, []string{
					strconv.Itoa(int(r.GetRev())),
					shortHash(r.GetHash()),
					stamp(r),
					r.GetCreatedBy(),
				})
			}
			u.table([]string{"REV", "HASH", "CREATED", "BY"}, rows)
			return nil
		},
	}
	return cmd
}

func newAgentDeleteCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Retire an agent name",
		Long: "Retire a name so no new tag is accepted for it.\n\n" +
			"History is not erased and in-flight runs are not cancelled: a run holds its\n" +
			"pinned revision independently of the registry, so deleting a name must not\n" +
			"make a completed run's provenance unreadable.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			u, err := resolveUI(cmd)
			if err != nil {
				return err
			}
			cl, err := dial()
			if err != nil {
				return err
			}
			if _, err := cl.agents.DeleteAgent(cmd.Context(),
				connect.NewRequest(&mandatumv1.DeleteAgentRequest{Name: args[0]})); err != nil {
				return connectErr("delete agent", err)
			}
			if u.format == FormatJSON {
				return u.json(map[string]any{"name": args[0], "deleted": true})
			}
			u.printf("%s retired\n", args[0])
			u.logf("%s\n", u.dim("history and pinned revisions remain readable"))
			return nil
		},
	}
	return cmd
}

func printRevision(u *ui, r *mandatumv1.Revision) error {
	if u.format == FormatJSON {
		return u.json(revisionMap(r))
	}
	u.printf("%s rev %d\n", u.bold(r.GetSpec().GetName()), r.GetRev())
	u.logf("%s %s\n", u.dim("hash"), shortHash(r.GetHash()))
	return nil
}

func printRevisionFull(u *ui, r *mandatumv1.Revision) error {
	if u.format == FormatJSON {
		return u.json(revisionMap(r))
	}
	s := r.GetSpec()
	u.printf("%s\n", u.bold(s.GetName()))
	u.printf("  revision     %d  %s\n", r.GetRev(), u.dim(shortHash(r.GetHash())))
	if s.GetDescription() != "" {
		u.printf("  description  %s\n", s.GetDescription())
	}
	u.printf("  provider     %s\n", s.GetProvider())
	u.printf("  model        %s\n", s.GetModel())
	if len(s.GetTools()) > 0 {
		u.printf("  tools        %v\n", s.GetTools())
	}
	for _, src := range s.GetSources() {
		u.printf("  source       %s\n", src.GetUri())
	}
	if a := s.GetAccess(); a != nil {
		if len(a.GetSpawn()) > 0 {
			u.printf("  spawn        %v\n", a.GetSpawn())
		}
		if len(a.GetWorkspaceAreas()) > 0 {
			u.printf("  areas        %v\n", a.GetWorkspaceAreas())
		}
	}
	u.printf("  created      %s by %s\n", stamp(r), r.GetCreatedBy())
	u.printf("\n%s\n%s\n", u.dim("system prompt"), s.GetSystemPrompt())
	return nil
}

func revisionMap(r *mandatumv1.Revision) map[string]any {
	s := r.GetSpec()
	sources := make([]map[string]any, 0, len(s.GetSources()))
	for _, src := range s.GetSources() {
		sources = append(sources, map[string]any{
			"name": src.GetName(), "uri": src.GetUri(), "options": src.GetOptions(),
		})
	}
	m := map[string]any{
		"name":          s.GetName(),
		"rev":           r.GetRev(),
		"hash":          r.GetHash(),
		"description":   s.GetDescription(),
		"provider":      s.GetProvider(),
		"model":         s.GetModel(),
		"system_prompt": s.GetSystemPrompt(),
		"tools":         s.GetTools(),
		"sources":       sources,
		"created_by":    r.GetCreatedBy(),
	}
	if a := s.GetAccess(); a != nil {
		m["access"] = map[string]any{
			"spawn":           a.GetSpawn(),
			"workspace_areas": a.GetWorkspaceAreas(),
		}
	}
	if ts := r.GetCreatedAt(); ts != nil {
		m["created_at"] = ts.AsTime().UTC().Format(time.RFC3339)
	}
	return m
}

func stamp(r *mandatumv1.Revision) string {
	if ts := r.GetCreatedAt(); ts != nil {
		return ts.AsTime().UTC().Format(time.RFC3339)
	}
	return ""
}

// shortHash trims a content hash for display. The full value stays in json,
// where a script may compare it; a terminal only needs enough to see that two
// revisions differ.
func shortHash(h string) string {
	const n = 12
	if len(h) > n {
		return h[:n]
	}
	return h
}
