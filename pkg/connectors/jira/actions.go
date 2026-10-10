package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
)

// Actions implements connector.Actor.
//
// These three are the "get work done" half of the connector. An agent that can
// only comment has written a report; an agent that can transition and assign has
// moved the ticket, and the action.taken event says so on the same bus as the
// conversation.
func (c *Connector) Actions() []connector.Action {
	return []connector.Action{
		{
			Name: "jira_transition",
			Description: "Move a Jira issue to another status, by naming the status or the " +
				"transition: \"Done\", \"In Progress\", \"Start Progress\". Only the " +
				"transitions available from the issue's current status can be applied, " +
				"and calling this with a name that is not available answers with the " +
				"list that is.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "to": {"type": "string", "description": "Target status or transition name, such as \"Done\"."},
    "key": {"type": "string", "description": "Issue key, such as PROJ-5. Defaults to the issue the run is answering on."}
  },
  "required": ["to"],
  "additionalProperties": false
}`),
			Invoke: c.transition,
		},
		{
			Name: "jira_comment",
			Description: "Post a comment on a Jira issue, in Jira wiki markup. Use this to " +
				"leave a durable note for humans; the run's answer is already delivered " +
				"as its own comment and does not need reposting.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "body": {"type": "string", "description": "Comment body, Jira wiki markup."},
    "key": {"type": "string", "description": "Issue key, such as PROJ-5. Defaults to the issue the run is answering on."}
  },
  "required": ["body"],
  "additionalProperties": false
}`),
			Invoke: c.comment,
		},
		{
			Name: "jira_assign",
			Description: "Set the assignee of a Jira issue to an Atlassian account id, or " +
				"clear it. Use it to hand work to the person who should do it next.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "assignee": {"type": "string", "description": "Atlassian account id. Empty, or \"unassigned\", clears the assignee."},
    "key": {"type": "string", "description": "Issue key, such as PROJ-5. Defaults to the issue the run is answering on."}
  },
  "additionalProperties": false
}`),
			Invoke: c.assign,
		},
	}
}

// transition moves an issue.
//
// The model names a status ("Done"); Jira's API takes a transition id ("31")
// that differs per workflow and means nothing to anybody. Resolving the one into
// the other here — and reporting the available options when it cannot — is what
// makes this an action a model can actually call, rather than one it has to
// guess an id for.
func (c *Connector) transition(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		To  string `json:"to"`
		Key string `json:"key"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("jira: jira_transition args: %w", err)
	}
	if strings.TrimSpace(args.To) == "" {
		return connector.Result{}, fmt.Errorf("jira: jira_transition needs a target status")
	}
	ref, err := c.resolve(target, args.Key)
	if err != nil {
		return connector.Result{}, err
	}

	available, err := c.cfg.API.Transitions(ctx, ref)
	if err != nil {
		return connector.Result{}, err
	}
	match, ok := matchTransition(available, args.To)
	if !ok {
		return connector.Result{}, fmt.Errorf("jira: %s has no transition to %q; available: %s",
			ref, args.To, describe(available))
	}
	if err := c.cfg.API.Transition(ctx, ref, match.ID); err != nil {
		return connector.Result{}, err
	}

	landed := match.To
	if landed == "" {
		landed = match.Name
	}
	return connector.Result{
		Summary: fmt.Sprintf("moved %s to %s", ref, landed),
		Address: Address(ref),
	}, nil
}

// matchTransition finds the transition the model meant.
//
// Three spellings are accepted, in the order a model is likely to produce them:
// the status it wants to land in, the name of the button, and the raw id. Case
// is ignored because a model writes "done" as often as "Done" and a workflow
// that refused it would be a workflow the agent silently stops using.
func matchTransition(available []Transition, want string) (Transition, bool) {
	want = strings.TrimSpace(want)
	for _, t := range available {
		if strings.EqualFold(t.To, want) {
			return t, true
		}
	}
	for _, t := range available {
		if strings.EqualFold(t.Name, want) {
			return t, true
		}
	}
	for _, t := range available {
		if t.ID == want {
			return t, true
		}
	}
	return Transition{}, false
}

// describe renders the available transitions for an error message a model can
// act on.
func describe(available []Transition) string {
	if len(available) == 0 {
		return "none, so this issue cannot be moved from its current status"
	}
	names := make([]string, 0, len(available))
	for _, t := range available {
		if t.To != "" && !strings.EqualFold(t.To, t.Name) {
			names = append(names, fmt.Sprintf("%s (to %s)", t.Name, t.To))
			continue
		}
		names = append(names, t.Name)
	}
	return strings.Join(names, ", ")
}

func (c *Connector) comment(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Body string `json:"body"`
		Key  string `json:"key"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("jira: jira_comment args: %w", err)
	}
	if strings.TrimSpace(args.Body) == "" {
		return connector.Result{}, fmt.Errorf("jira: jira_comment needs a body")
	}
	ref, err := c.resolve(target, args.Key)
	if err != nil {
		return connector.Result{}, err
	}
	id, err := c.cfg.API.CreateComment(ctx, ref, args.Body)
	if err != nil {
		return connector.Result{}, err
	}
	return connector.Result{
		Summary: fmt.Sprintf("commented on %s", ref),
		Address: Address(ref).WithParam("comment", id),
	}, nil
}

func (c *Connector) assign(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Assignee string `json:"assignee"`
		Key      string `json:"key"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("jira: jira_assign args: %w", err)
	}
	ref, err := c.resolve(target, args.Key)
	if err != nil {
		return connector.Result{}, err
	}

	assignee := strings.TrimSpace(args.Assignee)
	// A model asked to clear an assignee writes one of these rather than an
	// empty string, and refusing them would leave it with no way to say so.
	switch strings.ToLower(assignee) {
	case "unassigned", "none", "null", "nobody":
		assignee = ""
	}
	if err := c.cfg.API.Assign(ctx, ref, assignee); err != nil {
		return connector.Result{}, err
	}

	summary := fmt.Sprintf("assigned %s to %s", ref, assignee)
	if assignee == "" {
		summary = fmt.Sprintf("unassigned %s", ref)
	}
	return connector.Result{Summary: summary, Address: Address(ref)}, nil
}

// resolve settles which issue an action acts on.
//
// An action may name another issue on the SAME site, and may not name another
// site: the route decided which Jira this run can touch, and an argument the
// model produced must not be able to move that boundary. That is the same rule
// the GitHub connector applies to owners, for the same reason.
func (c *Connector) resolve(target address.Address, key string) (Ref, error) {
	ref, err := c.ref(target)
	if err != nil {
		return Ref{}, err
	}
	if key = strings.TrimSpace(key); key != "" && key != ref.Issue {
		if !ValidKey(key) {
			return Ref{}, fmt.Errorf("jira: %q is not an issue key such as PROJ-5", key)
		}
		ref.Issue = key
	}
	return ref, nil
}
