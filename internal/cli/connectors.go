package cli

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/cron"
	"github.com/urmzd/mandatum/pkg/connectors/github"
	"github.com/urmzd/mandatum/pkg/connectors/jira"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/connectors/slack"
	"github.com/urmzd/mandatum/pkg/connectors/webhook"
	"github.com/urmzd/mandatum/pkg/registry"
	"github.com/urmzd/mandatum/pkg/signature"
)

// Environment variables that turn a connector on.
//
// Each connector is registered only when its credentials are present, and its
// absence is not an error: a deployment that answers into Slack and nowhere
// else should not have to say so twice. What IS an error is a half-configured
// one — a token with no signing secret authenticates the internet — so every
// constructor below refuses rather than degrades.
const (
	// Slack. A signing secret is what makes the trigger safe; a bot token is
	// what makes the sink able to write.
	EnvSlackToken   = "MANDATUM_SLACK_TOKEN"
	EnvSlackSecret  = "MANDATUM_SLACK_SIGNING_SECRET"
	EnvSlackTeam    = "MANDATUM_SLACK_TEAM"
	EnvSlackBots    = "MANDATUM_SLACK_BOTS"
	EnvSlackDefault = "MANDATUM_SLACK_DEFAULT_AGENT"

	// GitHub.
	EnvGitHubToken  = "MANDATUM_GITHUB_TOKEN"
	EnvGitHubSecret = "MANDATUM_GITHUB_WEBHOOK_SECRET"
	EnvGitHubOwners = "MANDATUM_GITHUB_OWNERS"
	EnvGitHubBots   = "MANDATUM_GITHUB_BOTS"

	// Jira. Sites is a comma-separated list of short-key=base-URL pairs; the
	// short key, not the hostname, is what appears in a jira:// address.
	EnvJiraSites  = "MANDATUM_JIRA_SITES"
	EnvJiraEmail  = "MANDATUM_JIRA_EMAIL"
	EnvJiraToken  = "MANDATUM_JIRA_TOKEN"
	EnvJiraSecret = "MANDATUM_JIRA_WEBHOOK_SECRET"
	EnvJiraBots   = "MANDATUM_JIRA_BOTS"

	// Webhook. A comma-separated list of workspace=base-url=secret triples.
	// The workspace is an indirection precisely so that a route written in an
	// agent spec cannot name an arbitrary URL on the internet.
	EnvWebhookEndpoints = "MANDATUM_WEBHOOK_ENDPOINTS"
)

// buildConnectors registers every connector this deployment is configured for.
//
// Roles are DERIVED, never declared: a connector that implements Deliver is a
// sink, one that implements Ingest is a trigger, and the registry works that
// out from the types. So this function only has to construct; nothing here says
// what any connector is for.
func buildConnectors(cfg coreConfig, store registry.Store) (*connector.Registry, error) {
	reg := connector.NewRegistry()
	// Agent names come from the control plane, which is the only place that
	// knows them. Without a name set, "@urmzd" and "@docs-bot" are the same
	// syntax on every one of these surfaces, and guessing would start a run
	// each time one human tagged another.
	agents := agentNames{store: store, tenant: cfg.Tenant}

	for _, build := range []func(coreConfig, mention.Set) (connector.Connector, error){
		newSlack, newGitHub, newJira, newWebhook, newCron,
	} {
		c, err := build(cfg, agents)
		if err != nil {
			return nil, err
		}
		if c == nil {
			continue // Not configured, which is a valid deployment.
		}
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("cli: register %s: %w", c.Name(), err)
		}
	}
	return reg, nil
}

func newSlack(cfg coreConfig, agents mention.Set) (connector.Connector, error) {
	token, secret := os.Getenv(EnvSlackToken), os.Getenv(EnvSlackSecret)
	if token == "" && secret == "" {
		return nil, nil
	}
	if token == "" || secret == "" {
		return nil, usagef("slack needs both $%s and $%s", EnvSlackToken, EnvSlackSecret)
	}
	c, err := slack.New(slack.Config{
		API:          slack.NewHTTPAPI(token, http.DefaultClient),
		Verifier:     signature.NewSlack(secret),
		Team:         os.Getenv(EnvSlackTeam),
		Bots:         pairs(os.Getenv(EnvSlackBots)),
		Agents:       agents,
		DefaultAgent: os.Getenv(EnvSlackDefault),
	})
	if err != nil {
		return nil, fmt.Errorf("cli: slack: %w", err)
	}
	return c, nil
}

func newGitHub(cfg coreConfig, agents mention.Set) (connector.Connector, error) {
	token, secret := os.Getenv(EnvGitHubToken), os.Getenv(EnvGitHubSecret)
	if token == "" && secret == "" {
		return nil, nil
	}
	if token == "" || secret == "" {
		return nil, usagef("github needs both $%s and $%s", EnvGitHubToken, EnvGitHubSecret)
	}
	c, err := github.New(github.Config{
		API:      github.NewHTTPAPI(token, http.DefaultClient),
		Verifier: signature.NewGitHub(secret),
		Owners:   list(os.Getenv(EnvGitHubOwners)),
		Bots:     pairs(os.Getenv(EnvGitHubBots)),
		Agents:   agents,
	})
	if err != nil {
		return nil, fmt.Errorf("cli: github: %w", err)
	}
	return c, nil
}

func newJira(cfg coreConfig, agents mention.Set) (connector.Connector, error) {
	spec := os.Getenv(EnvJiraSites)
	if spec == "" {
		return nil, nil
	}
	email, token := os.Getenv(EnvJiraEmail), os.Getenv(EnvJiraToken)
	if email == "" || token == "" {
		return nil, usagef("jira needs $%s and $%s", EnvJiraEmail, EnvJiraToken)
	}
	secret := os.Getenv(EnvJiraSecret)
	if secret == "" {
		return nil, usagef("jira needs $%s: Jira signs nothing, so a shared secret is the only thing standing between its webhook and the internet", EnvJiraSecret)
	}
	sites := map[string]jira.Site{}
	for key, base := range pairs(spec) {
		sites[key] = jira.Site{BaseURL: strings.TrimRight(base, "/"), Email: email, Token: token}
	}
	if len(sites) == 0 {
		return nil, usagef("$%s named no sites; want acme=https://acme.atlassian.net", EnvJiraSites)
	}
	api, err := jira.NewHTTPAPI(sites, http.DefaultClient)
	if err != nil {
		return nil, fmt.Errorf("cli: jira: %w", err)
	}
	c, err := jira.New(jira.Config{
		API:      api,
		Sites:    sites,
		Verifier: jiraSecret(secret),
		Bots:     pairs(os.Getenv(EnvJiraBots)),
		Agents:   agents,
	})
	if err != nil {
		return nil, fmt.Errorf("cli: jira: %w", err)
	}
	return c, nil
}

func newWebhook(cfg coreConfig, _ mention.Set) (connector.Connector, error) {
	spec := os.Getenv(EnvWebhookEndpoints)
	if spec == "" {
		return nil, nil
	}
	endpoints := map[string]webhook.Endpoint{}
	for _, entry := range list(spec) {
		name, rest, ok := strings.Cut(entry, "=")
		if !ok {
			return nil, usagef("$%s entry %q is not workspace=url=secret", EnvWebhookEndpoints, entry)
		}
		base, secret, ok := strings.Cut(rest, "=")
		if !ok || secret == "" {
			return nil, usagef("$%s entry %q has no secret, and a receiver that accepts unsigned events accepts them from anyone who learned the URL", EnvWebhookEndpoints, entry)
		}
		endpoints[name] = webhook.Endpoint{BaseURL: strings.TrimRight(base, "/"), Secret: secret}
	}
	c, err := webhook.New(webhook.Config{Endpoints: endpoints})
	if err != nil {
		return nil, fmt.Errorf("cli: webhook: %w", err)
	}
	return c, nil
}

func newCron(cfg coreConfig, _ mention.Set) (connector.Connector, error) {
	if len(cfg.Schedules) == 0 {
		return nil, nil
	}
	store, err := cron.NewMemory(cfg.Schedules...)
	if err != nil {
		return nil, fmt.Errorf("cli: cron: %w", err)
	}
	c, err := cron.New(cron.Config{Store: store, Log: cfg.Log})
	if err != nil {
		return nil, fmt.Errorf("cli: cron: %w", err)
	}
	return c, nil
}

// HeaderJiraSecret carries the shared secret a Jira webhook is registered with.
// Jira signs nothing, so the secret is whatever the person who configured the
// URL put in a header; see jiraSecret.
const HeaderJiraSecret = "X-Mandatum-Secret"

// jiraSecret authenticates a Jira webhook by a shared secret in a header.
//
// That is strictly weaker than a MAC — it does not bind the secret to the body,
// so anyone who captures one request can replay a different one — and it is all
// Jira offers. The mitigation is the same as everywhere else here: Tag.ID
// idempotency downstream, plus TLS. The comparison is constant time because
// this endpoint is public and can be probed as often as an attacker likes.
type jiraSecret string

func (s jiraSecret) Verify(h http.Header, _ []byte) error {
	got := h.Values(HeaderJiraSecret)
	if len(got) != 1 || subtle.ConstantTimeCompare([]byte(got[0]), []byte(s)) != 1 {
		return fmt.Errorf("%w: %s does not match the configured secret", signature.ErrSignatureMismatch, HeaderJiraSecret)
	}
	return nil
}

// agentNames answers "does this token name an agent" from the control plane.
//
// mention.Set has no context because the question is asked deep inside parsing
// a payload, where there is nothing to plumb one from and nothing to cancel: a
// lookup is a map read. The registry is the authority for what agents exist, so
// an agent that has not been created cannot be mentioned into existence, which
// is the property that keeps a mention of a colleague from starting a run.
type agentNames struct {
	store  registry.Store
	tenant string
}

func (a agentNames) Lookup(token string) (string, bool) {
	if !mention.Valid(token) {
		return "", false
	}
	rev, err := a.store.Get(context.Background(), a.tenant, token, 0)
	if err != nil {
		return "", false
	}
	return rev.Spec.Name, true
}

// pairs parses "k=v,k=v" into a map. Empty entries are skipped so that a
// trailing comma is not a configuration error.
func pairs(s string) map[string]string {
	out := map[string]string{}
	for _, entry := range list(s) {
		k, v, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// list splits a comma-separated environment value.
func list(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}
