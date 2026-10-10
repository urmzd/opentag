// Package jira is the Jira peer in the mesh: trigger, sink and actor.
//
// This is the connector where the product's claim gets tested. Slack and GitHub
// are where the conversation happens; Jira is where the work is tracked, so
// "tag an agent and the work gets done" means an issue that moves, gets a
// comment, and lands on somebody. That is the Actor face, and it is half the
// reason this package exists.
//
// # A Jira site is named, not addressed by hostname
//
// pkg/address requires a workspace to be subject-token safe, because a workspace
// becomes a token in the mesh delivery family. "acme.atlassian.net" is not: the
// dots would split it. So a deployment gives each Jira instance a SHORT KEY and
// the key is what travels:
//
//	Config.Sites = map[string]jira.Site{
//	    "acme": {BaseURL: "https://acme.atlassian.net", Email: ..., Token: ...},
//	}
//
//	jira://acme/PROJ-5          the address
//	https://acme.atlassian.net  never appears in one
//
// The key is a name this deployment chose, so it is stable across a migration to
// a different hostname and it is readable in a topic, a log line and a route.
// Inbound, the direction is reversed: a webhook payload does not know our key, so
// the issue's own REST URL is matched against the configured base URLs to
// recover it (see siteOf). A delivery from an instance that is not configured
// raises no tag, rather than being attributed to whichever site sorted first.
//
// # Authentication is a shared secret, and that is Jira's fault
//
// Slack and GitHub sign their webhooks. Jira does not: there is no MAC over the
// body and nothing to verify. The strongest thing available is a header the
// person registering the webhook adds by hand, compared in constant time
// (see internal/inbound.Secret). It is weaker than a signature in a specific way
// — it does not bind the secret to the body, so a captured request can be
// replayed with a different one — and the honest mitigation is the same as
// everywhere else here: TLS, plus Tag.ID idempotency downstream. Saying so is
// better than implying a MAC exists.
//
// # Three ways a tag is raised
//
// A comment naming an agent, an issue created naming one, and an issue ASSIGNED
// to one. The third has no message at all: the assignment is the request and the
// ticket is what the agent reads, which is the same structural gesture as
// requesting a review from a bot on GitHub, and the way a human hands over work
// without typing anything.
//
// # Sink
//
// One comment per run, edited as the run streams (see internal/sink). Rendered
// in Jira wiki markup, with citations as [text|uri] links, because that is what
// the surface reads.
//
// # Actor
//
// jira_transition, jira_comment and jira_assign. Each returns the address of what
// it changed, so the action.taken event can cite it.
package jira

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/internal/inbound"
	"github.com/urmzd/mandatum/pkg/connectors/internal/render"
	"github.com/urmzd/mandatum/pkg/connectors/internal/sink"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Name is the address scheme this connector owns.
const Name = "jira"

// MaxBodyChars is Jira's limit on a comment body. A longer body is rejected
// outright, so the renderer clamps to it rather than losing the comment.
const MaxBodyChars = 32767

// Verifier authenticates a raw request body against its headers.
//
// inbound.Secret satisfies it and is what a Jira deployment passes, since Jira
// signs nothing. The interface is declared here so a deployment that terminates
// authentication at a gateway, or fronts Jira with something that does sign, can
// supply its own.
type Verifier interface {
	Verify(header http.Header, body []byte) error
}

// Config is everything the Jira connector needs.
type Config struct {
	// API talks to Jira. Required. Use NewHTTPAPI in production and NewFake in
	// tests, examples and local runs.
	API API

	// Sites maps the short site key that appears in an address workspace to the
	// instance it stands for. Required: without it there is no way to turn
	// jira://acme/PROJ-5 into a URL, and no way to recognise which instance a
	// webhook came from.
	//
	// A route naming a site that is not here is refused with ErrUndeliverable,
	// which is what stops one tenant's agent spec from addressing another
	// tenant's Jira.
	Sites map[string]Site

	// Verifier authenticates inbound deliveries. Required for the trigger: a
	// connector with no verifier refuses every request rather than trusting it.
	Verifier Verifier

	// Bots maps an Atlassian account id to the agent it stands for. It is how an
	// assignment names an agent, and how a mention of the agent is recognised:
	// Jira mentions by account id, never by name.
	Bots map[string]string

	// Agents is the set of agent names a textual mention may name. Jira renders
	// a real mention as an account id, so an "@docs-bot" in a comment is literal
	// text — which is exactly the case for an agent that is not a Jira user, and
	// the shared @name grammar applies. Nil disables textual resolution.
	Agents mention.Set

	// Deliver overrides where a tag's events go. Empty answers on the issue the
	// tag came from.
	Deliver []envelope.Route

	// Interval coalesces comment edits. Zero means sink.DefaultInterval.
	Interval time.Duration

	// Clock is the time source for coalescing. Nil means time.Now.
	Clock func() time.Time

	// MaxBody bounds an inbound request body. Zero means inbound.DefaultMaxBody.
	MaxBody int64
}

// Connector is the Jira peer. It implements pkg/connector's Trigger, Sink and
// Actor faces, and net/http's Handler.
type Connector struct {
	cfg  Config
	bots map[string]string
	// hosts maps a lower-cased instance hostname back to its short key, which is
	// how an inbound delivery is attributed to a site.
	hosts map[string]string

	gate   inbound.Gate
	pipe   inbound.Pipe
	engine *sink.Engine
}

// Compile-time proof of the faces. RolesOf derives them from the type, so these
// assertions are the declaration.
var (
	_ connector.Trigger = (*Connector)(nil)
	_ connector.Sink    = (*Connector)(nil)
	_ connector.Actor   = (*Connector)(nil)
	_ http.Handler      = (*Connector)(nil)
)

// New returns a Jira connector.
func New(cfg Config) (*Connector, error) {
	if cfg.API == nil {
		return nil, fmt.Errorf("jira: config needs an API")
	}
	if len(cfg.Sites) == 0 {
		return nil, fmt.Errorf("jira: config needs at least one site, keyed by the short name that appears in an address")
	}

	c := &Connector{
		cfg:   cfg,
		bots:  make(map[string]string, len(cfg.Bots)),
		hosts: make(map[string]string, len(cfg.Sites)),
	}
	for key, site := range cfg.Sites {
		if err := address.ValidWorkspace(key); err != nil {
			return nil, fmt.Errorf("jira: site key %q cannot be an address workspace, which is why a site is keyed by a short name rather than by its hostname: %w", key, err)
		}
		host := hostOf(site.BaseURL)
		if host == "" {
			return nil, fmt.Errorf("jira: site %q has no usable base url (%q)", key, site.BaseURL)
		}
		if other, clash := c.hosts[host]; clash {
			return nil, fmt.Errorf("jira: sites %q and %q both point at %s, so an inbound delivery could not be attributed to either", other, key, host)
		}
		c.hosts[host] = key
	}
	for account, agent := range cfg.Bots {
		if !mention.Valid(agent) {
			return nil, fmt.Errorf("jira: bots[%q] = %q is not a usable agent name", account, agent)
		}
		c.bots[strings.ToLower(account)] = agent
	}

	c.gate = inbound.Gate{Verifier: cfg.Verifier, MaxBody: cfg.MaxBody}
	interval := cfg.Interval
	if interval == 0 {
		interval = sink.DefaultInterval
	}
	c.engine = sink.New(surface{c: c}, c.body, sink.WithInterval(interval), sink.WithClock(cfg.Clock))
	return c, nil
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return Name }

// Ingest implements connector.Trigger. It parks until ctx is cancelled while
// ServeHTTP feeds out.
func (c *Connector) Ingest(ctx context.Context, out chan<- envelope.Tag) error {
	return c.pipe.Serve(ctx, out)
}

// Ingesting reports whether an Ingest is running, so a server can answer a
// readiness probe honestly.
func (c *Connector) Ingesting() bool { return c.pipe.Ingesting() }

// ServeHTTP handles one webhook delivery.
//
// Authenticate, translate, hand off, in that order. Nothing reads the payload
// before the gate has accepted it — see internal/inbound for why that ordering
// is the security property and not a style preference.
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, ok := c.gate.Body(w, r)
	if !ok {
		return
	}

	var h hook
	if err := json.Unmarshal(body, &h); err != nil {
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}

	tag, ok := c.tagFrom(r.Header, h)
	if !ok {
		// An authentic delivery that names no agent. 200, so Jira stops
		// retrying something that is not an error.
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := c.pipe.Send(r.Context(), tag); err != nil {
		http.Error(w, "not ready", inbound.SendStatus(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// agents is the set a mention or an assignee account is resolved against: the
// configured bot accounts first, then the agent name set.
func (c *Connector) agents() mention.Set { return agentSet{bots: c.bots, agents: c.cfg.Agents} }

type agentSet struct {
	bots   map[string]string
	agents mention.Set
}

// Lookup resolves an account id or a mention token to an agent.
func (s agentSet) Lookup(token string) (string, bool) {
	if agent, ok := s.bots[strings.ToLower(token)]; ok {
		return agent, true
	}
	if s.agents != nil {
		return s.agents.Lookup(token)
	}
	return "", false
}

// Deliver implements connector.Sink.
func (c *Connector) Deliver(ctx context.Context, target address.Address, e envelope.Event) error {
	if _, err := c.ref(target); err != nil {
		return err
	}
	return c.engine.Deliver(ctx, target, e)
}

// Flush writes every run whose comment edits are still being coalesced.
func (c *Connector) Flush(ctx context.Context) error { return c.engine.Flush(ctx) }

// ref reads a jira address: jira://<site key>/<ISSUE-KEY>.
func (c *Connector) ref(target address.Address) (Ref, error) {
	if target.Connector != Name {
		return Ref{}, fmt.Errorf("%w: %s is not a jira address", connector.ErrUndeliverable, target)
	}
	if _, ok := c.cfg.Sites[target.Workspace]; !ok {
		return Ref{}, fmt.Errorf("%w: %s names site %q, which this connector does not serve",
			connector.ErrUndeliverable, target, target.Workspace)
	}
	if len(target.Path) != 1 {
		return Ref{}, fmt.Errorf("%w: %s is not jira://<site>/<ISSUE-KEY>", connector.ErrUndeliverable, target)
	}
	if !ValidKey(target.Path[0]) {
		return Ref{}, fmt.Errorf("%w: %s does not end in an issue key such as PROJ-5",
			connector.ErrUndeliverable, target)
	}
	return Ref{Site: target.Workspace, Issue: target.Path[0]}, nil
}

// Address is the address of an issue on a site. It is the inverse of ref, and
// the one place an address is composed, so an action's Result and a trigger's
// Source cannot disagree about what an issue is called.
func Address(ref Ref) address.Address {
	return address.Address{Connector: Name, Workspace: ref.Site, Path: []string{ref.Issue}}
}

// surface adapts the Jira API to the sink engine. The handle is the comment id,
// which Jira gives out as a string.
type surface struct{ c *Connector }

func (s surface) Create(ctx context.Context, target address.Address, body string) (string, error) {
	ref, err := s.c.ref(target)
	if err != nil {
		return "", err
	}
	return s.c.cfg.API.CreateComment(ctx, ref, body)
}

func (s surface) Update(ctx context.Context, target address.Address, handle, body string) error {
	ref, err := s.c.ref(target)
	if err != nil {
		return err
	}
	return s.c.cfg.API.UpdateComment(ctx, ref, handle, body)
}

// body renders a run as a Jira comment.
//
// Unlike GitHub, the model's own text is not passed through untouched: Jira does
// not read Markdown, and the two constructs that would otherwise swallow the
// rest of a comment — a brace opening a macro and a bracket opening a link — are
// neutralized. See escape in wiki.go for what is deliberately left alone.
func (c *Connector) body(v render.View) string {
	return sink.Clamp(sink.Layout(wiki{}, sink.MapText(v, escape)), MaxBodyChars)
}
