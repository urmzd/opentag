// Package slack is the Slack peer in the mesh: trigger, sink and actor.
//
// # Trigger
//
// Slack delivers events over a signed HTTP POST, so the connector is both an
// http.Handler and a pkg/connector.Trigger: Ingest holds the channel and parks,
// and each request turns into at most one Tag. Nothing is parsed before the
// signature is checked (see internal/inbound), and nothing downstream re-checks,
// because nothing downstream can.
//
// Two event types raise tags: app_mention, which is a mention in a channel, and
// a direct message, which is a mention by context rather than by text. Everything
// else is acknowledged and dropped. Messages from a bot are always dropped: a
// mesh whose sink posts into the same channel its trigger reads would otherwise
// answer itself, forever, at whatever rate the model can produce tokens.
//
// # Which agent was tagged
//
// A Slack mention is <@U08BOT> — an opaque id, not a name — so the connector
// cannot read the agent's name out of the text the way GitHub can. Resolution is
// in three documented steps (see Config.Bots, Config.Agents, Config.DefaultAgent),
// and text-based resolution is off unless Config.Agents is set, because with an
// open name set the mention of a colleague would look exactly like the mention of
// an agent.
//
// # Sink
//
// One message per (run, thread), edited as the run streams, coalesced to respect
// chat.update's rate limit. That behaviour and its rationale live in
// internal/sink; this package supplies the Slack surface and mrkdwn rendering.
//
// The answer threads under the mention by default: Tag.Source carries the
// mention's own timestamp as ?thread=, so a channel that tags an agent fifty
// times has fifty threads rather than one flat conversation.
//
// # Actor
//
// slack_post_message and slack_add_reaction. Both are ordinary Slack verbs, and
// both report an address so the resulting action.taken event can cite the message
// it created.
package slack

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
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
const Name = "slack"

// MaxBodyChars is Slack's limit on a message body. A longer body is rejected
// outright, so the renderer clamps to it rather than losing the message.
const MaxBodyChars = 4000

// Verifier authenticates a raw request body against its headers.
//
// *signature.Slack satisfies it, and is what a deployment should pass. The
// interface is declared here rather than imported so that the connector depends
// on the check rather than on one implementation of it, and so a deployment
// terminating Slack's signature at a gateway can supply its own.
type Verifier interface {
	Verify(header http.Header, body []byte) error
}

// Config is everything the Slack connector needs. The zero value is not usable:
// New validates.
type Config struct {
	// API talks to Slack. Required. Use NewHTTPAPI in production and NewFake in
	// tests, examples and local runs.
	API API

	// Verifier authenticates inbound requests. Required for the trigger: a
	// connector with no verifier refuses every request rather than trusting it.
	Verifier Verifier

	// Team is the Slack team id this connector serves ("T01ABCDEF"). When set,
	// a delivery to any other workspace is refused with ErrUndeliverable, which
	// is what stops a route in one tenant's agent spec from addressing another
	// tenant's Slack. Empty accepts every workspace, which is only correct when
	// API dispatches on Message.Workspace itself.
	Team string

	// Bots maps a Slack user id to the agent it stands for: "U08BOT" ->
	// "docs-bot". This is the primary way an agent is named, because it is the
	// only one that works with Slack's mention encoding. An app that fronts one
	// agent maps its own bot user id to that agent.
	Bots map[string]string

	// Agents enables naming an agent in the text alongside the app's own
	// mention: "@mandatum @docs-bot summarize". Slack leaves "@docs-bot" as
	// literal text when no such Slack user exists, which is exactly the case
	// for an agent, so the shared @name grammar applies.
	//
	// Nil disables it entirely: without a set of known agent names, "@urmzd"
	// and "@docs-bot" are indistinguishable, and guessing would start a run
	// every time someone mentioned a colleague.
	Agents mention.Set

	// DefaultAgent answers a mention that named no agent. Empty means such a
	// mention raises no tag.
	DefaultAgent string

	// Deliver overrides where a tag's events go. Empty answers in the thread the
	// mention came from, which is what a human expects.
	Deliver []envelope.Route

	// Interval coalesces edits. Zero means sink.DefaultInterval.
	Interval time.Duration

	// Clock is the time source for coalescing. Nil means time.Now.
	Clock func() time.Time

	// MaxBody bounds an inbound request body. Zero means inbound.DefaultMaxBody.
	MaxBody int64
}

// Connector is the Slack peer. It implements pkg/connector's Trigger, Sink and
// Actor faces, and net/http's Handler.
type Connector struct {
	cfg    Config
	gate   inbound.Gate
	pipe   inbound.Pipe
	engine *sink.Engine
}

// Compile-time proof of the faces this connector claims. RolesOf derives them
// from the type, so these assertions are the declaration.
var (
	_ connector.Trigger = (*Connector)(nil)
	_ connector.Sink    = (*Connector)(nil)
	_ connector.Actor   = (*Connector)(nil)
	_ http.Handler      = (*Connector)(nil)
)

// New returns a Slack connector.
func New(cfg Config) (*Connector, error) {
	if cfg.API == nil {
		return nil, fmt.Errorf("slack: config needs an API")
	}
	if cfg.Team != "" {
		if err := address.ValidWorkspace(cfg.Team); err != nil {
			return nil, fmt.Errorf("slack: team %q cannot be an address workspace: %w", cfg.Team, err)
		}
	}
	for id, agent := range cfg.Bots {
		if !mention.Valid(agent) {
			return nil, fmt.Errorf("slack: bots[%q] = %q is not a usable agent name", id, agent)
		}
	}
	if cfg.DefaultAgent != "" && !mention.Valid(cfg.DefaultAgent) {
		return nil, fmt.Errorf("slack: default agent %q is not a usable agent name", cfg.DefaultAgent)
	}

	c := &Connector{cfg: cfg}
	c.gate = inbound.Gate{Verifier: cfg.Verifier, MaxBody: cfg.MaxBody}
	c.engine = sink.New(surface{c: c}, c.body,
		sink.WithInterval(intervalOr(cfg.Interval)),
		sink.WithClock(cfg.Clock),
	)
	return c, nil
}

func intervalOr(d time.Duration) time.Duration {
	if d == 0 {
		return sink.DefaultInterval
	}
	return d
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return Name }

// Ingest implements connector.Trigger. It parks until ctx is cancelled while
// ServeHTTP feeds out.
func (c *Connector) Ingest(ctx context.Context, out chan<- envelope.Tag) error {
	return c.pipe.Serve(ctx, out)
}

// Ingesting reports whether an Ingest is running and this connector can
// therefore accept events. A server answers its readiness probe with it: a
// webhook endpoint that is listening but has nowhere to send tags is not ready,
// and saying so keeps a load balancer from sending it traffic it will 503.
func (c *Connector) Ingesting() bool { return c.pipe.Ingesting() }

// ServeHTTP handles one Slack Events API request.
//
// Slack expects an answer within three seconds and retries otherwise, so the
// handler does exactly three things: authenticate, translate, hand off. The
// handoff blocks on the router's channel, bounded by the request context, which
// is back pressure rather than a queue we would then have to make durable.
func (c *Connector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, ok := c.gate.Body(w, r)
	if !ok {
		return
	}

	var env eventEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		http.Error(w, "malformed payload", http.StatusBadRequest)
		return
	}

	// url_verification is how Slack proves the endpoint is ours when it is
	// configured. It is signed like every other request, so it is answered only
	// after the gate accepted it.
	if env.Type == "url_verification" {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"challenge": env.Challenge})
		return
	}

	tag, ok := c.tagFrom(env)
	if !ok {
		// Not for us: acknowledge, or Slack retries it three more times.
		w.WriteHeader(http.StatusOK)
		return
	}
	if err := c.pipe.Send(r.Context(), tag); err != nil {
		http.Error(w, "not ready", inbound.SendStatus(err))
		return
	}
	w.WriteHeader(http.StatusOK)
}

// Deliver implements connector.Sink.
func (c *Connector) Deliver(ctx context.Context, target address.Address, e envelope.Event) error {
	if err := c.reachable(target); err != nil {
		return err
	}
	return c.engine.Deliver(ctx, target, e)
}

// Flush writes every run whose edits are still being coalesced. A server calls
// it on its own ticker and at shutdown; see internal/sink for why the connector
// owns no goroutine of its own.
func (c *Connector) Flush(ctx context.Context) error { return c.engine.Flush(ctx) }

// reachable reports whether this connector can deliver to target.
func (c *Connector) reachable(target address.Address) error {
	if target.Connector != Name {
		return fmt.Errorf("%w: %s is not a slack address", connector.ErrUndeliverable, target)
	}
	if c.cfg.Team != "" && target.Workspace != c.cfg.Team {
		return fmt.Errorf("%w: %s is in workspace %q, this connector serves %q",
			connector.ErrUndeliverable, target, target.Workspace, c.cfg.Team)
	}
	if len(target.Path) != 1 || target.Path[0] == "" {
		return fmt.Errorf("%w: %s does not name a channel (want slack://<team>/<channel>)",
			connector.ErrUndeliverable, target)
	}
	return nil
}

// surface adapts the Slack API to the sink engine.
type surface struct{ c *Connector }

// Create posts the one message this run owns, in the thread the tag came from.
func (s surface) Create(ctx context.Context, target address.Address, body string) (string, error) {
	thread, _ := target.Param("thread")
	return s.c.cfg.API.PostMessage(ctx, Message{
		Workspace: target.Workspace,
		Channel:   target.Path[0],
		ThreadTS:  thread,
		Text:      body,
	})
}

// Update edits it.
func (s surface) Update(ctx context.Context, target address.Address, handle, body string) error {
	return s.c.cfg.API.UpdateMessage(ctx, Message{
		Workspace: target.Workspace,
		Channel:   target.Path[0],
		TS:        handle,
		Text:      body,
	})
}

// body renders a run for Slack.
func (c *Connector) body(v render.View) string {
	return sink.Clamp(sink.Layout(mrkdwn{}, sink.MapText(v, escape)), MaxBodyChars)
}

// Actions implements connector.Actor.
func (c *Connector) Actions() []connector.Action {
	return []connector.Action{
		{
			Name: "slack_post_message",
			Description: "Post a message to a Slack channel or thread. Use this to " +
				"tell someone something outside the thread the agent is answering in; " +
				"the answer itself is already delivered to the thread and does not " +
				"need to be posted.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "text": {"type": "string", "description": "Message body, Slack mrkdwn."},
    "channel": {"type": "string", "description": "Channel id. Defaults to the channel the run is answering in."},
    "thread_ts": {"type": "string", "description": "Timestamp of the message to reply under. Omit to post to the channel."}
  },
  "required": ["text"],
  "additionalProperties": false
}`),
			Invoke: c.postMessage,
		},
		{
			Name: "slack_add_reaction",
			Description: "Add an emoji reaction to a Slack message. Use it to " +
				"acknowledge without adding noise to the channel.",
			Schema: json.RawMessage(`{
  "type": "object",
  "properties": {
    "name": {"type": "string", "description": "Emoji name without colons, such as eyes or white_check_mark."},
    "channel": {"type": "string", "description": "Channel id. Defaults to the channel the run is answering in."},
    "ts": {"type": "string", "description": "Timestamp of the message to react to. Defaults to the message that raised the run."}
  },
  "required": ["name"],
  "additionalProperties": false
}`),
			Invoke: c.addReaction,
		},
	}
}

func (c *Connector) postMessage(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Text     string `json:"text"`
		Channel  string `json:"channel"`
		ThreadTS string `json:"thread_ts"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("slack: slack_post_message args: %w", err)
	}
	if args.Text == "" {
		return connector.Result{}, fmt.Errorf("slack: slack_post_message needs text")
	}
	channel, workspace, err := c.resolve(target, args.Channel)
	if err != nil {
		return connector.Result{}, err
	}
	if args.ThreadTS == "" {
		args.ThreadTS, _ = target.Param("thread")
	}

	ts, err := c.cfg.API.PostMessage(ctx, Message{
		Workspace: workspace,
		Channel:   channel,
		ThreadTS:  args.ThreadTS,
		Text:      args.Text,
	})
	if err != nil {
		return connector.Result{}, err
	}
	posted := address.Address{Connector: Name, Workspace: workspace, Path: []string{channel}}.WithParam("thread", ts)
	return connector.Result{
		Summary: fmt.Sprintf("posted a message in %s", channel),
		Address: posted,
	}, nil
}

func (c *Connector) addReaction(ctx context.Context, target address.Address, raw json.RawMessage) (connector.Result, error) {
	var args struct {
		Name    string `json:"name"`
		Channel string `json:"channel"`
		TS      string `json:"ts"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return connector.Result{}, fmt.Errorf("slack: slack_add_reaction args: %w", err)
	}
	if args.Name == "" {
		return connector.Result{}, fmt.Errorf("slack: slack_add_reaction needs an emoji name")
	}
	channel, workspace, err := c.resolve(target, args.Channel)
	if err != nil {
		return connector.Result{}, err
	}
	if args.TS == "" {
		args.TS, _ = target.Param("thread")
	}
	if args.TS == "" {
		return connector.Result{}, fmt.Errorf("slack: slack_add_reaction needs the ts of a message")
	}

	if err := c.cfg.API.AddReaction(ctx, Reaction{
		Workspace: workspace,
		Channel:   channel,
		TS:        args.TS,
		Name:      args.Name,
	}); err != nil {
		return connector.Result{}, err
	}
	reacted := address.Address{Connector: Name, Workspace: workspace, Path: []string{channel}}.WithParam("thread", args.TS)
	return connector.Result{
		Summary: fmt.Sprintf("reacted :%s: in %s", args.Name, channel),
		Address: reacted,
	}, nil
}

// resolve settles which channel and workspace an action acts on.
//
// An action defaults to the surface the run is answering on, and may name
// another channel — but never another workspace. Letting an argument choose the
// workspace would let a prompt reach across the tenant boundary the route
// established, which is exactly the boundary an agent's own output must not be
// able to move.
func (c *Connector) resolve(target address.Address, channel string) (string, string, error) {
	if err := c.reachable(target); err != nil {
		return "", "", err
	}
	if channel == "" {
		channel = target.Path[0]
	}
	return channel, target.Workspace, nil
}
