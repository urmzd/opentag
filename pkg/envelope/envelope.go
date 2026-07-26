// Package envelope is the contract every opentag client speaks, independent
// of transport. gRPC, SSE, and the in-process path all carry these types.
//
// Two directions:
//
//	Tag    a request to an agent, raised by a trigger
//	Event  something an agent produced, delivered to sinks and observers
//
// The asymmetry between triggers and sinks is deliberate. A trigger is
// something that can raise a tag (a Slack mention, a GitHub review request, a
// cron schedule). A sink is something an event can be delivered to (a Slack
// thread, a GitHub comment, an outbound webhook). Most connectors are both,
// but not all: cron only triggers, and an outbound webhook is only a sink.
// Because the two roles are independent, a tag raised in GitHub can deliver
// into Jira, and a schedule with no origin surface at all can deliver into
// both.
//
// Delivery is N:M and selective. One tag fans out to many sinks; one sink
// serves many agents. Each route chooses its own event kinds, so the GitHub
// connector can stream every delta into a comment it keeps editing while a
// separate webhook receives only the terminal event and learns nothing about
// the intermediate steps.
package envelope

import (
	"errors"
	"fmt"
	"time"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/topic"
)

// ErrInvalid reports a malformed envelope value. Callers match with errors.Is.
var ErrInvalid = errors.New("envelope: invalid")

// Kind classifies an Event. Kinds are dotted so that a filter can select a
// whole family ("delta") or one member ("delta.citation"). Every kind travels
// on the same topic and in the same stream; kind is a field, never an address.
type Kind string

// Event kinds.
const (
	// Streaming output from the agent as it works.
	KindText     Kind = "delta.text"
	KindThinking Kind = "delta.thinking"
	KindToolCall Kind = "delta.tool.call"
	KindToolDone Kind = "delta.tool.done"
	KindCitation Kind = "delta.citation"

	// Run lifecycle. A sink that only wants to know the work landed
	// subscribes to lifecycle.completed and nothing else.
	KindAccepted  Kind = "lifecycle.accepted"
	KindStarted   Kind = "lifecycle.started"
	KindParked    Kind = "lifecycle.parked"
	KindResumed   Kind = "lifecycle.resumed"
	KindCompleted Kind = "lifecycle.completed"
	KindFailed    Kind = "lifecycle.failed"

	// Actions the agent took on a connector's surface: a comment posted, a
	// ticket transitioned. This is the "work got done" signal, distinct from
	// "the agent said something".
	KindActionTaken Kind = "action.taken"

	// Control-plane changes, on the same bus as the data so a dashboard sees
	// an agent get revised in the same stream as the runs that follow it.
	KindAgentCreated Kind = "control.created"
	KindAgentRevised Kind = "control.revised"
	KindAgentDeleted Kind = "control.deleted"
)

// Family returns the leading component of a kind: "delta", "lifecycle",
// "action", or "control".
func (k Kind) Family() string {
	for i := 0; i < len(k); i++ {
		if k[i] == '.' {
			return string(k[:i])
		}
	}
	return string(k)
}

// Matches reports whether k is selected by sel. A selector matches either the
// exact kind or its family, so "delta" selects every delta kind and
// "delta.citation" selects only citations.
func (k Kind) Matches(sel string) bool {
	if sel == "" || sel == string(k) {
		return true
	}
	return sel == k.Family()
}

// Actor is the human or system that raised a tag.
type Actor struct {
	// ID is the connector-native identifier (a Slack user ID, a GitHub
	// login). Stable within a workspace.
	ID string `json:"id"`
	// Display is a human-readable name, best effort.
	Display string `json:"display,omitempty"`
	// Bot reports whether the actor is automation rather than a person.
	Bot bool `json:"bot,omitempty"`
}

// Route is one delivery instruction: where to send events, and which ones.
//
// Kinds is a set of selectors matched against Event.Kind. An empty Kinds
// means every kind. This is what makes a single run serve both a connector
// rendering a live stream and a webhook that only wants the outcome.
type Route struct {
	Target address.Address `json:"target"`
	Kinds  []string        `json:"kinds,omitempty"`
}

// Wants reports whether an event of kind k should be delivered on this route.
func (r Route) Wants(k Kind) bool {
	if len(r.Kinds) == 0 {
		return true
	}
	for _, sel := range r.Kinds {
		if k.Matches(sel) {
			return true
		}
	}
	return false
}

// Validate checks that the route names a usable target.
func (r Route) Validate() error {
	if r.Target.IsZero() {
		return fmt.Errorf("%w: route has no target", ErrInvalid)
	}
	return nil
}

// Tag is a request to an agent. It is the canonical form every trigger
// translates into, and the only thing the core accepts as work.
type Tag struct {
	// ID is the trigger-native event identifier: a Slack event_id, a GitHub
	// delivery GUID, a schedule occurrence. It becomes the run's idempotency
	// key, so a webhook redelivery joins the existing run instead of starting
	// a second one.
	ID string `json:"id"`

	// Tenant scopes the tag. It is set by the server from the caller's
	// credential and is not trusted from the wire.
	Tenant string `json:"tenant"`

	// Agent is the name that was tagged.
	Agent string `json:"agent"`

	// Origin is the trigger that raised this tag ("slack", "github", "cron").
	Origin string `json:"origin"`

	// Source is where the tag came from, and the natural place to answer if
	// no route says otherwise. Zero for triggers with no addressable surface,
	// such as a schedule.
	Source address.Address `json:"source,omitempty"`

	// Text is the request, with the agent mention already stripped.
	Text string `json:"text"`

	// Actor raised the tag.
	Actor Actor `json:"actor,omitempty"`

	// Deliver lists every destination for this run's events. Empty means
	// answer in place, at Source.
	Deliver []Route `json:"deliver,omitempty"`

	// Meta carries trigger-specific context the agent's tools may need: a
	// pull request number, an issue title, a Jira status.
	Meta map[string]string `json:"meta,omitempty"`

	// At is when the trigger observed the event.
	At time.Time `json:"at"`
}

// Validate checks the fields the core requires to start a run.
func (t Tag) Validate() error {
	switch {
	case t.ID == "":
		return fmt.Errorf("%w: tag has no id, which is its idempotency key", ErrInvalid)
	case t.Agent == "":
		return fmt.Errorf("%w: tag names no agent", ErrInvalid)
	case t.Origin == "":
		return fmt.Errorf("%w: tag has no origin", ErrInvalid)
	}
	for i, r := range t.Deliver {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("%w (route %d)", err, i)
		}
	}
	return nil
}

// Routes returns the tag's delivery routes, falling back to answering in
// place at Source when none were given. A tag with neither routes nor a
// source produces no routes: the run still executes and still publishes to
// the bus, it simply has nowhere to be rendered.
func (t Tag) Routes() []Route {
	if len(t.Deliver) > 0 {
		return t.Deliver
	}
	if t.Source.IsZero() {
		return nil
	}
	return []Route{{Target: t.Source}}
}

// Event is one thing an agent produced. Every event, of every kind, travels
// the same path: published to its run's topic, seen by every prefix
// subscriber, and delivered to whichever routes want its kind.
type Event struct {
	// Seq orders events within a run, starting at 1. It is the resume
	// cursor: a consumer that reconnects asks for everything after the last
	// Seq it processed.
	Seq uint64 `json:"seq"`

	// Topic is where this event was published: always the most specific
	// form, agent:<name>:<run>.
	Topic topic.Topic `json:"topic"`

	// RunID is the durable run that produced the event.
	RunID string `json:"run_id"`

	// Tenant scopes the event for authorization on the read path.
	Tenant string `json:"tenant"`

	// Agent and Rev identify exactly what produced this event. Rev is the
	// pinned revision, so a consumer comparing two revisions of an agent
	// filters on this field rather than subscribing to two topics.
	Agent string `json:"agent"`
	Rev   int    `json:"rev"`

	// Origin is the trigger that raised the run.
	Origin string `json:"origin"`

	Kind Kind `json:"kind"`

	// Payload is the kind-specific body.
	Payload []byte `json:"payload,omitempty"`

	At time.Time `json:"at"`
}

// Filter narrows a subscription without changing the topic it listens to.
// The zero Filter selects everything on the topic.
//
// Filtering rather than topic-splitting is what keeps the namespace at
// "agent:<name>": a consumer watching a canary revision and a consumer
// watching every citation are both listening to the same topic, and neither
// has to discover a topic name that only exists once some agent has been
// revised that many times.
type Filter struct {
	// Kinds selects event kinds by exact match or family. Empty means all.
	Kinds []string `json:"kinds,omitempty"`
	// Rev selects one pinned agent revision. Zero means any.
	Rev int `json:"rev,omitempty"`
	// Origin selects the trigger that raised the run. Empty means any.
	Origin string `json:"origin,omitempty"`
	// RunID selects a single run. Empty means any. Subscribing to
	// agent:<name>:<run> is usually clearer, but this lets a consumer hold
	// one agent-level subscription and narrow it dynamically.
	RunID string `json:"run_id,omitempty"`
}

// Allows reports whether e passes the filter.
func (f Filter) Allows(e Event) bool {
	if f.Rev != 0 && e.Rev != f.Rev {
		return false
	}
	if f.Origin != "" && e.Origin != f.Origin {
		return false
	}
	if f.RunID != "" && e.RunID != f.RunID {
		return false
	}
	if len(f.Kinds) == 0 {
		return true
	}
	for _, sel := range f.Kinds {
		if e.Kind.Matches(sel) {
			return true
		}
	}
	return false
}

// Subscription is a topic plus a filter plus a resume position: everything
// the bus needs to serve one consumer.
type Subscription struct {
	Topic  topic.Topic `json:"topic"`
	Filter Filter      `json:"filter,omitempty"`
	// From resumes after this sequence number within each run. Zero starts
	// at the beginning of what the bus still holds.
	From uint64 `json:"from,omitempty"`
}

// Accepts reports whether this subscription should receive e.
func (s Subscription) Accepts(e Event) bool {
	if !s.Topic.Covers(e.Topic) {
		return false
	}
	if e.Seq <= s.From {
		return false
	}
	return s.Filter.Allows(e)
}
