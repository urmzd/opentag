// Package topic is the addressing scheme of the opentag bus.
//
// The mental model is one line: users listen to an agent.
//
//	agent:docs-bot            every event the agent emits, across every run
//	agent:docs-bot:run_01J    one run of that agent
//	agent                     every agent (an audit or dashboard consumer)
//
// Topics are colon-delimited segments, matched by prefix. Publishing always
// targets the most specific topic; subscribing to any prefix of it receives
// the event. There is no wildcard language, because prefix containment
// already expresses every subscription the mesh needs and it cannot be got
// wrong.
//
// Everything else that could have been a topic token is deliberately not one:
//
//   - Event kind is a field, not a segment. All events stream the same way;
//     a consumer that wants only citations filters for them (see Filter).
//     Splitting kinds across topics would force a consumer wanting the whole
//     conversation to fan in from several topics and re-order them.
//   - Agent revision is a field. Canary observation is a filter on one topic,
//     not a second topic that appears and disappears as agents are revised.
//   - Tenant is an authorization scope carried by the subscriber's
//     credential, not an addressing concern. A subscriber never names its own
//     tenant; the broker scopes it.
//   - Destination is a route, not a topic. Where output is delivered is
//     decided per run (see pkg/envelope), so the same agent can answer into
//     Slack for one tag and into GitHub for the next without either
//     destination existing in the topic namespace.
//
// This package is a leaf: stdlib only.
package topic

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Separator divides topic segments.
const Separator = ":"

// Root is the top of the namespace. Subscribing here receives every agent's
// events, subject to the subscriber's tenant scope.
const Root = "agent"

// ErrInvalid reports a malformed topic. Callers match with errors.Is.
var ErrInvalid = errors.New("topic: invalid")

// Topic is a hierarchical address: "agent", "agent:<name>", or
// "agent:<name>:<runID>". The zero value is unset.
type Topic struct {
	segments []string
}

// Parse validates s as a topic. It must begin with the root segment and carry
// at most a name and a run id beneath it.
func Parse(s string) (Topic, error) {
	if s == "" {
		return Topic{}, fmt.Errorf("%w: empty topic", ErrInvalid)
	}
	segments := strings.Split(s, Separator)
	if segments[0] != Root {
		return Topic{}, fmt.Errorf("%w: %q must begin with %q", ErrInvalid, s, Root)
	}
	if len(segments) > 3 {
		return Topic{}, fmt.Errorf("%w: %q has %d segments, the namespace is at most %s:<name>:<run>", ErrInvalid, s, len(segments), Root)
	}
	for i, seg := range segments {
		if seg == "" {
			return Topic{}, fmt.Errorf("%w: empty segment at position %d in %q", ErrInvalid, i, s)
		}
		if err := validSegment(seg, s, i); err != nil {
			return Topic{}, err
		}
	}
	return Topic{segments: segments}, nil
}

// MustParse is Parse for constants and tests. It panics on a malformed topic.
func MustParse(s string) Topic {
	t, err := Parse(s)
	if err != nil {
		panic(err)
	}
	return t
}

// All is the root topic: every agent, every run.
func All() Topic { return Topic{segments: []string{Root}} }

// Agent is the topic a user listens to: every run of one agent.
func Agent(name string) (Topic, error) {
	if err := oneSegment("agent name", name); err != nil {
		return Topic{}, err
	}
	return Parse(Root + Separator + name)
}

// Run is the most specific topic: one run of one agent. Events are always
// published here, so that every prefix subscriber sees them.
func Run(agent, runID string) (Topic, error) {
	if err := oneSegment("agent name", agent); err != nil {
		return Topic{}, err
	}
	if err := oneSegment("run id", runID); err != nil {
		return Topic{}, err
	}
	return Parse(Root + Separator + agent + Separator + runID)
}

// oneSegment rejects an argument that is not exactly one segment. Parse splits
// on the separator before it validates a charset, so without this check a name
// carrying a separator would be silently promoted to a deeper topic: Agent
// ("docs:bot") would return the run topic agent:docs:bot, whose Name is "docs"
// and which covers nothing the caller asked for.
func oneSegment(what, s string) error {
	if s == "" {
		return fmt.Errorf("%w: empty %s", ErrInvalid, what)
	}
	if strings.Contains(s, Separator) {
		return fmt.Errorf("%w: %s %q contains %q and is not one segment", ErrInvalid, what, s, Separator)
	}
	return nil
}

// String returns the colon-delimited form.
func (t Topic) String() string { return strings.Join(t.segments, Separator) }

// IsZero reports whether t is unset.
func (t Topic) IsZero() bool { return len(t.segments) == 0 }

// Name returns the agent name, or "" for the root topic.
func (t Topic) Name() string {
	if len(t.segments) < 2 {
		return ""
	}
	return t.segments[1]
}

// RunID returns the run id, or "" if the topic is not run-scoped.
func (t Topic) RunID() string {
	if len(t.segments) < 3 {
		return ""
	}
	return t.segments[2]
}

// MarshalJSON renders the topic as its string form.
//
// A Topic keeps its segments unexported so that the only way to hold one is to
// have parsed it. That would otherwise make it encode as an empty object and
// decode as the zero value, which is a silent failure: a Subscription whose
// topic came back zero accepts nothing, and an Event whose topic came back zero
// is covered by no subscription at all. Every type in pkg/envelope carries a
// json tag for this field, so the representation belongs here rather than in a
// shadow struct in each transport.
func (t Topic) MarshalJSON() ([]byte, error) { return json.Marshal(t.String()) }

// UnmarshalJSON parses the string form, validating it exactly as Parse does. An
// empty string decodes to the zero Topic, which is what the unset field means.
func (t *Topic) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("topic: decode: %w", err)
	}
	if s == "" {
		*t = Topic{}
		return nil
	}
	parsed, err := Parse(s)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}

// Covers reports whether a subscription to t receives events published to
// other. Containment is by segment prefix, so "agent:docs-bot" covers
// "agent:docs-bot:run_01J", and every topic covers itself.
//
// Prefix matching is segment-wise rather than string-wise on purpose:
// "agent:docs" must not cover "agent:docs-bot".
func (t Topic) Covers(other Topic) bool {
	if t.IsZero() || other.IsZero() {
		return false
	}
	if len(t.segments) > len(other.segments) {
		return false
	}
	for i, seg := range t.segments {
		if seg != other.segments[i] {
			return false
		}
	}
	return true
}

// validSegment enforces the segment charset. Anything that is not a letter,
// digit, "-", or "_" is rejected so a segment can never be confused with a
// separator and topics stay safe to embed in stream keys and URLs unescaped.
func validSegment(seg, whole string, pos int) error {
	for _, r := range seg {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_':
		default:
			return fmt.Errorf("%w: illegal character %q in segment %q at position %d of %q", ErrInvalid, r, seg, pos, whole)
		}
	}
	return nil
}
