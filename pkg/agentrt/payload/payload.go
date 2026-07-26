// Package payload is the body of every event an agent run publishes.
//
// envelope.Event carries an opaque []byte and a Kind that says how to read it.
// This package is the "how": one struct per kind, with json tags, and the
// helpers to get in and out of Event.Payload. It exists as its own package for
// one reason — a connector rendering a Slack message, a CLI printing a stream,
// or a browser client decoding SSE needs to understand a payload, and none of
// them should have to link saige, an LLM SDK, or a durable-execution engine to
// do it. pkg/agentrt (which produces these) imports saige; this package must
// not, and the compiler enforces that by import graph:
//
//	payload  -> envelope, address, stdlib          (a leaf)
//	agentrt  -> payload, saige                     (produces them)
//	runtime  -> payload, agentrt, duraturo, dispatch
//
// The structs are therefore a wire contract, not an implementation detail.
// Fields are only ever added, never renamed or repurposed, and every field is
// optional to a reader: an old client decoding a newer payload sees the fields
// it knows and ignores the rest, which is what json.Unmarshal does by default.
//
// Addresses travel as their canonical URI string rather than as an
// address.Address struct. Address has no json tags — marshalling it directly
// would put Go field names on the wire and freeze them there — and the URI is
// already the stable, documented rendering of an endpoint.
package payload

import (
	"encoding/json"
	"fmt"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/envelope"
)

// Tool call phases reported in ToolCall.Phase. They distinguish the model
// deciding to call a tool from the tool actually starting, which is the
// difference between "thinking about running the tests" and "running them".
const (
	// PhaseRequested is the model generating a tool call.
	PhaseRequested = "requested"
	// PhaseExecuting is the harness having begun executing it.
	PhaseExecuting = "executing"
)

// Text is the body of envelope.KindText: one fragment of the agent's answer,
// in order. A sink concatenates fragments; it must not assume a fragment ends
// on a token, word, or UTF-8-safe boundary of any semantic unit beyond a valid
// string.
type Text struct {
	Text string `json:"text"`
}

// Thinking is the body of envelope.KindThinking: one fragment of extended
// reasoning. It is a separate kind rather than a flag on Text because most
// sinks should not render it by default — a route that wants the answer
// selects "delta.text" and gets no reasoning at all.
type Thinking struct {
	Text string `json:"text"`
}

// ToolCall is the body of envelope.KindToolCall: the agent reaching for a
// tool. Phase says whether the model is still generating the call or the
// harness has started running it; both arrive on the same kind so a sink can
// render one line per tool call and update it.
type ToolCall struct {
	// ID correlates this call with its ToolDone. It is the provider's tool
	// call id, unique within a turn.
	ID string `json:"id"`
	// Name is the tool being called.
	Name string `json:"name"`
	// Phase is PhaseRequested or PhaseExecuting.
	Phase string `json:"phase"`
}

// ToolDone is the body of envelope.KindToolDone: a tool execution finished.
// Error is set when the tool failed; Result is then the failure text the model
// was shown, because the model always sees something and an auditor should see
// the same thing.
type ToolDone struct {
	ID     string `json:"id"`
	Name   string `json:"name,omitempty"`
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// Provenance locates the source a citation came from, well enough to follow.
// It mirrors saige's rag/types.Provenance field for field; it is restated here
// so a client can decode a citation without importing the RAG stack.
type Provenance struct {
	DocumentUUID   string `json:"document_uuid,omitempty"`
	DocumentTitle  string `json:"document_title,omitempty"`
	SourceURI      string `json:"source_uri,omitempty"`
	SectionUUID    string `json:"section_uuid,omitempty"`
	SectionHeading string `json:"section_heading,omitempty"`
	SectionIndex   int    `json:"section_index,omitempty"`
}

// Citation is the body of envelope.KindCitation: one retrieved passage the
// turn was grounded in.
//
// Connectors render these as footnote links: Label is the marker that appears
// inline in the answer ("[1]"), Text is the exact quoted source so a reader
// can see what was used without following anything, and Source is where it
// came from — SourceURI first, falling back to document and section titles
// when the source has no addressable URI.
type Citation struct {
	// Label is the inline marker the assembled context used, such as "[1]".
	Label string `json:"label"`
	// Text is the exact source text, not a summary of it.
	Text string `json:"text"`
	// Source locates the passage.
	Source Provenance `json:"source"`
	// Index is the citation's position in the assembled context, from 0. It
	// orders citations for a sink that renders a footnote list.
	Index int `json:"index"`
}

// Lifecycle is the body of every envelope lifecycle kind: accepted, started,
// parked, resumed, completed, failed.
//
// Attempt and Terminal are what make a failure readable. Retry budget belongs
// to the durable engine, not to this package, so a failed attempt is reported
// as it happens with Terminal false: the run may still succeed later. Only a
// failure the engine will not retry reports Terminal true. The ledger remains
// the authority on a run's status — the bus reports what each attempt did.
type Lifecycle struct {
	// Attempt is the execution attempt that produced this event, from 1.
	Attempt int `json:"attempt,omitempty"`
	// Rev is the pinned agent revision the run executes under.
	Rev int `json:"rev,omitempty"`
	// Error is the failure message on envelope.KindFailed.
	Error string `json:"error,omitempty"`
	// Terminal reports that the run will not be retried. Meaningful on
	// envelope.KindFailed.
	Terminal bool `json:"terminal,omitempty"`
	// Reason explains a park, in the words of whatever the run is waiting
	// for.
	Reason string `json:"reason,omitempty"`
	// Text is the final answer, on envelope.KindCompleted. A sink that only
	// subscribes to lifecycle.completed still gets the whole answer without
	// having to reassemble deltas.
	Text string `json:"text,omitempty"`
}

// Action is the body of envelope.KindActionTaken: a connector's native verb
// ran. This is the "the work got done" signal, distinct from the agent saying
// it would.
//
// Target is where the action was aimed; Address is what it produced or
// changed, and is the value a sink turns into a link. Both are address URIs.
type Action struct {
	// Name is the action's tool name, such as "jira_transition".
	Name string `json:"name"`
	// Target is the address URI the action was invoked against.
	Target string `json:"target,omitempty"`
	// Summary is the connector's own description of what happened.
	Summary string `json:"summary,omitempty"`
	// Address is the address URI of whatever was created or changed.
	Address string `json:"address,omitempty"`
	// Data is the connector's structured result, passed through untouched.
	Data json.RawMessage `json:"data,omitempty"`
	// Error is set when the action failed. The agent saw this text too.
	Error string `json:"error,omitempty"`
}

// TargetAddress parses Target back into an address.
func (a Action) TargetAddress() (address.Address, error) { return parseAddr(a.Target) }

// ResultAddress parses Address back into an address.
func (a Action) ResultAddress() (address.Address, error) { return parseAddr(a.Address) }

func parseAddr(s string) (address.Address, error) {
	if s == "" {
		return address.Address{}, nil
	}
	return address.Parse(s)
}

// Encode marshals a payload body for envelope.Event.Payload.
func Encode(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("payload: encode %T: %w", v, err)
	}
	return b, nil
}

// Decode unmarshals an event's payload into T. It is the reader's half of
// Encode:
//
//	txt, err := payload.Decode[payload.Text](e)
//
// An empty payload decodes to the zero value, because a kind may carry no body
// and a reader should not have to distinguish "no body" from "empty body".
func Decode[T any](e envelope.Event) (T, error) {
	var out T
	if len(e.Payload) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(e.Payload, &out); err != nil {
		return out, fmt.Errorf("payload: decode %s as %T: %w", e.Kind, out, err)
	}
	return out, nil
}

// Of decodes an event into the body type its Kind declares, for a reader that
// switches on kind rather than knowing it in advance — a generic renderer, a
// debug dump, a bridge onto another transport.
//
// An unknown kind is not an error: kind is an open string on purpose, so a
// connector may emit one this package has not been recompiled for. Of returns
// the raw payload and false for those, which lets a renderer pass them through
// instead of dropping them.
func Of(e envelope.Event) (body any, known bool, err error) {
	switch e.Kind {
	case envelope.KindText:
		return decodeOr[Text](e)
	case envelope.KindThinking:
		return decodeOr[Thinking](e)
	case envelope.KindToolCall:
		return decodeOr[ToolCall](e)
	case envelope.KindToolDone:
		return decodeOr[ToolDone](e)
	case envelope.KindCitation:
		return decodeOr[Citation](e)
	case envelope.KindActionTaken:
		return decodeOr[Action](e)
	case envelope.KindAccepted, envelope.KindStarted, envelope.KindParked,
		envelope.KindResumed, envelope.KindCompleted, envelope.KindFailed:
		return decodeOr[Lifecycle](e)
	default:
		return json.RawMessage(e.Payload), false, nil
	}
}

func decodeOr[T any](e envelope.Event) (any, bool, error) {
	v, err := Decode[T](e)
	if err != nil {
		return nil, true, err
	}
	return v, true, nil
}
