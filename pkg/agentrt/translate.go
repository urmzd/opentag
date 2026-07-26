package agentrt

import (
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/envelope"
)

// Chunk is one event body a turn produced: a kind and its marshalled payload.
//
// It is deliberately NOT an envelope.Event. An Event carries run identity —
// topic, sequence, tenant, revision — and this package does not know what run
// it is inside, on purpose: the same translation must serve a durable run, a
// one-shot CLI invocation, and a test. pkg/runtime owns the run and stamps the
// identity on.
type Chunk struct {
	Kind    envelope.Kind
	Payload []byte
}

// Translator converts saige's typed deltas into event bodies.
//
// It is stateful for one reason: saige reports a tool's name when the call
// starts and its result when the call ends, correlated only by tool call id. A
// stateless mapping would publish a "tool finished" event that could not say
// which tool finished, so the translator remembers the names it saw. State is
// per turn; a Translator is not safe for concurrent use and is not meant to be
// shared between turns.
type Translator struct {
	names map[string]string // tool call id -> tool name
}

// NewTranslator returns a translator for one turn.
func NewTranslator() *Translator {
	return &Translator{names: make(map[string]string)}
}

// Delta maps one saige delta to an event body. The second result reports
// whether this delta becomes an event at all.
//
// Most deltas do not. saige's delta set is a rendering protocol — block starts
// and ends, token usage, marker prompts, handoffs — while envelope.Kind is a
// published contract with a fixed set of members. Rather than invent kinds for
// the rest, or overload one, this package publishes the five that mean
// something to a subscriber and drops the framing:
//
//	TextContentDelta                        -> delta.text
//	ThinkingContentDelta                    -> delta.thinking
//	ToolCallStartDelta, ToolExecStartDelta  -> delta.tool.call
//	ToolExecEndDelta                        -> delta.tool.done
//
// Citations are the exception that is not in this list: saige v0.14.0 has no
// agent-level citation delta, so they are derived from retrieval instead. See
// Citations.
//
// Two deltas are dropped for reasons worth stating:
//
//   - ErrorDelta becomes the turn's error, not an event. Failure is a run
//     lifecycle fact and pkg/runtime publishes it with the attempt and
//     terminality a subscriber needs; a bare error event mid-stream would say
//     less and could contradict the lifecycle event that follows.
//   - UsageDelta is telemetry. It belongs on a metrics path, and putting token
//     counts on a fan-out bus that connectors render would leak cost data into
//     Slack threads.
func (t *Translator) Delta(d saigetypes.Delta) (Chunk, bool) {
	switch v := d.(type) {
	case saigetypes.TextContentDelta:
		if v.Content == "" {
			return Chunk{}, false
		}
		return chunk(envelope.KindText, payload.Text{Text: v.Content})

	case saigetypes.ThinkingContentDelta:
		if v.Content == "" {
			return Chunk{}, false
		}
		return chunk(envelope.KindThinking, payload.Thinking{Text: v.Content})

	case saigetypes.ToolCallStartDelta:
		t.remember(v.ID, v.Name)
		return chunk(envelope.KindToolCall, payload.ToolCall{
			ID: v.ID, Name: v.Name, Phase: payload.PhaseRequested,
		})

	case saigetypes.ToolExecStartDelta:
		t.remember(v.ToolCallID, v.Name)
		return chunk(envelope.KindToolCall, payload.ToolCall{
			ID: v.ToolCallID, Name: t.name(v.ToolCallID), Phase: payload.PhaseExecuting,
		})

	case saigetypes.ToolExecEndDelta:
		return chunk(envelope.KindToolDone, payload.ToolDone{
			ID:     v.ToolCallID,
			Name:   t.name(v.ToolCallID),
			Result: v.Result,
			Error:  v.Error,
		})

	case saigetypes.ToolExecDelta:
		// A streaming tool or sub-agent nested inside a tool call. Its inner
		// deltas are the agent's work too, so they translate the same way;
		// the tool call id already told a subscriber that a tool is running.
		return t.Delta(v.Inner)

	default:
		return Chunk{}, false
	}
}

// remember records a tool call id's name so the matching ToolDone can report it.
func (t *Translator) remember(id, name string) {
	if id == "" || name == "" {
		return
	}
	t.names[id] = name
}

// name returns the remembered tool name for a call id, or "" if the delta
// stream never announced one. An unnamed tool is reported as unnamed rather
// than guessed: the id is still there to correlate with.
func (t *Translator) name(id string) string { return t.names[id] }

// chunk marshals a body into a Chunk. A payload that will not marshal is a
// programming error in this package (every payload type is a plain struct of
// strings and ints), so the chunk is dropped rather than failing a live turn
// over it.
func chunk(kind envelope.Kind, body any) (Chunk, bool) {
	b, err := payload.Encode(body)
	if err != nil {
		return Chunk{}, false
	}
	return Chunk{Kind: kind, Payload: b}, true
}
