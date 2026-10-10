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
// It holds no state: saige names the tool on every tool delta, including the
// ToolExecEndDelta that reports the result, so each delta translates on its
// own. A Translator is still made per turn with NewTranslator, which keeps the
// call sites stable if the mapping ever needs per-turn state again.
type Translator struct{}

// NewTranslator returns a translator for one turn.
func NewTranslator() *Translator { return &Translator{} }

// Delta maps one saige delta to an event body. The second result reports
// whether this delta becomes an event at all.
//
// Most deltas do not. saige's delta set is a rendering protocol — part starts
// and ends, token usage, marker prompts, handoffs — while envelope.Kind is a
// published contract with a fixed set of members. Rather than invent kinds for
// the rest, or overload one, this package publishes the five that mean
// something to a subscriber and drops the framing:
//
//	PartDelta with Text                        -> delta.text
//	PartDelta with Thinking                    -> delta.thinking
//	PartStart of a tool_call, ToolExecStartDelta -> delta.tool.call
//	ToolExecEndDelta                           -> delta.tool.done
//
// Citations are the exception that is not in this list: they are derived from
// retrieval instead (see Citations). saige's CitationDelta, which reports what
// a model or tool cited, is not translated yet and is dropped with the framing.
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
	case saigetypes.PartDelta:
		switch {
		case v.Text != "":
			return chunk(envelope.KindText, payload.Text{Text: v.Text})
		case v.Thinking != "":
			return chunk(envelope.KindThinking, payload.Thinking{Text: v.Thinking})
		default:
			return Chunk{}, false
		}

	case saigetypes.PartStart:
		if v.Kind != saigetypes.KindToolCall {
			return Chunk{}, false
		}
		return chunk(envelope.KindToolCall, payload.ToolCall{
			ID: v.ID, Name: v.Name, Phase: payload.PhaseRequested,
		})

	case saigetypes.ToolExecStartDelta:
		return chunk(envelope.KindToolCall, payload.ToolCall{
			ID: v.ToolCallID, Name: v.Name, Phase: payload.PhaseExecuting,
		})

	case saigetypes.ToolExecEndDelta:
		return chunk(envelope.KindToolDone, payload.ToolDone{
			ID:     v.ToolCallID,
			Name:   v.Name,
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
