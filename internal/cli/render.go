package cli

import (
	"fmt"
	"strings"
	"time"

	opentagv1 "github.com/urmzd/opentag/gen/opentag/v1"
	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/envelope"
)

// eventJSON is the shape the streaming commands emit under --format json.
//
// It is a hand-written struct rather than protojson of the wire Event for one
// reason: payload is bytes on the wire, and protojson would base64 it. A
// consumer piping `opentag listen` into jq wants the citation it can read, not
// a blob it has to decode in a second step. Kind-specific bodies are decoded
// here and inlined, and anything unrecognised falls back to the raw string so
// a new kind is still legible to an old client.
type eventJSON struct {
	Seq     uint64    `json:"seq"`
	Topic   string    `json:"topic"`
	RunID   string    `json:"run_id"`
	Agent   string    `json:"agent"`
	Rev     int32     `json:"rev"`
	Origin  string    `json:"origin"`
	Kind    string    `json:"kind"`
	At      time.Time `json:"at"`
	Payload any       `json:"payload,omitempty"`
}

func toEventJSON(e *opentagv1.Event) eventJSON {
	out := eventJSON{
		Seq:    e.GetSeq(),
		Topic:  e.GetTopic(),
		RunID:  e.GetRunId(),
		Agent:  e.GetAgent(),
		Rev:    e.GetRev(),
		Origin: e.GetOrigin(),
		Kind:   e.GetKind(),
	}
	if ts := e.GetAt(); ts != nil {
		out.At = ts.AsTime()
	}
	if body, known, err := payload.Of(asEnvelope(e)); err == nil && known {
		out.Payload = body
	} else if raw := e.GetPayload(); len(raw) > 0 {
		out.Payload = string(raw)
	}
	return out
}

// asEnvelope rebuilds enough of an envelope.Event for payload.Of to dispatch
// on. Only Kind and Payload are read, but the whole value is filled so that a
// future decoder needing more does not silently see zeros.
func asEnvelope(e *opentagv1.Event) envelope.Event {
	return envelope.Event{
		Seq:     e.GetSeq(),
		RunID:   e.GetRunId(),
		Tenant:  e.GetTenant(),
		Agent:   e.GetAgent(),
		Rev:     int(e.GetRev()),
		Origin:  e.GetOrigin(),
		Kind:    envelope.Kind(e.GetKind()),
		Payload: e.GetPayload(),
	}
}

// renderEventText writes one event for a human.
//
// answerOnly is what makes `opentag tag` feel like a conversation rather than a
// log: text fragments are written bare and unterminated so they accumulate into
// the answer as it arrives, and everything else goes to stderr as a diagnostic.
// `opentag listen` sets it false and gets one labelled line per event, because
// there a stream of many runs concatenated into one paragraph would be
// unreadable.
func renderEventText(u *ui, e *opentagv1.Event, answerOnly bool) {
	kind := envelope.Kind(e.GetKind())
	body, known, err := payload.Of(asEnvelope(e))
	if err != nil || !known {
		if !answerOnly {
			u.printf("%s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), kind)
		}
		return
	}

	switch v := body.(type) {
	case payload.Text:
		if answerOnly {
			u.printf("%s", v.Text)
			u.flush()
			return
		}
		u.printf("%s %s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.cyan(string(kind)), oneLine(v.Text))

	case payload.Thinking:
		// Reasoning is a diagnostic even in listen: a reader asked for the
		// agent's output, and thinking is how it got there.
		u.logf("%s %s\n", u.dim("thinking"), u.dim(oneLine(v.Text)))

	case payload.ToolCall:
		u.logf("%s %s %s\n", u.blue("tool"), v.Name, u.dim(v.Phase))

	case payload.ToolDone:
		if v.Error != "" {
			u.logf("%s %s %s\n", u.red("tool"), v.Name, u.red(oneLine(v.Error)))
			return
		}
		u.logf("%s %s %s\n", u.green("tool"), v.Name, u.dim(oneLine(v.Result)))

	case payload.Citation:
		// Citations go to stdout even in answerOnly mode: they are part of the
		// answer, not commentary on it, and an answer whose grounding is only
		// visible on stderr cannot be piped anywhere useful.
		src := v.Source.SourceURI
		if src == "" {
			src = strings.TrimSpace(v.Source.DocumentTitle + " " + v.Source.SectionHeading)
		}
		if answerOnly {
			u.printf("\n%s %s %s\n", u.yellow(v.Label), oneLine(v.Text), u.dim(src))
			return
		}
		u.printf("%s %s %s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), u.yellow(string(kind)), oneLine(v.Text), u.dim(src))

	case payload.Lifecycle:
		line := string(kind)
		if v.Attempt > 0 {
			line += fmt.Sprintf(" attempt=%d", v.Attempt)
		}
		if v.Rev > 0 {
			line += fmt.Sprintf(" rev=%d", v.Rev)
		}
		paint := u.dim
		switch kind {
		case envelope.KindCompleted:
			paint = u.green
		case envelope.KindFailed:
			paint = u.red
		}
		if answerOnly {
			u.logf("%s\n", paint(line))
			return
		}
		u.printf("%s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), paint(line))

	case payload.Action:
		u.logf("%s %s\n", u.green("action"), oneLine(v.Summary))

	default:
		if !answerOnly {
			u.printf("%s %s\n", u.dim(fmt.Sprintf("%4d", e.GetSeq())), kind)
		}
	}
}

// oneLine flattens a fragment for a single-line render. Deltas arrive with
// whatever whitespace the model produced, and a newline inside a table cell
// breaks the alignment of every row after it.
func oneLine(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	s = strings.ReplaceAll(s, "\r", "")
	s = strings.TrimSpace(s)
	const max = 120
	if len(s) > max {
		return s[:max-1] + "…"
	}
	return s
}

// terminal reports whether an event ends its run. `tag` stops on it; `listen`
// ignores it, because a topic outlives the runs on it.
func terminal(e *opentagv1.Event) bool {
	switch envelope.Kind(e.GetKind()) {
	case envelope.KindCompleted, envelope.KindFailed:
		return true
	default:
		return false
	}
}
