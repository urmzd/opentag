package render

import (
	"bytes"
	"encoding/json"

	"github.com/urmzd/opentag/pkg/address"
)

// Event.Payload is opaque to the bus, which means the runtime that publishes it
// and the sinks that draw it agree by convention. This file is that convention,
// written from the reader's side.
//
// The shapes, all JSON objects:
//
//	delta.text          {"text": "..."}                also: content, delta
//	delta.thinking      {"text": "..."}
//	delta.tool.call     {"id": "...", "name": "...", "args": {...}}
//	delta.tool.done     {"id": "...", "result": "...", "error": "..."}
//	delta.citation      {"title": "...", "uri": "...", "snippet": "...",
//	                     "label": "..."}   or a JSON array of those
//	action.taken        {"summary": "...", "address": "github://o/r/issues/1"}
//	lifecycle.failed    {"error": "..."}                also: message
//
// Every decoder here is tolerant in two specific ways, and it is worth being
// precise about why, because "be liberal in what you accept" is usually how a
// contract rots:
//
//   - Alternate field names (content/delta for text, args/arguments/input for
//     tool arguments, uri/url/source for a citation) are accepted because the
//     producer of these payloads is an LLM runtime bridging saige's delta types,
//     and which of those names it uses is a detail of that bridge rather than a
//     semantic difference. Accepting the synonyms costs nothing and removes a
//     class of silent blank renders.
//   - A payload that is not a JSON object is treated as the text itself. A
//     runtime that publishes raw fragment bytes for delta.text is not wrong —
//     the field is bytes — and rendering those bytes is always closer to right
//     than rendering nothing.
//
// What is NOT tolerated: guessing at structure. An unrecognized object renders
// as empty rather than as its own JSON, because dumping a struct into a Slack
// thread is worse than saying nothing.

// textPayload is the decoded form of a text or thinking delta.
type textPayload struct {
	Text    string `json:"text"`
	Content string `json:"content"`
	Delta   string `json:"delta"`
}

// decodeText extracts the fragment from a text-bearing payload.
func decodeText(p []byte) string {
	trimmed := bytes.TrimSpace(p)
	if len(trimmed) == 0 {
		return ""
	}
	switch trimmed[0] {
	case '{':
		var tp textPayload
		if err := json.Unmarshal(trimmed, &tp); err != nil {
			return string(p)
		}
		return first(tp.Text, tp.Content, tp.Delta)
	case '"':
		var s string
		if err := json.Unmarshal(trimmed, &s); err == nil {
			return s
		}
	}
	// Not JSON: the payload is the fragment. Note that the original bytes are
	// returned, not the trimmed ones — leading and trailing whitespace is
	// meaningful in a stream of text fragments that will be concatenated.
	return string(p)
}

// toolPayload is the decoded form of a tool call or completion.
type toolPayload struct {
	ID     string
	Name   string
	Args   string
	Result string
	Error  string
}

type toolWire struct {
	ID         string          `json:"id"`
	ToolCallID string          `json:"tool_call_id"`
	Name       string          `json:"name"`
	Tool       string          `json:"tool"`
	Args       json.RawMessage `json:"args"`
	Arguments  json.RawMessage `json:"arguments"`
	Input      json.RawMessage `json:"input"`
	Result     string          `json:"result"`
	Output     string          `json:"output"`
	Error      string          `json:"error"`
}

// decodeTool extracts a tool call or completion.
func decodeTool(p []byte) toolPayload {
	var w toolWire
	if err := json.Unmarshal(bytes.TrimSpace(p), &w); err != nil {
		return toolPayload{}
	}
	return toolPayload{
		ID:     first(w.ID, w.ToolCallID),
		Name:   first(w.Name, w.Tool),
		Args:   compact(first(string(w.Args), string(w.Arguments), string(w.Input))),
		Result: first(w.Result, w.Output),
		Error:  w.Error,
	}
}

type citeWire struct {
	Label    string `json:"label"`
	Title    string `json:"title"`
	URI      string `json:"uri"`
	URL      string `json:"url"`
	Source   string `json:"source"`
	Citation string `json:"citation"`
	Snippet  string `json:"snippet"`
	Text     string `json:"text"`
}

// decodeCitations extracts one or many citations. Both forms are accepted
// because a RAG step naturally produces its sources as a set, while a streaming
// runtime naturally emits them one at a time.
func decodeCitations(p []byte) []Citation {
	trimmed := bytes.TrimSpace(p)
	if len(trimmed) == 0 {
		return nil
	}
	if trimmed[0] == '[' {
		var wires []citeWire
		if err := json.Unmarshal(trimmed, &wires); err != nil {
			return nil
		}
		out := make([]Citation, 0, len(wires))
		for _, w := range wires {
			out = append(out, w.citation())
		}
		return out
	}
	var w citeWire
	if err := json.Unmarshal(trimmed, &w); err != nil {
		return nil
	}
	return []Citation{w.citation()}
}

func (w citeWire) citation() Citation {
	return Citation{
		Label:   w.Label,
		Title:   first(w.Title, w.Citation),
		URI:     first(w.URI, w.URL, w.Source),
		Snippet: first(w.Snippet, w.Text),
	}
}

type actionWire struct {
	Summary string          `json:"summary"`
	Address json.RawMessage `json:"address"`
	Target  string          `json:"target"`
}

// decodeAction extracts an action.taken payload, which is a marshalled
// connector.Result.
//
// The address is accepted either as a URI string or as the object form
// address.Address marshals to by default, because which one appears depends on
// whether the publisher stringified it before marshalling. Both are reduced to
// the URI string, and an address that does not parse is dropped rather than
// rendered as a broken link.
func decodeAction(p []byte) Action {
	var w actionWire
	if err := json.Unmarshal(bytes.TrimSpace(p), &w); err != nil {
		return Action{}
	}
	return Action{Summary: w.Summary, Address: first(decodeAddress(w.Address), w.Target)}
}

func decodeAddress(raw json.RawMessage) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	if trimmed[0] == '"' {
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return ""
		}
		if _, err := address.Parse(s); err != nil {
			return ""
		}
		return s
	}
	var a address.Address
	if err := json.Unmarshal(trimmed, &a); err != nil {
		return ""
	}
	if a.IsZero() {
		return ""
	}
	return a.String()
}

type errorWire struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// decodeError extracts a failure message.
func decodeError(p []byte) string {
	trimmed := bytes.TrimSpace(p)
	if len(trimmed) == 0 {
		return ""
	}
	if trimmed[0] == '{' {
		var w errorWire
		if err := json.Unmarshal(trimmed, &w); err == nil {
			return first(w.Error, w.Message)
		}
	}
	return string(trimmed)
}

// compact removes insignificant whitespace from a JSON fragment so that tool
// arguments render on one line. A fragment that is not valid JSON is returned
// as it came, since it is about to be shown to a human either way.
func compact(raw string) string {
	if raw == "" || raw == "null" {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Compact(&buf, []byte(raw)); err != nil {
		return raw
	}
	return buf.String()
}

// first returns the first non-empty argument.
func first(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
