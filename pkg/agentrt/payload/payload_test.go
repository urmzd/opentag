package payload_test

import (
	"encoding/json"
	"testing"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/envelope"
)

// A payload is a wire contract: a reader that only has the JSON must be able to
// reconstruct the body, so every kind has to survive the round trip a bus makes
// it take.
func TestEveryKindSurvivesEncodeDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		kind envelope.Kind
		body any
	}{
		{"text", envelope.KindText, payload.Text{Text: "hello"}},
		{"thinking", envelope.KindThinking, payload.Thinking{Text: "considering"}},
		{"tool call", envelope.KindToolCall, payload.ToolCall{ID: "call_1", Name: "grep", Phase: payload.PhaseExecuting}},
		{"tool done", envelope.KindToolDone, payload.ToolDone{ID: "call_1", Name: "grep", Result: "3 hits"}},
		{"tool failed", envelope.KindToolDone, payload.ToolDone{ID: "call_1", Name: "grep", Error: "no such file"}},
		{"citation", envelope.KindCitation, payload.Citation{
			Label: "[1]", Text: "the quoted passage", Index: 0,
			Source: payload.Provenance{DocumentUUID: "doc-1", SourceURI: "https://example.test/a", SectionIndex: 2},
		}},
		{"lifecycle", envelope.KindCompleted, payload.Lifecycle{Attempt: 2, Rev: 6, Text: "done"}},
		{"failure", envelope.KindFailed, payload.Lifecycle{Attempt: 1, Rev: 6, Error: "boom", Terminal: true}},
		{"action", envelope.KindActionTaken, payload.Action{
			Name: "jira_transition", Target: "jira://acme/PROJ-5", Summary: "moved to done",
			Address: "jira://acme/PROJ-5", Data: json.RawMessage(`{"status":"done"}`),
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw, err := payload.Encode(tc.body)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			e := envelope.Event{Kind: tc.kind, Payload: raw}

			got, known, err := payload.Of(e)
			if err != nil {
				t.Fatalf("Of: %v", err)
			}
			if !known {
				t.Fatalf("Of reported kind %s as unknown", tc.kind)
			}
			// Compare through JSON: a body may hold a json.RawMessage, which
			// is a slice and so is not comparable with ==.
			gotJSON, _ := json.Marshal(got)
			wantJSON, _ := json.Marshal(tc.body)
			if string(gotJSON) != string(wantJSON) {
				t.Fatalf("round trip changed the body:\n got %s\nwant %s", gotJSON, wantJSON)
			}
		})
	}
}

// An old client must survive a newer kind rather than dropping it, because kind
// is an open string and connectors ship independently of this package.
func TestUnknownKindDegradesToRawPayload(t *testing.T) {
	t.Parallel()

	e := envelope.Event{Kind: envelope.Kind("delta.hologram"), Payload: []byte(`{"shape":"cube"}`)}
	body, known, err := payload.Of(e)
	if err != nil {
		t.Fatalf("Of: %v", err)
	}
	if known {
		t.Fatal("an unrecognised kind must not be reported as known")
	}
	raw, ok := body.(json.RawMessage)
	if !ok {
		t.Fatalf("body is %T, want json.RawMessage", body)
	}
	if string(raw) != `{"shape":"cube"}` {
		t.Fatalf("raw payload is %s, want it passed through untouched", raw)
	}
}

// A kind may carry no body at all; a reader must not have to distinguish that
// from an empty one.
func TestEmptyPayloadDecodesToZeroValue(t *testing.T) {
	t.Parallel()

	got, err := payload.Decode[payload.Lifecycle](envelope.Event{Kind: envelope.KindStarted})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != (payload.Lifecycle{}) {
		t.Fatalf("got %+v, want the zero value", got)
	}
}

// Addresses travel as URIs so that Go field names never reach the wire; they
// must come back as addresses.
func TestActionAddressesRoundTripThroughTheirURIs(t *testing.T) {
	t.Parallel()

	target := address.MustParse("github://urmzd/opentag/issues/42")
	result := address.MustParse("github://urmzd/opentag/issues/42/comments/7")
	a := payload.Action{Name: "github_comment", Target: target.String(), Address: result.String()}

	raw, err := payload.Encode(a)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	back, err := payload.Decode[payload.Action](envelope.Event{Kind: envelope.KindActionTaken, Payload: raw})
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	gotTarget, err := back.TargetAddress()
	if err != nil {
		t.Fatalf("TargetAddress: %v", err)
	}
	if gotTarget.String() != target.String() {
		t.Fatalf("target is %s, want %s", gotTarget, target)
	}
	gotResult, err := back.ResultAddress()
	if err != nil {
		t.Fatalf("ResultAddress: %v", err)
	}
	if gotResult.String() != result.String() {
		t.Fatalf("address is %s, want %s", gotResult, result)
	}
}

// An action with no addresses must not turn the empty string into an error.
func TestAbsentAddressesParseAsUnset(t *testing.T) {
	t.Parallel()

	a := payload.Action{Name: "cron_noop"}
	for _, tc := range []struct {
		name string
		get  func() (address.Address, error)
	}{
		{"target", a.TargetAddress},
		{"address", a.ResultAddress},
	} {
		got, err := tc.get()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if !got.IsZero() {
			t.Fatalf("%s is %s, want the zero address", tc.name, got)
		}
	}
}

// The field names are the contract. This test fails on a rename, which is the
// point: a renamed field silently breaks every deployed client.
func TestJSONFieldNamesAreStable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body any
		want string
	}{
		{payload.Text{Text: "a"}, `{"text":"a"}`},
		{payload.Thinking{Text: "a"}, `{"text":"a"}`},
		{payload.ToolCall{ID: "1", Name: "n", Phase: payload.PhaseRequested}, `{"id":"1","name":"n","phase":"requested"}`},
		{payload.ToolDone{ID: "1"}, `{"id":"1"}`},
		{payload.Lifecycle{Attempt: 1}, `{"attempt":1}`},
		{payload.Citation{Label: "[1]", Text: "t"}, `{"label":"[1]","text":"t","source":{},"index":0}`},
		{payload.Action{Name: "n"}, `{"name":"n"}`},
	}
	for _, tc := range tests {
		raw, err := payload.Encode(tc.body)
		if err != nil {
			t.Fatalf("Encode %T: %v", tc.body, err)
		}
		if string(raw) != tc.want {
			t.Errorf("%T encodes as %s, want %s", tc.body, raw, tc.want)
		}
	}
}
