package agentrt_test

import (
	"encoding/json"
	"testing"
	"time"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/opentag/pkg/agentrt"
	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/envelope"
)

// Every delta the mapping claims to carry must produce an event of the declared
// kind with a body a reader can decode. A gap here is an event kind that quietly
// never appears on the bus.
func TestEveryMappedDeltaBecomesItsKind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   saigetypes.Delta
		kind envelope.Kind
		want string // the expected payload JSON
	}{
		{
			name: "text content",
			in:   saigetypes.PartDelta{Text: "hello"},
			kind: envelope.KindText,
			want: `{"text":"hello"}`,
		},
		{
			name: "thinking content",
			in:   saigetypes.PartDelta{Thinking: "weighing options"},
			kind: envelope.KindThinking,
			want: `{"text":"weighing options"}`,
		},
		{
			name: "tool call start",
			in:   saigetypes.PartStart{Kind: saigetypes.KindToolCall, ID: "call_1", Name: "grep"},
			kind: envelope.KindToolCall,
			want: `{"id":"call_1","name":"grep","phase":"requested"}`,
		},
		{
			name: "tool exec start",
			in:   saigetypes.ToolExecStartDelta{ToolCallID: "call_2", Name: "build"},
			kind: envelope.KindToolCall,
			want: `{"id":"call_2","name":"build","phase":"executing"}`,
		},
		{
			name: "tool exec end",
			in:   saigetypes.ToolExecEndDelta{ToolCallID: "call_3", Name: "build", Result: "ok"},
			kind: envelope.KindToolDone,
			want: `{"id":"call_3","name":"build","result":"ok"}`,
		},
		{
			name: "tool exec end with error",
			in:   saigetypes.ToolExecEndDelta{ToolCallID: "call_4", Result: "failed", Error: "exit 1"},
			kind: envelope.KindToolDone,
			want: `{"id":"call_4","result":"failed","error":"exit 1"}`,
		},
		{
			name: "nested tool delta unwraps to its inner kind",
			in:   saigetypes.ToolExecDelta{ToolCallID: "call_5", Inner: saigetypes.PartDelta{Text: "from a sub-agent"}},
			kind: envelope.KindText,
			want: `{"text":"from a sub-agent"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, ok := agentrt.NewTranslator().Delta(tc.in)
			if !ok {
				t.Fatalf("%T produced no event", tc.in)
			}
			if got.Kind != tc.kind {
				t.Errorf("kind is %s, want %s", got.Kind, tc.kind)
			}
			if string(got.Payload) != tc.want {
				t.Errorf("payload is %s, want %s", got.Payload, tc.want)
			}
		})
	}
}

// A finished tool must be reported by name, taken from the ToolExecEndDelta
// itself. Two calls finishing in either order must not be confused for each
// other, which a translator that guessed from earlier deltas could do.
func TestToolDoneReportsItsOwnName(t *testing.T) {
	t.Parallel()

	tr := agentrt.NewTranslator()
	tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "a", Name: "read"})
	tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "b", Name: "write"})

	for _, end := range []saigetypes.ToolExecEndDelta{
		{ToolCallID: "b", Name: "write"},
		{ToolCallID: "a", Name: "read"},
	} {
		c, ok := tr.Delta(end)
		if !ok {
			t.Fatalf("call %s produced no done event", end.ToolCallID)
		}
		var body payload.ToolDone
		if err := json.Unmarshal(c.Payload, &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.ID != end.ToolCallID || body.Name != end.Name {
			t.Errorf("call %s finished as %s/%q, want %q", end.ToolCallID, body.ID, body.Name, end.Name)
		}
	}
}

// The deltas that are framing, telemetry, or lifecycle must NOT become events:
// envelope.Kind is a published contract and this package does not get to invent
// members of it.
func TestFramingAndTelemetryDeltasProduceNoEvents(t *testing.T) {
	t.Parallel()

	dropped := []saigetypes.Delta{
		saigetypes.PartStart{Kind: saigetypes.KindText},
		saigetypes.PartEnd{Part: saigetypes.Text("hi")},
		saigetypes.PartStart{Kind: saigetypes.KindThinking},
		saigetypes.PartDelta{Signature: "sig"},
		saigetypes.PartDelta{Args: `{"path":`},
		saigetypes.PartEnd{Part: saigetypes.ToolCallPart{ID: "call_1", Name: "grep", Arguments: map[string]any{"path": "."}}},
		saigetypes.UsageDelta{PromptTokens: 10, CompletionTokens: 3, Latency: time.Second},
		saigetypes.HandoffDelta{From: "a", To: "b"},
		saigetypes.FeedbackDelta{TargetNodeID: "n1"},
		saigetypes.DoneDelta{},
		saigetypes.ErrorDelta{},
		saigetypes.CitationDelta{Citation: saigetypes.Citation{Title: "model-cited"}}, // not translated yet: citations come from retrieval
		saigetypes.PartDelta{}, // empty fragment: nothing to render
	}
	tr := agentrt.NewTranslator()
	for _, d := range dropped {
		if c, ok := tr.Delta(d); ok {
			t.Errorf("%T became a %s event, want no event", d, c.Kind)
		}
	}
}
