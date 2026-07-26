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
			in:   saigetypes.TextContentDelta{Content: "hello"},
			kind: envelope.KindText,
			want: `{"text":"hello"}`,
		},
		{
			name: "thinking content",
			in:   saigetypes.ThinkingContentDelta{Content: "weighing options"},
			kind: envelope.KindThinking,
			want: `{"text":"weighing options"}`,
		},
		{
			name: "tool call start",
			in:   saigetypes.ToolCallStartDelta{ID: "call_1", Name: "grep"},
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
			in:   saigetypes.ToolExecEndDelta{ToolCallID: "call_3", Result: "ok"},
			kind: envelope.KindToolDone,
			want: `{"id":"call_3","result":"ok"}`,
		},
		{
			name: "tool exec end with error",
			in:   saigetypes.ToolExecEndDelta{ToolCallID: "call_4", Result: "failed", Error: "exit 1"},
			kind: envelope.KindToolDone,
			want: `{"id":"call_4","result":"failed","error":"exit 1"}`,
		},
		{
			name: "nested tool delta unwraps to its inner kind",
			in:   saigetypes.ToolExecDelta{ToolCallID: "call_5", Inner: saigetypes.TextContentDelta{Content: "from a sub-agent"}},
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

// A finished tool must be reported by name, and saige only says the name when
// the call starts, so the translator has to remember it across deltas.
func TestToolDoneReportsTheNameFromTheCallThatStartedIt(t *testing.T) {
	t.Parallel()

	tr := agentrt.NewTranslator()
	if _, ok := tr.Delta(saigetypes.ToolCallStartDelta{ID: "call_1", Name: "run_tests"}); !ok {
		t.Fatal("tool call start produced no event")
	}
	done, ok := tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: "call_1", Result: "42 passed"})
	if !ok {
		t.Fatal("tool exec end produced no event")
	}
	var body payload.ToolDone
	if err := json.Unmarshal(done.Payload, &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Name != "run_tests" {
		t.Fatalf("tool name is %q, want %q", body.Name, "run_tests")
	}
}

// Names are remembered per call id, so two tools running in parallel must not be
// confused for each other.
func TestParallelToolCallsKeepTheirOwnNames(t *testing.T) {
	t.Parallel()

	tr := agentrt.NewTranslator()
	tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "a", Name: "read"})
	tr.Delta(saigetypes.ToolExecStartDelta{ToolCallID: "b", Name: "write"})

	for id, want := range map[string]string{"a": "read", "b": "write"} {
		c, ok := tr.Delta(saigetypes.ToolExecEndDelta{ToolCallID: id})
		if !ok {
			t.Fatalf("call %s produced no done event", id)
		}
		var body payload.ToolDone
		if err := json.Unmarshal(c.Payload, &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.Name != want {
			t.Errorf("call %s finished as %q, want %q", id, body.Name, want)
		}
	}
}

// The deltas that are framing, telemetry, or lifecycle must NOT become events:
// envelope.Kind is a published contract and this package does not get to invent
// members of it.
func TestFramingAndTelemetryDeltasProduceNoEvents(t *testing.T) {
	t.Parallel()

	dropped := []saigetypes.Delta{
		saigetypes.TextStartDelta{},
		saigetypes.TextEndDelta{},
		saigetypes.ThinkingStartDelta{},
		saigetypes.ThinkingEndDelta{Signature: "sig"},
		saigetypes.ToolCallArgumentDelta{Content: `{"path":`},
		saigetypes.ToolCallEndDelta{Arguments: map[string]any{"path": "."}},
		saigetypes.UsageDelta{PromptTokens: 10, CompletionTokens: 3, Latency: time.Second},
		saigetypes.HandoffDelta{From: "a", To: "b"},
		saigetypes.FeedbackDelta{TargetNodeID: "n1"},
		saigetypes.DoneDelta{},
		saigetypes.ErrorDelta{},
		saigetypes.TextContentDelta{},     // empty fragment: nothing to render
		saigetypes.ThinkingContentDelta{}, // ditto
	}
	tr := agentrt.NewTranslator()
	for _, d := range dropped {
		if c, ok := tr.Delta(d); ok {
			t.Errorf("%T became a %s event, want no event", d, c.Kind)
		}
	}
}
