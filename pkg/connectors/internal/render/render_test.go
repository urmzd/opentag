package render_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/opentag/pkg/connectors/internal/render"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/topic"
)

// ev builds a run-scoped event the way the bus delivers one.
func ev(seq uint64, kind envelope.Kind, payload string) envelope.Event {
	return envelope.Event{
		Seq:     seq,
		Topic:   topic.MustParse("agent:docs-bot:run_1"),
		RunID:   "run_1",
		Agent:   "docs-bot",
		Rev:     3,
		Origin:  "slack",
		Tenant:  "acme",
		Kind:    kind,
		Payload: []byte(payload),
		At:      time.Unix(1700000000, 0).UTC().Add(time.Duration(seq) * time.Second),
	}
}

func text(seq uint64, s string) envelope.Event {
	return ev(seq, envelope.KindText, `{"text":`+quote(s)+`}`)
}

func quote(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestApplyingTheSameEventTwiceLeavesTheDocumentIdentical(t *testing.T) {
	events := []envelope.Event{
		ev(1, envelope.KindAccepted, ""),
		text(2, "hello "),
		ev(3, envelope.KindCitation, `{"title":"Design","uri":"https://example.com/d"}`),
		text(4, "world"),
		ev(5, envelope.KindCompleted, ""),
	}

	once := render.New()
	for _, e := range events {
		once.Apply(e)
	}

	twice := render.New()
	for _, e := range events {
		twice.Apply(e)
		if changed := twice.Apply(e); changed {
			t.Errorf("second Apply of seq %d reported a change", e.Seq)
		}
	}

	if got, want := twice.View(), once.View(); !equalViews(got, want) {
		t.Errorf("redelivery changed the document:\n got %+v\nwant %+v", got, want)
	}
}

func TestTextIsAssembledInSequenceOrderNotArrivalOrder(t *testing.T) {
	tests := []struct {
		name  string
		order []uint64
	}{
		{name: "in order", order: []uint64{1, 2, 3, 4}},
		{name: "reversed", order: []uint64{4, 3, 2, 1}},
		{name: "one late arrival", order: []uint64{1, 3, 2, 4}},
		{name: "last first", order: []uint64{4, 1, 2, 3}},
		{name: "with redelivery", order: []uint64{1, 2, 2, 4, 3, 1, 4}},
	}
	fragments := map[uint64]string{1: "the ", 2: "quick ", 3: "brown ", 4: "fox"}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := render.New()
			for _, seq := range tc.order {
				d.Apply(text(seq, fragments[seq]))
			}
			if got := d.View().Text; got != "the quick brown fox" {
				t.Errorf("Text = %q, want %q", got, "the quick brown fox")
			}
		})
	}
}

func TestALateFragmentOutsideTheWindowIsDroppedRatherThanAppendedOutOfPlace(t *testing.T) {
	d := &render.Doc{Window: 4}
	// Seq 1 never arrives, so the mark cannot advance contiguously; the window
	// forces the oldest fragments to settle.
	for seq := uint64(2); seq <= 10; seq++ {
		d.Apply(text(seq, "x"))
	}
	if got := d.View().Text; got != strings.Repeat("x", 9) {
		t.Fatalf("Text = %q, want nine fragments", got)
	}
	if changed := d.Apply(text(1, "STALE")); changed {
		t.Error("a fragment older than the window was applied")
	}
	if got := d.View().Text; strings.Contains(got, "STALE") {
		t.Errorf("Text = %q, must not contain the stale fragment", got)
	}
}

func TestStatusFollowsTheHighestSequenceNumberNotTheLastArrival(t *testing.T) {
	tests := []struct {
		name  string
		order []envelope.Event
		want  render.Status
	}{
		{
			name:  "in order",
			order: []envelope.Event{ev(1, envelope.KindAccepted, ""), ev(2, envelope.KindStarted, ""), ev(9, envelope.KindCompleted, "")},
			want:  render.StatusDone,
		},
		{
			name:  "completion arrives before the start it followed",
			order: []envelope.Event{ev(9, envelope.KindCompleted, ""), ev(2, envelope.KindStarted, "")},
			want:  render.StatusDone,
		},
		{
			name:  "no lifecycle event at all",
			order: []envelope.Event{text(4, "hi")},
			want:  render.StatusUnknown,
		},
		{
			name:  "failure carries its message",
			order: []envelope.Event{ev(1, envelope.KindStarted, ""), ev(2, envelope.KindFailed, `{"error":"provider timeout"}`)},
			want:  render.StatusFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := render.New()
			for _, e := range tc.order {
				d.Apply(e)
			}
			v := d.View()
			if v.Status != tc.want {
				t.Errorf("Status = %q, want %q", v.Status, tc.want)
			}
			if tc.want == render.StatusFailed && v.Error != "provider timeout" {
				t.Errorf("Error = %q, want the failure message", v.Error)
			}
			if v.Terminal() != tc.want.Terminal() {
				t.Errorf("Terminal = %v, want %v", v.Terminal(), tc.want.Terminal())
			}
		})
	}
}

func TestCitationsAreNumberedBySequenceAndDeduplicated(t *testing.T) {
	d := render.New()
	d.Apply(ev(7, envelope.KindCitation, `{"title":"Second","uri":"https://example.com/2"}`))
	d.Apply(ev(3, envelope.KindCitation, `{"title":"First","uri":"https://example.com/1"}`))
	// The same source cited again is one footnote, not two.
	d.Apply(ev(9, envelope.KindCitation, `{"title":"First","uri":"https://example.com/1"}`))

	cites := d.View().Citations
	if len(cites) != 2 {
		t.Fatalf("got %d citations, want 2: %+v", len(cites), cites)
	}
	if cites[0].Title != "First" || cites[0].N != 1 || cites[0].Ref() != "1" {
		t.Errorf("first citation = %+v, want First numbered 1", cites[0])
	}
	if cites[1].Title != "Second" || cites[1].N != 2 {
		t.Errorf("second citation = %+v, want Second numbered 2", cites[1])
	}
}

func TestCitationsAcceptASetAndPreferAnAgentSuppliedLabel(t *testing.T) {
	d := render.New()
	d.Apply(ev(2, envelope.KindCitation, `[
		{"label":"rfc","title":"RFC 9110","url":"https://example.com/rfc"},
		{"citation":"Design doc","source":"https://example.com/d","text":"a snippet"}
	]`))

	cites := d.View().Citations
	if len(cites) != 2 {
		t.Fatalf("got %d citations, want 2", len(cites))
	}
	if cites[0].Ref() != "rfc" || cites[0].URI != "https://example.com/rfc" {
		t.Errorf("citation from url/label = %+v", cites[0])
	}
	if cites[1].Title != "Design doc" || cites[1].URI != "https://example.com/d" || cites[1].Snippet != "a snippet" {
		t.Errorf("citation from citation/source/text = %+v", cites[1])
	}
}

func TestToolCallAndCompletionJoinOnTheirIDInEitherOrder(t *testing.T) {
	tests := []struct {
		name  string
		order []envelope.Event
	}{
		{name: "call then done", order: []envelope.Event{
			ev(1, envelope.KindToolCall, `{"id":"t1","name":"search","args":{"q":  "opentag"}}`),
			ev(2, envelope.KindToolDone, `{"id":"t1","result":"3 hits"}`),
		}},
		{name: "done then call", order: []envelope.Event{
			ev(2, envelope.KindToolDone, `{"id":"t1","result":"3 hits"}`),
			ev(1, envelope.KindToolCall, `{"id":"t1","name":"search","args":{"q":  "opentag"}}`),
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := render.New()
			for _, e := range tc.order {
				d.Apply(e)
			}
			tools := d.View().Tools
			if len(tools) != 1 {
				t.Fatalf("got %d tools, want 1: %+v", len(tools), tools)
			}
			got := tools[0]
			want := render.Tool{ID: "t1", Name: "search", Args: `{"q":"opentag"}`, Result: "3 hits", Done: true}
			if got.ID != want.ID || got.Name != want.Name || got.Args != want.Args ||
				got.Result != want.Result || got.Error != want.Error || got.Done != want.Done {
				t.Errorf("tool = %+v, want %+v", got, want)
			}
		})
	}
}

func TestPayloadsThatAreNotTheDocumentedShapeStillRender(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    string
	}{
		{name: "documented object", payload: `{"text":"hi"}`, want: "hi"},
		{name: "content synonym", payload: `{"content":"hi"}`, want: "hi"},
		{name: "delta synonym", payload: `{"delta":"hi"}`, want: "hi"},
		{name: "json string", payload: `"hi"`, want: "hi"},
		{name: "raw bytes", payload: `hi`, want: "hi"},
		{name: "raw bytes keep their whitespace", payload: " hi ", want: " hi "},
		{name: "empty payload contributes nothing", payload: ``, want: ""},
		{name: "unrecognized object renders as nothing", payload: `{"unknown":"hi"}`, want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := render.New()
			d.Apply(ev(1, envelope.KindText, tc.payload))
			if got := d.View().Text; got != tc.want {
				t.Errorf("Text = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestActionsRenderTheirAddressWhicheverFormItWasMarshalledIn(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    render.Action
	}{
		{
			name:    "address as a uri string",
			payload: `{"summary":"commented","address":"github://urmzd/opentag/issues/42"}`,
			want:    render.Action{Summary: "commented", Address: "github://urmzd/opentag/issues/42"},
		},
		{
			name:    "address as the struct it marshals to",
			payload: `{"summary":"commented","address":{"Connector":"github","Workspace":"urmzd","Path":["opentag","issues","42"]}}`,
			want:    render.Action{Summary: "commented", Address: "github://urmzd/opentag/issues/42"},
		},
		{
			name:    "zero address is dropped, not rendered as a broken link",
			payload: `{"summary":"noted","address":{"Connector":"","Workspace":"","Path":null,"Params":null}}`,
			want:    render.Action{Summary: "noted"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := render.New()
			d.Apply(ev(1, envelope.KindActionTaken, tc.payload))
			actions := d.View().Actions
			if len(actions) != 1 {
				t.Fatalf("got %d actions, want 1", len(actions))
			}
			if actions[0].Summary != tc.want.Summary || actions[0].Address != tc.want.Address {
				t.Errorf("action = %+v, want %+v", actions[0], tc.want)
			}
		})
	}
}

func TestAnUnknownKindReportsNoChangeSoTheSinkSkipsTheRoundTrip(t *testing.T) {
	d := render.New()
	if changed := d.Apply(ev(1, envelope.Kind("delta.something.new"), `{"text":"hi"}`)); changed {
		t.Error("an unknown kind reported a change")
	}
	if v := d.View(); !v.Empty() {
		t.Errorf("an unknown kind contributed to the document: %+v", v)
	}
	// It is still recorded, so a redelivery is cheap and the mark still moves.
	if changed := d.Apply(text(2, "hi")); !changed {
		t.Error("a following text delta was swallowed")
	}
	if got := d.View().Text; got != "hi" {
		t.Errorf("Text = %q, want %q", got, "hi")
	}
}

func TestIdentityIsTakenFromTheFirstEventApplied(t *testing.T) {
	d := render.New()
	d.Apply(text(4, "hi"))
	v := d.View()
	if v.Agent != "docs-bot" || v.RunID != "run_1" || v.Rev != 3 || v.Origin != "slack" || v.Tenant != "acme" {
		t.Errorf("identity = %+v, want the event's agent/run/rev/origin/tenant", v)
	}
	if v.Seq != 4 {
		t.Errorf("Seq = %d, want 4", v.Seq)
	}
}

func TestThinkingAccumulatesSeparatelyFromTheAnswer(t *testing.T) {
	d := render.New()
	d.Apply(ev(1, envelope.KindThinking, `{"text":"let me check "}`))
	d.Apply(text(2, "the answer is 4"))
	d.Apply(ev(3, envelope.KindThinking, `{"text":"the docs"}`))

	v := d.View()
	if v.Text != "the answer is 4" {
		t.Errorf("Text = %q, want the answer only", v.Text)
	}
	if v.Thinking != "let me check the docs" {
		t.Errorf("Thinking = %q, want the reasoning only", v.Thinking)
	}
}

func TestMemoryStaysBoundedByTheWindowUnderASparseStream(t *testing.T) {
	// A route selecting only lifecycle kinds receives a sparse subsequence, so
	// the contiguous rule never fires and only the window bounds the document.
	d := &render.Doc{Window: 8}
	for i := uint64(1); i <= 500; i++ {
		d.Apply(text(i*10, "x"))
	}
	if got := len(d.View().Text); got != 500 {
		t.Fatalf("Text length = %d, want 500 fragments", got)
	}
	// Everything but the newest window has settled, so a redelivery of an old
	// sequence number is still refused.
	if changed := d.Apply(text(10, "x")); changed {
		t.Error("a settled sequence number was applied again")
	}
}

func equalViews(a, b render.View) bool {
	if a.Text != b.Text || a.Thinking != b.Thinking || a.Status != b.Status || a.Error != b.Error {
		return false
	}
	if a.Seq != b.Seq || !a.At.Equal(b.At) {
		return false
	}
	if len(a.Citations) != len(b.Citations) || len(a.Tools) != len(b.Tools) || len(a.Actions) != len(b.Actions) {
		return false
	}
	for i := range a.Citations {
		x, y := a.Citations[i], b.Citations[i]
		if x.N != y.N || x.Ref() != y.Ref() || x.Title != y.Title || x.URI != y.URI || x.Snippet != y.Snippet {
			return false
		}
	}
	for i := range a.Tools {
		x, y := a.Tools[i], b.Tools[i]
		if x.ID != y.ID || x.Name != y.Name || x.Args != y.Args || x.Result != y.Result || x.Error != y.Error || x.Done != y.Done {
			return false
		}
	}
	for i := range a.Actions {
		if a.Actions[i].Summary != b.Actions[i].Summary || a.Actions[i].Address != b.Actions[i].Address {
			return false
		}
	}
	return true
}
