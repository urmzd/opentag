package envelope_test

import (
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

func TestKindFamilyAndMatches(t *testing.T) {
	cases := []struct {
		kind   envelope.Kind
		family string
		sel    string
		want   bool
	}{
		{envelope.KindText, "delta", "delta", true},
		{envelope.KindCitation, "delta", "delta", true},
		{envelope.KindCitation, "delta", "delta.citation", true},
		{envelope.KindCitation, "delta", "delta.text", false},
		{envelope.KindCompleted, "lifecycle", "delta", false},
		{envelope.KindCompleted, "lifecycle", "lifecycle", true},
		{envelope.KindCompleted, "lifecycle", "lifecycle.completed", true},
		{envelope.KindActionTaken, "action", "action", true},
		{envelope.KindText, "delta", "", true},
	}
	for _, tc := range cases {
		if got := tc.kind.Family(); got != tc.family {
			t.Errorf("%s.Family() = %q, want %q", tc.kind, got, tc.family)
		}
		if got := tc.kind.Matches(tc.sel); got != tc.want {
			t.Errorf("%s.Matches(%q) = %v, want %v", tc.kind, tc.sel, got, tc.want)
		}
	}
}

// The N:M case from the design: one run, two sinks, each choosing a different
// granularity. GitHub renders the live stream; the webhook only learns it
// shipped.
func TestRouteSelectivity(t *testing.T) {
	stream := envelope.Route{
		Target: address.MustParse("github://urmzd/mandatum/issues/42"),
		Kinds:  []string{"delta", "lifecycle.completed"},
	}
	notify := envelope.Route{
		Target: address.MustParse("webhook://acme/deploys"),
		Kinds:  []string{"lifecycle.completed"},
	}
	everything := envelope.Route{Target: address.MustParse("slack://T01/C02")}

	cases := []struct {
		name                            string
		kind                            envelope.Kind
		wantStream, wantNotify, wantAll bool
	}{
		{"text delta", envelope.KindText, true, false, true},
		{"citation", envelope.KindCitation, true, false, true},
		{"started", envelope.KindStarted, false, false, true},
		{"completed", envelope.KindCompleted, true, true, true},
		{"failed", envelope.KindFailed, false, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := stream.Wants(tc.kind); got != tc.wantStream {
				t.Errorf("stream.Wants(%s) = %v, want %v", tc.kind, got, tc.wantStream)
			}
			if got := notify.Wants(tc.kind); got != tc.wantNotify {
				t.Errorf("notify.Wants(%s) = %v, want %v", tc.kind, got, tc.wantNotify)
			}
			if got := everything.Wants(tc.kind); got != tc.wantAll {
				t.Errorf("everything.Wants(%s) = %v, want %v", tc.kind, got, tc.wantAll)
			}
		})
	}
}

func TestTagValidate(t *testing.T) {
	valid := envelope.Tag{
		ID: "ev1", Agent: "docs-bot", Origin: "github",
		Source: address.MustParse("github://urmzd/mandatum/issues/42"),
		At:     time.Now(),
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid tag rejected: %v", err)
	}

	missingID := valid
	missingID.ID = ""
	if err := missingID.Validate(); err == nil {
		t.Error("tag without an id accepted; it is the idempotency key")
	}

	missingAgent := valid
	missingAgent.Agent = ""
	if err := missingAgent.Validate(); err == nil {
		t.Error("tag without an agent accepted")
	}

	missingOrigin := valid
	missingOrigin.Origin = ""
	if err := missingOrigin.Validate(); err == nil {
		t.Error("tag without an origin accepted")
	}

	badRoute := valid
	badRoute.Deliver = []envelope.Route{{}}
	if err := badRoute.Validate(); err == nil {
		t.Error("tag with a targetless route accepted")
	}
}

// A tag raised in GitHub delivering into Jira: origin and destination move
// independently, which is the whole point of the mesh.
func TestRoutesDecoupleOriginFromDestination(t *testing.T) {
	tag := envelope.Tag{
		ID: "ev1", Agent: "review-bot", Origin: "github",
		Source:  address.MustParse("github://urmzd/mandatum/pull/7"),
		Deliver: []envelope.Route{{Target: address.MustParse("jira://acme/PROJ-5")}},
	}
	routes := tag.Routes()
	if len(routes) != 1 {
		t.Fatalf("Routes() returned %d routes, want 1", len(routes))
	}
	if got := routes[0].Target.Connector; got != "jira" {
		t.Fatalf("route target connector = %q, want jira", got)
	}
}

// With no explicit routes, the answer goes back where the tag came from.
func TestRoutesFallBackToSource(t *testing.T) {
	tag := envelope.Tag{
		ID: "ev1", Agent: "docs-bot", Origin: "slack",
		Source: address.MustParse("slack://T01/C02?thread=1699.001"),
	}
	routes := tag.Routes()
	if len(routes) != 1 {
		t.Fatalf("Routes() returned %d routes, want 1", len(routes))
	}
	if routes[0].Target.String() != tag.Source.String() {
		t.Fatalf("fallback route = %q, want source %q", routes[0].Target, tag.Source)
	}
	if !routes[0].Wants(envelope.KindText) {
		t.Error("fallback route should want every kind")
	}
}

// A cron-triggered tag has no surface to answer into, so it produces no
// routes unless it declares them. It still runs and still reaches the bus.
func TestCronTagWithoutRoutesHasNoDestination(t *testing.T) {
	tag := envelope.Tag{ID: "occ-1", Agent: "nightly", Origin: "cron"}
	if got := tag.Routes(); len(got) != 0 {
		t.Fatalf("Routes() = %v, want none", got)
	}
	if err := tag.Validate(); err != nil {
		t.Fatalf("a routeless cron tag should still be valid: %v", err)
	}
}

func newEvent(seq uint64, agent, runID string, rev int, origin string, kind envelope.Kind) envelope.Event {
	return envelope.Event{
		Seq:    seq,
		Topic:  topic.MustParse("agent:" + agent + ":" + runID),
		RunID:  runID,
		Agent:  agent,
		Rev:    rev,
		Origin: origin,
		Kind:   kind,
	}
}

func TestFilterZeroAllowsEverything(t *testing.T) {
	var f envelope.Filter
	for _, k := range []envelope.Kind{envelope.KindText, envelope.KindCompleted, envelope.KindActionTaken} {
		if !f.Allows(newEvent(1, "docs-bot", "run1", 3, "slack", k)) {
			t.Errorf("zero Filter rejected %s", k)
		}
	}
}

func TestFilterNarrowsWithoutChangingTopic(t *testing.T) {
	rev7 := newEvent(1, "docs-bot", "run1", 7, "slack", envelope.KindText)
	rev6 := newEvent(1, "docs-bot", "run2", 6, "slack", envelope.KindText)

	canary := envelope.Filter{Rev: 7}
	if !canary.Allows(rev7) {
		t.Error("canary filter rejected rev 7")
	}
	if canary.Allows(rev6) {
		t.Error("canary filter accepted rev 6")
	}

	citations := envelope.Filter{Kinds: []string{"delta.citation"}}
	if citations.Allows(rev7) {
		t.Error("citation filter accepted a text delta")
	}
	if !citations.Allows(newEvent(2, "docs-bot", "run1", 7, "slack", envelope.KindCitation)) {
		t.Error("citation filter rejected a citation")
	}

	fromGitHub := envelope.Filter{Origin: "github"}
	if fromGitHub.Allows(rev7) {
		t.Error("origin filter accepted a slack-origin event")
	}

	oneRun := envelope.Filter{RunID: "run1"}
	if !oneRun.Allows(rev7) || oneRun.Allows(rev6) {
		t.Error("run filter did not isolate run1")
	}
}

// Listening to an agent means receiving every run on one subscription, with
// From acting as the resume cursor.
func TestSubscriptionAcceptsAndResumes(t *testing.T) {
	sub := envelope.Subscription{Topic: topic.MustParse("agent:docs-bot")}

	for _, runID := range []string{"run1", "run2"} {
		if !sub.Accepts(newEvent(1, "docs-bot", runID, 1, "slack", envelope.KindText)) {
			t.Errorf("agent subscription missed %s", runID)
		}
	}
	if sub.Accepts(newEvent(1, "triage-bot", "run9", 1, "slack", envelope.KindText)) {
		t.Error("agent subscription accepted another agent's event")
	}

	resumed := envelope.Subscription{Topic: topic.MustParse("agent:docs-bot"), From: 5}
	if resumed.Accepts(newEvent(5, "docs-bot", "run1", 1, "slack", envelope.KindText)) {
		t.Error("resume cursor redelivered the last processed event")
	}
	if !resumed.Accepts(newEvent(6, "docs-bot", "run1", 1, "slack", envelope.KindText)) {
		t.Error("resume cursor dropped the next event")
	}
}
