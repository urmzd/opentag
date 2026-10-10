package server

import (
	"context"
	"testing"
	"time"

	"connectrpc.com/connect"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

func tagMsg() *mandatumv1.Tag {
	return &mandatumv1.Tag{
		Id:     "slack-evt-1",
		Agent:  "docs-bot",
		Origin: "slack",
		Text:   "what changed in v2?",
		Actor:  &mandatumv1.Actor{Id: "U1", Display: "Alice"},
		Source: &mandatumv1.Address{
			Connector: "slack",
			Workspace: "T0123",
			Path:      []string{"C0456"},
			Params:    map[string]string{"thread": "1699123456.001"},
		},
		Deliver: []*mandatumv1.Route{{
			Target: &mandatumv1.Address{Connector: "jira", Workspace: "acme", Path: []string{"PROJ-5"}},
			Kinds:  []string{"lifecycle.completed"},
		}},
		Meta: map[string]string{"channel_name": "docs"},
	}
}

// A tag reaches the runtime exactly as it was sent, apart from the two fields
// the server owns: the tenant, from the credential, and the observation time
// when the trigger left it unset.
func TestInvokeForwardsTheTagWithTheServersTenant(t *testing.T) {
	h := newHarness(t, nil)
	res, err := h.invokeClient(tokenAcme).Invoke(context.Background(), connect.NewRequest(tagMsg()))
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	accepted := h.invoker.accepted()
	if len(accepted) != 1 {
		t.Fatalf("want one accepted tag, got %d", len(accepted))
	}
	tag := accepted[0]
	if tag.Tenant != tenantAcme {
		t.Errorf("tenant: got %q, want %q", tag.Tenant, tenantAcme)
	}
	if !tag.At.Equal(fixedTime) {
		t.Errorf("at: got %s, want the server clock %s", tag.At, fixedTime)
	}
	if tag.ID != "slack-evt-1" || tag.Agent != "docs-bot" || tag.Origin != "slack" {
		t.Errorf("identity fields changed in transit: %+v", tag)
	}
	if got := tag.Source.String(); got != "slack://T0123/C0456?thread=1699123456.001" {
		t.Errorf("source: got %q", got)
	}
	if len(tag.Deliver) != 1 || tag.Deliver[0].Target.String() != "jira://acme/PROJ-5" {
		t.Errorf("routes did not survive: %+v", tag.Deliver)
	}
	if !tag.Deliver[0].Wants(envelope.KindCompleted) || tag.Deliver[0].Wants(envelope.KindText) {
		t.Errorf("route selectivity did not survive: %v", tag.Deliver[0].Kinds)
	}
	if tag.Meta["channel_name"] != "docs" {
		t.Errorf("meta did not survive: %v", tag.Meta)
	}

	// The acknowledgement carries the topic, so a caller never builds it.
	if got := res.Msg.GetTopic(); got != "agent:docs-bot:run_slack-evt-1" {
		t.Errorf("topic: got %q", got)
	}
	if got := res.Msg.GetRev(); got != 7 {
		t.Errorf("rev: got %d, want the pinned 7", got)
	}
	if res.Msg.GetAcceptedAt().AsTime().UTC() != fixedTime {
		t.Errorf("accepted_at: got %s", res.Msg.GetAcceptedAt().AsTime())
	}
}

// A redelivery is answered with the run the first delivery created.
func TestInvokeIsAnsweredIdempotentlyForARedeliveredTag(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	first, err := h.invokeClient(tokenAcme).Invoke(ctx, connect.NewRequest(tagMsg()))
	if err != nil {
		t.Fatalf("first invoke: %v", err)
	}
	second, err := h.invokeClient(tokenAcme).Invoke(ctx, connect.NewRequest(tagMsg()))
	if err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	if first.Msg.GetRunId() != second.Msg.GetRunId() {
		t.Fatalf("redelivery started a second run: %q then %q", first.Msg.GetRunId(), second.Msg.GetRunId())
	}
}

// A tag missing what makes it a tag is refused by the edge.
func TestUnrunnableTagsAreRefused(t *testing.T) {
	tests := map[string]func(*mandatumv1.Tag){
		"no id":           func(tag *mandatumv1.Tag) { tag.Id = "" },
		"no agent":        func(tag *mandatumv1.Tag) { tag.Agent = "" },
		"no origin":       func(tag *mandatumv1.Tag) { tag.Origin = "" },
		"unusable source": func(tag *mandatumv1.Tag) { tag.Source.Workspace = "not a workspace" },
		"route no target": func(tag *mandatumv1.Tag) { tag.Deliver[0].Target = nil },
		"unusable route":  func(tag *mandatumv1.Tag) { tag.Deliver[0].Target.Connector = "" },
	}
	h := newHarness(t, nil)
	for name, break_ := range tests {
		t.Run(name, func(t *testing.T) {
			tag := tagMsg()
			break_(tag)
			_, err := h.invokeClient(tokenAcme).Invoke(context.Background(), connect.NewRequest(tag))
			requireCode(t, err, connect.CodeInvalidArgument)
		})
	}
}

// InvokeStream ends when the run reaches a terminal lifecycle event, and ends
// cleanly: the client sees the terminal event and then EOF.
//
// The first event is published before the call, because that is the race the
// endpoint exists to close: a run can emit before its caller has subscribed, and
// the retained window is what makes those events arrive anyway.
func TestInvokeStreamEndsOnTheTerminalLifecycleEvent(t *testing.T) {
	tests := map[string]envelope.Kind{
		"completed": envelope.KindCompleted,
		"failed":    envelope.KindFailed,
	}
	for name, terminal := range tests {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			h.publish(event(tenantAcme, "run_slack-evt-1", 1, envelope.KindStarted, `{}`))

			stream, err := h.invokeClient(tokenAcme).InvokeStream(ctx, connect.NewRequest(tagMsg()))
			if err != nil {
				t.Fatalf("invoke stream: %v", err)
			}
			defer func() { _ = stream.Close() }()

			var kinds []string
			if !stream.Receive() {
				t.Fatalf("want the event emitted before the subscription, got %v", stream.Err())
			}
			kinds = append(kinds, stream.Msg().GetKind())

			h.publish(event(tenantAcme, "run_slack-evt-1", 2, envelope.KindText, `{"text":"hi"}`))
			h.publish(event(tenantAcme, "run_slack-evt-1", 3, terminal, `{}`))
			// Anything after a terminal event belongs to nobody: the stream is
			// already over.
			h.publish(event(tenantAcme, "run_slack-evt-1", 4, envelope.KindText, `{"text":"late"}`))

			for stream.Receive() {
				kinds = append(kinds, stream.Msg().GetKind())
			}
			if err := stream.Err(); err != nil {
				t.Fatalf("stream ended with an error: %v", err)
			}
			want := []string{string(envelope.KindStarted), string(envelope.KindText), string(terminal)}
			if len(kinds) != len(want) {
				t.Fatalf("want the stream to end at the terminal event, got kinds %v", kinds)
			}
			for i := range want {
				if kinds[i] != want[i] {
					t.Fatalf("event %d: got %q, want %q", i, kinds[i], want[i])
				}
			}
			eventually(t, "the bus subscription to be released", func() bool { return h.bus.openStreams() == 0 })
		})
	}
}

// InvokeStream narrows to the run it accepted, so a busy agent's other runs do
// not arrive on a caller's stream.
func TestInvokeStreamCarriesOnlyItsOwnRun(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	h.publish(event(tenantAcme, "run_other-tag", 1, envelope.KindText, `{"text":"not yours"}`))
	h.publish(event(tenantAcme, "run_slack-evt-1", 1, envelope.KindCompleted, `{}`))

	stream, err := h.invokeClient(tokenAcme).InvokeStream(ctx, connect.NewRequest(tagMsg()))
	if err != nil {
		t.Fatalf("invoke stream: %v", err)
	}
	defer func() { _ = stream.Close() }()

	if !stream.Receive() {
		t.Fatalf("want an event, got %v", stream.Err())
	}
	if got := stream.Msg().GetRunId(); got != "run_slack-evt-1" {
		t.Fatalf("another run's events reached the stream: run %q", got)
	}
	if stream.Receive() {
		t.Fatalf("want the stream to end, got %v", stream.Msg())
	}

	subs := h.bus.subscriptions()
	if len(subs) != 1 || subs[0].Topic.String() != "agent:docs-bot:run_slack-evt-1" {
		t.Fatalf("want a subscription on the run topic, got %+v", subs)
	}
}

// A run's record is rendered whole, and a run belonging to another tenant is
// reported as absent even by a reader that ignored the tenant it was given.
func TestGetRunIsScopedToTheCallersTenant(t *testing.T) {
	h := newHarness(t, nil)
	h.runs.ignoreTenant = true
	h.runs.put(Run{
		ID:        "run_1",
		Tenant:    tenantAcme,
		Agent:     "docs-bot",
		Rev:       7,
		Origin:    "slack",
		Topic:     topic.MustParse("agent:docs-bot:run_1"),
		Status:    StatusCompleted,
		Tag:       envelope.Tag{ID: "slack-evt-1", Agent: "docs-bot", Origin: "slack", Tenant: tenantAcme},
		LastSeq:   12,
		CreatedAt: fixedTime,
		EndedAt:   fixedTime.Add(time.Minute),
	})
	h.runs.put(Run{ID: "run_2", Tenant: tenantOther, Agent: "docs-bot", Status: StatusRunning})

	got, err := h.invokeClient(tokenAcme).GetRun(context.Background(), connect.NewRequest(&mandatumv1.GetRunRequest{RunId: "run_1"}))
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Msg.GetStatus() != mandatumv1.RunStatus_RUN_STATUS_COMPLETED {
		t.Errorf("status: got %v", got.Msg.GetStatus())
	}
	if got.Msg.GetLastSeq() != 12 {
		t.Errorf("last_seq: got %d, want the cursor 12", got.Msg.GetLastSeq())
	}
	if got.Msg.GetTag().GetId() != "slack-evt-1" {
		t.Errorf("the run's tag was not carried verbatim: %v", got.Msg.GetTag())
	}
	if got.Msg.GetTenant() != tenantAcme {
		t.Errorf("tenant: got %q", got.Msg.GetTenant())
	}
	if got.Msg.GetStartedAt() != nil {
		t.Errorf("a run that never started must not report a start time, got %v", got.Msg.GetStartedAt().AsTime())
	}

	_, err = h.invokeClient(tokenAcme).GetRun(context.Background(), connect.NewRequest(&mandatumv1.GetRunRequest{RunId: "run_2"}))
	requireCode(t, err, connect.CodeNotFound)

	_, err = h.invokeClient(tokenAcme).GetRun(context.Background(), connect.NewRequest(&mandatumv1.GetRunRequest{}))
	requireCode(t, err, connect.CodeInvalidArgument)
}

// A runtime that refuses a tag is reported as refusing it, and its refusal is
// classified rather than flattened into an internal error.
func TestARuntimeRefusalReachesTheClientClassified(t *testing.T) {
	h := newHarness(t, nil)
	h.invoker.err = ErrUnavailable
	_, err := h.invokeClient(tokenAcme).Invoke(context.Background(), connect.NewRequest(tagMsg()))
	requireCode(t, err, connect.CodeUnavailable)
}
