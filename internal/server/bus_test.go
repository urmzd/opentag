package server

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
	"strings"
)

func subscribeReq(t string, filter *mandatumv1.Filter, from uint64) *connect.Request[mandatumv1.SubscribeRequest] {
	return connect.NewRequest(&mandatumv1.SubscribeRequest{
		Subscription: &mandatumv1.Subscription{Topic: t, Filter: filter, From: from},
	})
}

// The bus addresses and does not authorize, so the edge is what keeps one
// tenant's events out of another's stream — even when both are published under
// topics the subscription covers.
func TestSubscribeDeliversOnlyTheCallersTenant(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.publish(event(tenantOther, "run_other", 1, envelope.KindText, `{"text":"secret"}`))
	h.publish(event(tenantAcme, "run_acme", 1, envelope.KindText, `{"text":"mine"}`))

	stream, err := h.busClient(tokenAcme).Subscribe(ctx, subscribeReq("agent:docs-bot", nil, 0))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = stream.Close() }()

	if !stream.Receive() {
		t.Fatalf("want an event, got %v", stream.Err())
	}
	got := stream.Msg()
	if got.GetTenant() != tenantAcme || got.GetRunId() != "run_acme" {
		t.Fatalf("want the caller's own event, got tenant %q run %q", got.GetTenant(), got.GetRunId())
	}
	// Nothing else is retained for this tenant, so a second event can only be
	// the other tenant's leaking through.
	h.publish(event(tenantOther, "run_other", 2, envelope.KindText, `{"text":"secret"}`))
	h.publish(event(tenantAcme, "run_acme", 2, envelope.KindCompleted, `{}`))
	if !stream.Receive() {
		t.Fatalf("want the second event, got %v", stream.Err())
	}
	if got := stream.Msg(); got.GetTenant() != tenantAcme {
		t.Fatalf("another tenant's event reached the stream: %v", got)
	}
}

// From is a resume cursor and Filter narrows what is delivered; both are honoured
// as fields of one subscription over one topic.
func TestSubscribeHonoursTheCursorAndTheFilter(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.publish(event(tenantAcme, "run_1", 1, envelope.KindText, `{"text":"one"}`))
	h.publish(event(tenantAcme, "run_1", 2, envelope.KindThinking, `{"text":"hmm"}`))
	h.publish(event(tenantAcme, "run_1", 3, envelope.KindCitation, `{"source":"docs"}`))
	h.publish(event(tenantAcme, "run_1", 4, envelope.KindCompleted, `{}`))

	stream, err := h.busClient(tokenAcme).Subscribe(ctx,
		subscribeReq("agent:docs-bot:run_1", &mandatumv1.Filter{Kinds: []string{"lifecycle.completed", "delta.citation"}}, 2))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = stream.Close() }()

	var kinds []string
	for range 2 {
		if !stream.Receive() {
			t.Fatalf("want two events, got %v", stream.Err())
		}
		if got := stream.Msg().GetSeq(); got <= 2 {
			t.Fatalf("cursor ignored: got seq %d, want > 2", got)
		}
		kinds = append(kinds, stream.Msg().GetKind())
	}
	if kinds[0] != string(envelope.KindCitation) || kinds[1] != string(envelope.KindCompleted) {
		t.Fatalf("filter ignored: got kinds %v", kinds)
	}
}

// Publish assigns the three fields a client must not: tenant, sequence, and
// time. A client that sets all three gets none of them back.
func TestPublishOverwritesTheFieldsTheServerOwns(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watch, err := h.bus.Subscribe(ctx, envelope.Subscription{Topic: topic.MustParse("agent:docs-bot:run_1")})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}
	defer func() { _ = watch.Close() }()

	_, err = h.busClient(tokenAcme).Publish(ctx, connect.NewRequest(&mandatumv1.PublishRequest{
		Event: &mandatumv1.Event{
			Topic:   "agent:docs-bot:run_1",
			Kind:    string(envelope.KindActionTaken),
			Payload: []byte(`{"summary":"commented"}`),
			// All three are claims the server must ignore.
			Tenant: tenantOther,
			Seq:    9999,
			At:     timestamppb.New(fixedTime.AddDate(-1, 0, 0)),
		},
	}))
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	got, err := watch.Recv(ctx)
	if err != nil {
		t.Fatalf("recv: %v", err)
	}
	if got.Tenant != tenantAcme {
		t.Errorf("tenant: got %q, want the credential's %q", got.Tenant, tenantAcme)
	}
	if got.Seq != 1 {
		t.Errorf("seq: got %d, want the broker's 1 — a client must not be able to park a run's sequence space", got.Seq)
	}
	if !got.At.Equal(fixedTime) {
		t.Errorf("at: got %s, want the server clock %s", got.At, fixedTime)
	}
}

// A broker that can report the sequence it assigned does, and one that cannot
// answers zero rather than inventing a number.
func TestPublishReportsASequenceOnlyWhenTheBrokerCan(t *testing.T) {
	t.Run("plain broker", func(t *testing.T) {
		h := newHarness(t, nil)
		res, err := h.busClient(tokenAcme).Publish(context.Background(), connect.NewRequest(&mandatumv1.PublishRequest{
			Event: &mandatumv1.Event{Topic: "agent:docs-bot:run_1", Kind: string(envelope.KindText)},
		}))
		if err != nil {
			t.Fatalf("publish: %v", err)
		}
		if res.Msg.GetSeq() != 0 {
			t.Fatalf("want seq 0 from a broker that cannot report one, got %d", res.Msg.GetSeq())
		}
		if res.Msg.GetAt().AsTime().UTC() != fixedTime {
			t.Fatalf("want the stamped time %s, got %s", fixedTime, res.Msg.GetAt().AsTime())
		}
	})

	t.Run("sequencing broker", func(t *testing.T) {
		sequencing := &sequencingBus{fakeBus: newFakeBus()}
		h := newHarness(t, func(cfg *Config) { cfg.Bus = sequencing })
		for want := uint64(1); want <= 2; want++ {
			res, err := h.busClient(tokenAcme).Publish(context.Background(), connect.NewRequest(&mandatumv1.PublishRequest{
				Event: &mandatumv1.Event{Topic: "agent:docs-bot:run_1", Kind: string(envelope.KindText)},
			}))
			if err != nil {
				t.Fatalf("publish: %v", err)
			}
			if res.Msg.GetSeq() != want {
				t.Fatalf("want assigned seq %d, got %d", want, res.Msg.GetSeq())
			}
		}
	})
}

// An event the bus cannot carry is refused as a bad request, not reported as a
// server fault.
func TestUnpublishableEventsAreRefusedAsBadRequests(t *testing.T) {
	tests := map[string]*mandatumv1.Event{
		"no topic":             {Kind: string(envelope.KindText)},
		"topic is not a run":   {Topic: "agent:docs-bot", Kind: string(envelope.KindText)},
		"no kind":              {Topic: "agent:docs-bot:run_1"},
		"topic is not a topic": {Topic: "not-an-agent-topic:x", Kind: string(envelope.KindText)},
		"run id disagrees": {
			Topic: "agent:docs-bot:run_1",
			RunId: "run_2",
			Kind:  string(envelope.KindText),
		},
	}
	h := newHarness(t, nil)
	for name, e := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := h.busClient(tokenAcme).Publish(context.Background(),
				connect.NewRequest(&mandatumv1.PublishRequest{Event: e}))
			requireCode(t, err, connect.CodeInvalidArgument)
		})
	}
	t.Run("no event at all", func(t *testing.T) {
		_, err := h.busClient(tokenAcme).Publish(context.Background(), connect.NewRequest(&mandatumv1.PublishRequest{}))
		requireCode(t, err, connect.CodeInvalidArgument)
	})
}

// A subscription released by the client releases its bus stream: the handler
// holds nothing once the caller has gone.
func TestSubscribeReleasesItsStreamWhenTheClientGoesAway(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())

	h.publish(event(tenantAcme, "run_1", 1, envelope.KindText, `{"text":"one"}`))
	stream, err := h.busClient(tokenAcme).Subscribe(ctx, subscribeReq("agent:docs-bot", nil, 0))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if !stream.Receive() {
		t.Fatalf("want an event, got %v", stream.Err())
	}
	if got := h.bus.openStreams(); got != 1 {
		t.Fatalf("want one open bus stream while subscribed, got %d", got)
	}

	cancel()
	_ = stream.Close()
	eventually(t, "the bus subscription to be released", func() bool { return h.bus.openStreams() == 0 })
}

// A malformed subscription is rejected before any stream is opened.
func TestUnaddressableSubscriptionsAreRefused(t *testing.T) {
	h := newHarness(t, nil)
	tests := map[string]*mandatumv1.SubscribeRequest{
		"no subscription": {},
		"empty topic":     {Subscription: &mandatumv1.Subscription{}},
		"foreign topic":   {Subscription: &mandatumv1.Subscription{Topic: "run:docs-bot"}},
		"too deep":        {Subscription: &mandatumv1.Subscription{Topic: "agent:docs-bot:run_1:extra"}},
	}
	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			stream, err := h.busClient(tokenAcme).Subscribe(context.Background(), connect.NewRequest(req))
			if err == nil {
				// A server-stream error can surface on the first Receive.
				stream.Receive()
				err = stream.Err()
				_ = stream.Close()
			}
			requireCode(t, err, connect.CodeInvalidArgument)
			if got := h.bus.openStreams(); got != 0 {
				t.Fatalf("a rejected subscription left %d bus streams open", got)
			}
		})
	}
}

// A consumer that fell behind learns it, with the status that says "resubscribe
// from your cursor" rather than a silent gap in its stream.
func TestSubscribeReportsASlowConsumer(t *testing.T) {
	h := newHarness(t, func(cfg *Config) {
		cfg.Bus = &failingBus{fakeBus: newFakeBus(), err: bus.ErrSlowConsumer}
	})
	stream, err := h.busClient(tokenAcme).Subscribe(context.Background(), subscribeReq("agent:docs-bot", nil, 0))
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer func() { _ = stream.Close() }()
	if stream.Receive() {
		t.Fatalf("want no events, got %v", stream.Msg())
	}
	requireCode(t, stream.Err(), connect.CodeResourceExhausted)
}

// A publish the broker refuses is reported, and an unclassified refusal is
// reported without its detail.
func TestPublishReportsABrokerFailure(t *testing.T) {
	h := newHarness(t, nil)
	h.bus.publishErr = errInjected
	_, err := h.busClient(tokenAcme).Publish(context.Background(), connect.NewRequest(&mandatumv1.PublishRequest{
		Event: &mandatumv1.Event{Topic: "agent:docs-bot:run_1", Kind: string(envelope.KindText)},
	}))
	requireCode(t, err, connect.CodeInternal)
	if strings.Contains(err.Error(), errInjected.Error()) {
		t.Fatalf("the failure leaked its detail: %v", err)
	}
}
