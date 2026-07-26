package webhook_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/connector"
	"github.com/urmzd/opentag/pkg/connectors/webhook"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/signature"
	"github.com/urmzd/opentag/pkg/topic"
)

const secret = "an outbound signing secret"

var target = address.MustParse("webhook://acme/deploys")

// ── Signing ─────────────────────────────────────────────────────────

func TestTheOutboundSignatureVerifiesUnderPkgSignaturesScheme(t *testing.T) {
	// The point of reusing Slack's base string is that a receiver can
	// authenticate us with a verifier it already has. This test IS that
	// receiver: pkg/signature's real Slack verifier, over our request, with the
	// two headers renamed.
	rec := newReceiver(t, http.StatusOK)
	signedAt := time.Unix(1784600000, 0)
	c := connectorAt(t, rec.URL, func(cfg *webhook.Config) {
		cfg.Now = func() time.Time { return signedAt }
	})

	if err := c.Deliver(context.Background(), target, textEvent(1, "hello")); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	got := rec.only(t)
	verifier := &signature.Slack{Secret: []byte(secret), Now: func() time.Time { return signedAt }}
	header := http.Header{}
	header.Set(signature.HeaderSlackSignature, got.header.Get(webhook.HeaderSignature))
	header.Set(signature.HeaderSlackTimestamp, got.header.Get(webhook.HeaderTimestamp))
	if err := verifier.Verify(header, got.body); err != nil {
		t.Fatalf("the outbound signature does not verify under pkg/signature: %v", err)
	}

	// The timestamp is inside the digest, which is the whole reason it is there:
	// a captured request cannot be aged forward.
	replayed := &signature.Slack{Secret: []byte(secret), Now: func() time.Time { return signedAt.Add(time.Hour) }}
	if err := replayed.Verify(header, got.body); !errors.Is(err, signature.ErrTimestampSkew) {
		t.Errorf("an hour-old capture verified with %v, want ErrTimestampSkew", err)
	}

	// And a tampered body does not verify, because the digest covers the bytes.
	if err := verifier.Verify(header, append(got.body, ' ')); !errors.Is(err, signature.ErrSignatureMismatch) {
		t.Errorf("a tampered body verified with %v, want ErrSignatureMismatch", err)
	}
}

func TestASignatureIsOnlyGoodForTheSecretItWasMadeWith(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	c := connectorAt(t, rec.URL, nil)
	if err := c.Deliver(context.Background(), target, textEvent(1, "hello")); err != nil {
		t.Fatal(err)
	}
	got := rec.only(t)

	wrong := &signature.Slack{Secret: []byte("another receiver's secret"), Now: time.Now}
	header := http.Header{}
	header.Set(signature.HeaderSlackSignature, got.header.Get(webhook.HeaderSignature))
	header.Set(signature.HeaderSlackTimestamp, got.header.Get(webhook.HeaderTimestamp))
	if err := wrong.Verify(header, got.body); !errors.Is(err, signature.ErrSignatureMismatch) {
		t.Errorf("error = %v, want ErrSignatureMismatch: two receivers must not be able to forge each other", err)
	}
}

// ── What lands on the wire ──────────────────────────────────────────

func TestTheBodyCarriesTheEventWithAJsonPayloadInline(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	c := connectorAt(t, rec.URL, nil)

	e := textEvent(7, "the retry path posts twice")
	e.Tenant, e.Rev = "acme", 3
	if err := c.Deliver(context.Background(), target, e); err != nil {
		t.Fatal(err)
	}

	got := rec.only(t)
	if got.method != http.MethodPost {
		t.Errorf("method = %s, want POST", got.method)
	}
	if got.path != "/hooks/deploys" {
		t.Errorf("path = %q, want the endpoint base plus the address path", got.path)
	}
	if ct := got.header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q", ct)
	}
	for header, want := range map[string]string{
		webhook.HeaderDelivery: "run_1/7",
		webhook.HeaderEvent:    string(envelope.KindText),
		webhook.HeaderTopic:    "agent:docs-bot:run_1",
		"X-Receiver-Key":       "configured",
	} {
		if have := got.header.Get(header); have != want {
			t.Errorf("%s = %q, want %q", header, have, want)
		}
	}

	var body webhook.Body
	if err := json.Unmarshal(got.body, &body); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	if body.Seq != 7 || body.RunID != "run_1" || body.Agent != "docs-bot" || body.Rev != 3 || body.Tenant != "acme" {
		t.Errorf("body = %+v", body)
	}
	if body.Kind != string(envelope.KindText) || body.Origin != "cron" {
		t.Errorf("kind/origin = %q/%q", body.Kind, body.Origin)
	}
	// The payload arrives as an object, not as a base64 string a receiver would
	// have to decode before it could read one field of it.
	var payload struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(body.Payload, &payload); err != nil {
		t.Fatalf("payload is not inline JSON: %v", err)
	}
	if payload.Text != "the retry path posts twice" {
		t.Errorf("payload.text = %q", payload.Text)
	}
	if len(body.PayloadB64) != 0 {
		t.Error("a JSON payload was also base64 encoded, so a receiver has two places to look")
	}
}

func TestAPayloadThatIsNotJsonTravelsInItsOwnField(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	c := connectorAt(t, rec.URL, nil)

	e := event(1, envelope.KindText, "\xff\xfe raw bytes")
	if err := c.Deliver(context.Background(), target, e); err != nil {
		t.Fatal(err)
	}
	var body webhook.Body
	if err := json.Unmarshal(rec.only(t).body, &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Payload) != 0 {
		t.Errorf("payload = %s, want the non-JSON field instead", body.Payload)
	}
	if string(body.PayloadB64) != "\xff\xfe raw bytes" {
		t.Errorf("payload_b64 = %q", body.PayloadB64)
	}
}

// ── Failure policy ──────────────────────────────────────────────────

func TestATransientFailureIsRetriedUntilItClears(t *testing.T) {
	for _, status := range []int{
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusTooManyRequests,
		http.StatusRequestTimeout,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			rec := newReceiver(t, status, status, http.StatusOK)
			c := connectorAt(t, rec.URL, nil)
			if err := c.Deliver(context.Background(), target, textEvent(1, "hi")); err != nil {
				t.Fatalf("Deliver: %v", err)
			}
			if n := rec.count(); n != 3 {
				t.Errorf("attempts = %d, want 3: two failures then success", n)
			}
		})
	}
}

func TestAPermanentFailureIsNotRetriedAndSaysSo(t *testing.T) {
	// A 4xx that is not about timing is a statement about the request, and the
	// request will be identical next time. Repeating it only spends the
	// receiver's quota — and the router must not spend its own budget either,
	// which is what ErrUndeliverable tells it.
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusUnprocessableEntity,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			rec := newReceiver(t, status, http.StatusOK, http.StatusOK)
			c := connectorAt(t, rec.URL, nil)
			err := c.Deliver(context.Background(), target, textEvent(1, "hi"))
			if !errors.Is(err, connector.ErrUndeliverable) {
				t.Fatalf("error = %v, want ErrUndeliverable", err)
			}
			if n := rec.count(); n != 1 {
				t.Errorf("attempts = %d, want exactly 1", n)
			}
		})
	}
}

func TestRetriesAreBoundedAndTheLastFailureIsReported(t *testing.T) {
	rec := newReceiver(t, http.StatusBadGateway, http.StatusBadGateway, http.StatusBadGateway, http.StatusOK)
	c := connectorAt(t, rec.URL, nil)

	err := c.Deliver(context.Background(), target, textEvent(1, "hi"))
	if err == nil {
		t.Fatal("Deliver reported success after exhausting its attempts")
	}
	if errors.Is(err, connector.ErrUndeliverable) {
		t.Error("an exhausted retry was reported as permanent, which would stop the router retrying too")
	}
	if n := rec.count(); n != 3 {
		t.Errorf("attempts = %d, want DefaultAttempts of 3", n)
	}
	if !strings.Contains(err.Error(), "502") {
		t.Errorf("error = %v, want the receiver's status in it", err)
	}
}

func TestARetryRepeatsTheDeliveryIdAndRefreshesTheTimestamp(t *testing.T) {
	// The delivery id is what the receiver dedupes on, so it must be identical
	// across attempts. The timestamp must not be: a retry four minutes later
	// would otherwise fall outside the receiver's replay window.
	rec := newReceiver(t, http.StatusServiceUnavailable, http.StatusOK)
	clock := &movingClock{at: time.Unix(1784600000, 0), step: 5 * time.Minute}
	c := connectorAt(t, rec.URL, func(cfg *webhook.Config) { cfg.Now = clock.now })

	if err := c.Deliver(context.Background(), target, textEvent(4, "hi")); err != nil {
		t.Fatal(err)
	}
	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("attempts = %d, want 2", len(got))
	}
	if a, b := got[0].header.Get(webhook.HeaderDelivery), got[1].header.Get(webhook.HeaderDelivery); a != b || a != "run_1/4" {
		t.Errorf("delivery ids = %q and %q, want both to be run_1/4", a, b)
	}
	if a, b := got[0].header.Get(webhook.HeaderTimestamp), got[1].header.Get(webhook.HeaderTimestamp); a == b {
		t.Errorf("both attempts signed timestamp %s, so a retry would age out of the receiver's window", a)
	}
	if a, b := got[0].header.Get(webhook.HeaderSignature), got[1].header.Get(webhook.HeaderSignature); a == b {
		t.Error("the signature did not change with the timestamp, so one of them is not in the digest")
	}
	// The retried attempt is still authentic under its own timestamp.
	verifier := &signature.Slack{Secret: []byte(secret), Now: func() time.Time { return clock.last() }}
	header := http.Header{}
	header.Set(signature.HeaderSlackSignature, got[1].header.Get(webhook.HeaderSignature))
	header.Set(signature.HeaderSlackTimestamp, got[1].header.Get(webhook.HeaderTimestamp))
	if err := verifier.Verify(header, got[1].body); err != nil {
		t.Errorf("the retried attempt does not verify: %v", err)
	}
}

func TestATimeoutIsHonouredRatherThanHoldingTheDeliveryLoop(t *testing.T) {
	hang := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	}))
	// Cleanups run last-registered-first, so the handler is released before the
	// server is closed. The other order deadlocks: Close waits for the handler,
	// and the handler waits for the channel Close has not reached yet.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(hang) })

	c := connectorAt(t, server.URL, func(cfg *webhook.Config) {
		cfg.Timeout = 50 * time.Millisecond
		cfg.Attempts = 1
	})

	start := time.Now()
	err := c.Deliver(context.Background(), target, textEvent(1, "hi"))
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a receiver that never answers was reported as delivered")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("error = %v, want a deadline", err)
	}
	if elapsed > 2*time.Second {
		t.Errorf("Deliver took %s, so the timeout did not bound it", elapsed)
	}
}

func TestACancelledContextStopsImmediatelyWithoutSleeping(t *testing.T) {
	rec := newReceiver(t, http.StatusServiceUnavailable, http.StatusServiceUnavailable, http.StatusOK)
	c := connectorAt(t, rec.URL, func(cfg *webhook.Config) {
		// Long enough that a retry that ignored cancellation would be obvious.
		cfg.Backoff = 30 * time.Second
		cfg.MaxBackoff = 30 * time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	if err := c.Deliver(ctx, target, textEvent(1, "hi")); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Deliver took %s after cancellation", elapsed)
	}
}

// ── Addressing ──────────────────────────────────────────────────────

func TestDeliveryToATargetThisConnectorDoesNotServeIsRefused(t *testing.T) {
	rec := newReceiver(t, http.StatusOK)
	c := connectorAt(t, rec.URL, nil)

	tests := []struct{ name, target string }{
		{"another connector", "github://urmzd/opentag/issues/1"},
		{"a workspace with no configured endpoint", "webhook://someone-else/deploys"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := c.Deliver(context.Background(), address.MustParse(tc.target), textEvent(1, "hi"))
			if !errors.Is(err, connector.ErrUndeliverable) {
				t.Errorf("error = %v, want ErrUndeliverable", err)
			}
			if rec.count() != 0 {
				t.Error("a refused target still produced a request")
			}
		})
	}
}

func TestTheTargetAddressRoundTripsAndDecidesTheUrl(t *testing.T) {
	uri := "webhook://acme/deploys/prod"
	parsed, err := address.Parse(uri)
	if err != nil {
		t.Fatalf("Parse(%q): %v", uri, err)
	}
	if parsed.String() != uri {
		t.Errorf("round trip = %q, want %q", parsed.String(), uri)
	}

	rec := newReceiver(t, http.StatusOK)
	c := connectorAt(t, rec.URL, nil)
	if err := c.Deliver(context.Background(), parsed, textEvent(1, "hi")); err != nil {
		t.Fatal(err)
	}
	if got := rec.only(t).path; got != "/hooks/deploys/prod" {
		t.Errorf("path = %q, want the whole address path under the endpoint base", got)
	}
}

func TestAPathSegmentCannotClimbOutOfTheEndpointsBase(t *testing.T) {
	// The workspace indirection exists so that a route written in an agent spec
	// cannot name an arbitrary URL. A segment containing a slash must not be
	// able to walk the path either.
	rec := newReceiver(t, http.StatusOK)
	c := connectorAt(t, rec.URL, nil)
	escape := address.Address{Connector: "webhook", Workspace: "acme", Path: []string{"../../admin"}}
	if err := c.Deliver(context.Background(), escape, textEvent(1, "hi")); err != nil {
		t.Fatal(err)
	}
	if got := rec.only(t).rawPath; got != "/hooks/..%2F..%2Fadmin" {
		t.Errorf("escaped path = %q, want the separators still escaped so no hop reads them as path structure", got)
	}
}

// ── Configuration and faces ─────────────────────────────────────────

func TestAnEndpointThatCannotBeUsedIsRefusedAtStartup(t *testing.T) {
	tests := []struct {
		name string
		cfg  webhook.Config
	}{
		{"no endpoints at all", webhook.Config{}},
		{
			name: "an endpoint with no signing secret",
			cfg:  webhook.Config{Endpoints: map[string]webhook.Endpoint{"acme": {BaseURL: "https://example.com"}}},
		},
		{
			name: "a base url that is not absolute",
			cfg:  webhook.Config{Endpoints: map[string]webhook.Endpoint{"acme": {BaseURL: "/hooks", Secret: secret}}},
		},
		{
			name: "a workspace that could not be an address",
			cfg:  webhook.Config{Endpoints: map[string]webhook.Endpoint{"acme.co": {BaseURL: "https://example.com", Secret: secret}}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := webhook.New(tc.cfg); err == nil {
				t.Fatal("New accepted an unusable configuration")
			}
		})
	}
}

func TestWebhookIsASinkAndNothingElse(t *testing.T) {
	c := connectorAt(t, "https://example.com", nil)
	roles := connector.RolesOf(c)
	if !roles.Sink || roles.Trigger || roles.Actor {
		t.Fatalf("RolesOf(webhook) = %+v, want sink only", roles)
	}

	registry := connector.NewRegistry()
	if err := registry.Register(c); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := registry.Sink(target); err != nil {
		t.Errorf("Registry.Sink(webhook://...) = %v, want the connector", err)
	}
	if len(registry.Triggers()) != 0 {
		t.Error("webhook registered as a trigger, and an outbound POST raises nothing")
	}
	if len(registry.Actions()) != 0 {
		t.Error("webhook contributed actions")
	}
}

// ── Helpers ─────────────────────────────────────────────────────────

// request is one POST as the receiver saw it.
type request struct {
	method  string
	path    string
	rawPath string
	header  http.Header
	body    []byte
}

// receiver is a complete receiving end: it records every request and answers
// with the statuses it was given, in order, repeating the last one.
type receiver struct {
	*httptest.Server
	statuses []int

	mu       sync.Mutex
	requests []request
}

func newReceiver(t *testing.T, statuses ...int) *receiver {
	t.Helper()
	r := &receiver{statuses: statuses}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		n := len(r.requests)
		r.requests = append(r.requests, request{
			method:  req.Method,
			path:    req.URL.Path,
			rawPath: req.URL.EscapedPath(),
			header:  req.Header.Clone(),
			body:    body,
		})
		r.mu.Unlock()

		status := http.StatusOK
		if len(r.statuses) > 0 {
			status = r.statuses[min(n, len(r.statuses)-1)]
		}
		w.WriteHeader(status)
		if status >= 400 {
			_, _ = w.Write([]byte(`{"error":"receiver said no"}`))
		}
	}))
	t.Cleanup(r.Close)
	return r
}

func (r *receiver) all() []request {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]request(nil), r.requests...)
}

func (r *receiver) count() int { return len(r.all()) }

func (r *receiver) only(t *testing.T) request {
	t.Helper()
	got := r.all()
	if len(got) != 1 {
		t.Fatalf("the receiver saw %d requests, want exactly 1", len(got))
	}
	return got[0]
}

// movingClock advances by a fixed step on every read, so a retry is guaranteed
// to sign a different timestamp without any sleeping.
type movingClock struct {
	mu   sync.Mutex
	at   time.Time
	step time.Duration
}

func (c *movingClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.at
	c.at = c.at.Add(c.step)
	return at
}

func (c *movingClock) last() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.at.Add(-c.step)
}

func connectorAt(t *testing.T, base string, tweak func(*webhook.Config)) *webhook.Connector {
	t.Helper()
	header := http.Header{}
	header.Set("X-Receiver-Key", "configured")
	cfg := webhook.Config{
		Endpoints: map[string]webhook.Endpoint{
			"acme": {BaseURL: base + "/hooks", Secret: secret, Header: header},
		},
		Backoff:    time.Millisecond,
		MaxBackoff: 2 * time.Millisecond,
	}
	if tweak != nil {
		tweak(&cfg)
	}
	c, err := webhook.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func event(seq uint64, kind envelope.Kind, payload string) envelope.Event {
	return envelope.Event{
		Seq:     seq,
		Topic:   topic.MustParse("agent:docs-bot:run_1"),
		RunID:   "run_1",
		Agent:   "docs-bot",
		Origin:  "cron",
		Kind:    kind,
		Payload: []byte(payload),
		At:      time.Unix(1784600000, 0).UTC(),
	}
}

func textEvent(seq uint64, s string) envelope.Event {
	encoded, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return event(seq, envelope.KindText, `{"text":`+string(encoded)+`}`)
}
