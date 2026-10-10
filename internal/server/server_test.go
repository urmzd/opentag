package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/urmzd/dispatch/pkg/metrics"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/signature"
)

// A configuration that cannot be secure is refused at construction, where the
// operator is still watching, rather than at the first request.
func TestNewRefusesAConfigurationItCannotSecure(t *testing.T) {
	auth, err := NewTokens(map[string]Identity{tokenAcme: {Tenant: tenantAcme}})
	if err != nil {
		t.Fatalf("NewTokens: %v", err)
	}
	ok := func(connector string) Ingress {
		return Ingress{
			Connector: connector,
			Verifier:  signature.NewGitHub(webhookSecret),
			Tags:      func(http.Header, []byte) (Inbound, error) { return Inbound{}, nil },
		}
	}
	tests := map[string]Config{
		"no authenticator": {},
		"ingress without a verifier": {
			Auth:    auth,
			Ingress: []Ingress{{Connector: "github", Tags: ok("github").Tags}},
		},
		"ingress without a conversion": {
			Auth:    auth,
			Ingress: []Ingress{{Connector: "github", Verifier: signature.NewGitHub(webhookSecret)}},
		},
		"ingress without a name": {
			Auth:    auth,
			Ingress: []Ingress{ok("")},
		},
		"ingress with an unusable name": {
			Auth:    auth,
			Ingress: []Ingress{ok("git hub")},
		},
		"two ingresses for one connector": {
			Auth:    auth,
			Ingress: []Ingress{ok("github"), ok("github")},
		},
	}
	for name, cfg := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := New(cfg); err == nil {
				t.Fatal("want New to refuse this configuration, got no error")
			}
		})
	}
	if _, err := NewTokens(map[string]Identity{"": {Tenant: tenantAcme}}); err == nil {
		t.Fatal("want an empty bearer token to be refused")
	}
}

// A feature a deployment did not wire answers "not implemented" rather than
// disappearing: a client discovering a feature is off should get an answer, not
// a 404 it has to interpret.
func TestUnconfiguredFeaturesReportNotImplemented(t *testing.T) {
	h := newHarness(t, func(cfg *Config) {
		cfg.Store = nil
		cfg.Bus = nil
		cfg.Invoker = nil
		cfg.Runs = nil
		cfg.Ingress = []Ingress{{
			Connector: "github",
			Verifier:  signature.NewGitHub(webhookSecret),
			Tags:      func(http.Header, []byte) (Inbound, error) { return Inbound{}, nil },
		}}
	})
	ctx := context.Background()

	_, err := h.agentClient(tokenAcme).GetAgent(ctx, connect.NewRequest(&mandatumv1.GetAgentRequest{Name: "docs-bot"}))
	requireCode(t, err, connect.CodeUnimplemented)

	_, err = h.busClient(tokenAcme).Publish(ctx, connect.NewRequest(&mandatumv1.PublishRequest{
		Event: &mandatumv1.Event{Topic: "agent:docs-bot:run_1", Kind: string(envelope.KindText)},
	}))
	requireCode(t, err, connect.CodeUnimplemented)

	_, err = h.invokeClient(tokenAcme).Invoke(ctx, connect.NewRequest(tagMsg()))
	requireCode(t, err, connect.CodeUnimplemented)

	_, err = h.invokeClient(tokenAcme).GetRun(ctx, connect.NewRequest(&mandatumv1.GetRunRequest{RunId: "run_1"}))
	requireCode(t, err, connect.CodeUnimplemented)

	res, err := h.get(ctx, "/v1/sse?topic=agent", tokenAcme)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNotImplemented {
		t.Fatalf("sse status: got %d, want 501", res.StatusCode)
	}

	hook := h.postWebhook(t, "github", `{}`, nil)
	defer hook.Body.Close()
	if hook.StatusCode != http.StatusNotImplemented {
		t.Fatalf("webhook status: got %d, want 501", hook.StatusCode)
	}
}

// Health is honest about draining, so a load balancer can stop sending new
// streams to an instance whose existing ones are still finishing.
func TestHealthReportsDrainingOnceShutdownStarts(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	res, err := h.get(ctx, "/healthz", "")
	if err != nil {
		t.Fatalf("healthz: %v", err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != "ok" {
		t.Fatalf("healthz: got %d %q", res.StatusCode, body)
	}

	shutdown, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if err := h.server.Shutdown(shutdown); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	res, err = h.get(ctx, "/healthz", "")
	if err != nil {
		t.Fatalf("healthz while draining: %v", err)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable || strings.TrimSpace(string(body)) != "draining" {
		t.Fatalf("healthz while draining: got %d %q", res.StatusCode, body)
	}
}

// Metrics are exposed from the recorder's own snapshot, so a series recorded by a
// handler is a series a scrape can see.
func TestMetricsExposeWhatTheHandlersRecorded(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	if _, err := h.agentClient(tokenAcme).CreateAgent(ctx, connect.NewRequest(&mandatumv1.CreateAgentRequest{
		Spec: specMsg("docs-bot"),
	})); err != nil {
		t.Fatalf("create: %v", err)
	}

	res, err := h.get(ctx, "/metrics", "")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	text := string(body)

	for _, want := range []string{
		MetricUp,
		MetricRequests + `{code="ok",procedure="/mandatum.v1.AgentService/CreateAgent"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("want %q in the exposition, got:\n%s", want, text)
		}
	}

	// A refused credential is counted too: observation wraps authentication, so a
	// spike of unauthenticated calls is visible rather than silent.
	if _, err := h.agentClient("bogus").GetAgent(ctx, connect.NewRequest(&mandatumv1.GetAgentRequest{Name: "docs-bot"})); err == nil {
		t.Fatal("want the bogus credential refused")
	}
	res, err = h.get(ctx, "/metrics", "")
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	defer res.Body.Close()
	body, _ = io.ReadAll(res.Body)
	if !strings.Contains(string(body), `code="unauthenticated"`) {
		t.Errorf("want refused credentials counted, got:\n%s", body)
	}

	t.Run("recorder that cannot be scraped", func(t *testing.T) {
		plain := newHarness(t, func(cfg *Config) { cfg.Metrics = metrics.Nop() })
		res, err := plain.get(ctx, "/metrics", "")
		if err != nil {
			t.Fatalf("metrics: %v", err)
		}
		defer res.Body.Close()
		body, _ := io.ReadAll(res.Body)
		if !strings.Contains(string(body), MetricUp+" 1") {
			t.Fatalf("want liveness even without a scrapeable recorder, got:\n%s", body)
		}
	})
}

// Serve runs until its context is cancelled and then drains: the listener stops
// accepting and the in-flight streams end.
func TestServeStopsAndDrainsWhenItsContextIsCancelled(t *testing.T) {
	auth, err := NewTokens(map[string]Identity{tokenAcme: {Tenant: tenantAcme}})
	if err != nil {
		t.Fatalf("NewTokens: %v", err)
	}
	fake := newFakeBus()
	srv, err := New(Config{Auth: auth, Bus: fake, Now: func() time.Time { return fixedTime }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- srv.Serve(ctx, ln) }()

	base := "http://" + ln.Addr().String()
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

	// A stream is open across the shutdown, which is what makes this a drain
	// rather than a close.
	streamReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, base+"/v1/sse?topic=agent", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	streamReq.Header.Set(HeaderAuthorization, "Bearer "+tokenAcme)
	stream, err := client.Do(streamReq)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer stream.Body.Close()
	newSSEReader(t, stream.Body).frame() // the retry frame: the handler is serving

	cancel()

	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after its context was cancelled")
	}
	if got := fake.openStreams(); got != 0 {
		t.Fatalf("draining left %d bus streams open", got)
	}
	if _, err := client.Get(base + "/healthz"); err == nil {
		t.Fatal("want the listener closed after Serve returned")
	}
}

// The two error mappings are one classification seen twice: an error's Connect
// code and the HTTP status the plain endpoints answer with must agree.
func TestErrorClassificationIsConsistentAcrossTransports(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		code   connect.Code
		status int
	}{
		{"unauthenticated", ErrUnauthenticated, connect.CodeUnauthenticated, http.StatusUnauthorized},
		{"not found", ErrNotFound, connect.CodeNotFound, http.StatusNotFound},
		{"exists", ErrExists, connect.CodeAlreadyExists, http.StatusConflict},
		{"conflict", ErrConflict, connect.CodeAborted, http.StatusConflict},
		{"invalid", ErrInvalid, connect.CodeInvalidArgument, http.StatusBadRequest},
		{"envelope invalid", envelope.ErrInvalid, connect.CodeInvalidArgument, http.StatusBadRequest},
		{"unavailable", ErrUnavailable, connect.CodeUnavailable, http.StatusServiceUnavailable},
		{"unimplemented", ErrUnimplemented, connect.CodeUnimplemented, http.StatusNotImplemented},
		{"cancelled", context.Canceled, connect.CodeCanceled, 499},
		{"deadline", context.DeadlineExceeded, connect.CodeDeadlineExceeded, http.StatusGatewayTimeout},
		{"unclassified", errInjected, connect.CodeInternal, http.StatusInternalServerError},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := codeOf(tt.err); got != tt.code {
				t.Errorf("code: got %v, want %v", got, tt.code)
			}
			if got := httpStatus(tt.code); got != tt.status {
				t.Errorf("status: got %d, want %d", got, tt.status)
			}
		})
	}
}
