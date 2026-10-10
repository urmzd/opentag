package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/signature"
)

const webhookSecret = "s3cr3t"

// recordingTags is the connector's translation step, instrumented. Its call
// count is how a test proves the body was never interpreted: the conversion is
// the first and only thing that looks inside a webhook body.
type recordingTags struct {
	mu     sync.Mutex
	calls  int
	bodies []string

	inbound Inbound
	err     error
}

func (r *recordingTags) fn(_ http.Header, body []byte) (Inbound, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	r.bodies = append(r.bodies, string(body))
	return r.inbound, r.err
}

func (r *recordingTags) called() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// githubTag is what a connector would produce from a verified delivery. It sets
// a tenant and an origin on purpose: both are the server's to assign, and this
// is what proves the payload's version is discarded.
func githubTag() envelope.Tag {
	return envelope.Tag{
		ID:     "delivery-guid-1",
		Agent:  "docs-bot",
		Tenant: "tenant-from-the-payload",
		Origin: "spoofed",
		Text:   "please review",
	}
}

func githubSignature(body string) string {
	mac := hmac.New(sha256.New, []byte(webhookSecret))
	mac.Write([]byte(body))
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// webhookHarness wires one github ingress and hands back the conversion so a
// test can see whether it ran.
func webhookHarness(t *testing.T, mutate func(*Ingress)) (*harness, *recordingTags) {
	t.Helper()
	tags := &recordingTags{inbound: Inbound{Tags: []envelope.Tag{githubTag()}}}
	in := Ingress{
		Connector: "github",
		Tenant:    tenantAcme,
		Verifier:  signature.NewGitHub(webhookSecret),
		Tags:      tags.fn,
	}
	if mutate != nil {
		mutate(&in)
	}
	h := newHarness(t, func(cfg *Config) { cfg.Ingress = []Ingress{in} })
	return h, tags
}

func (h *harness) postWebhook(t *testing.T, connector, body string, header map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		h.http.URL+"/v1/webhooks/"+connector, strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	res, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatalf("post webhook: %v", err)
	}
	return res
}

// A request whose signature does not verify is refused, and the body is never
// interpreted: the conversion that would read it is not called at all.
func TestWebhookRefusesABadSignatureWithoutParsingTheBody(t *testing.T) {
	h, tags := webhookHarness(t, nil)
	body := `{"action":"opened"}`

	res := h.postWebhook(t, "github", body, map[string]string{
		signature.HeaderGitHubSignature: githubSignature("a different body"),
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status: got %d, want 401", res.StatusCode)
	}
	if got := tags.called(); got != 0 {
		t.Fatalf("the body was interpreted %d times despite a failed signature", got)
	}
	if got := len(h.invoker.accepted()); got != 0 {
		t.Fatalf("a forged request raised %d tags", got)
	}
	out, _ := io.ReadAll(res.Body)
	if strings.Contains(string(out), webhookSecret) || strings.Contains(string(out), "mismatch") {
		t.Fatalf("the refusal leaked why it failed: %q", out)
	}
}

// A verified request is accepted, and the tenant and origin come from the
// registration rather than from the payload that claimed both.
func TestWebhookAcceptsAVerifiedRequestAndStampsItsOwnScope(t *testing.T) {
	h, tags := webhookHarness(t, nil)
	body := `{"action":"opened","tenant":"tenant-from-the-payload"}`

	res := h.postWebhook(t, "github", body, map[string]string{
		signature.HeaderGitHubSignature: githubSignature(body),
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusAccepted {
		out, _ := io.ReadAll(res.Body)
		t.Fatalf("status: got %d (%s), want 202", res.StatusCode, out)
	}
	if got := tags.called(); got != 1 {
		t.Fatalf("want the body converted once, got %d", got)
	}
	if got := tags.bodies[0]; got != body {
		t.Fatalf("the conversion saw %q, want the exact bytes received", got)
	}

	accepted := h.invoker.accepted()
	if len(accepted) != 1 {
		t.Fatalf("want one tag raised, got %d", len(accepted))
	}
	if accepted[0].Tenant != tenantAcme {
		t.Errorf("tenant: got %q, want the registration's %q", accepted[0].Tenant, tenantAcme)
	}
	if accepted[0].Origin != "github" {
		t.Errorf("origin: got %q, want the endpoint's %q", accepted[0].Origin, "github")
	}
	if !accepted[0].At.Equal(fixedTime) {
		t.Errorf("at: got %s, want the server clock", accepted[0].At)
	}

	var out accepted_
	if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(out.Accepted) != 1 || out.Accepted[0].RunID != "run_delivery-guid-1" {
		t.Fatalf("want the run id in the response, got %+v", out.Accepted)
	}
	if out.Accepted[0].Topic != "agent:docs-bot:run_delivery-guid-1" {
		t.Fatalf("want the run topic in the response, got %q", out.Accepted[0].Topic)
	}
}

// accepted_ mirrors the response body, decoded from the outside as a client
// would see it.
type accepted_ struct {
	Accepted []struct {
		TagID string `json:"tag_id"`
		RunID string `json:"run_id"`
		Rev   int    `json:"rev"`
		Topic string `json:"topic"`
	} `json:"accepted"`
}

// Each way a request can fail verification maps onto the status that describes
// it: forged and replayed are 401, unparseable headers are 400, and our own
// missing secret is 500.
func TestWebhookVerificationFailuresMapToStatuses(t *testing.T) {
	body := `{"action":"opened"}`
	stale := fixedTime.Add(-time.Hour)

	tests := []struct {
		name     string
		verifier signature.Verifier
		header   map[string]string
		want     int
	}{
		{
			name:     "forged digest",
			verifier: signature.NewGitHub(webhookSecret),
			header:   map[string]string{signature.HeaderGitHubSignature: githubSignature("other")},
			want:     http.StatusUnauthorized,
		},
		{
			name:     "no signature header",
			verifier: signature.NewGitHub(webhookSecret),
			want:     http.StatusBadRequest,
		},
		{
			name:     "digest is not hex",
			verifier: signature.NewGitHub(webhookSecret),
			header:   map[string]string{signature.HeaderGitHubSignature: "sha256=zzzz"},
			want:     http.StatusBadRequest,
		},
		{
			name:     "no secret configured",
			verifier: signature.NewGitHub(""),
			header:   map[string]string{signature.HeaderGitHubSignature: githubSignature(body)},
			want:     http.StatusInternalServerError,
		},
		{
			name: "replayed outside the window",
			verifier: &signature.Slack{
				Secret: []byte(webhookSecret),
				Now:    func() time.Time { return fixedTime },
			},
			header: slackHeaders(body, stale),
			want:   http.StatusUnauthorized,
		},
		{
			name: "slack request inside the window",
			verifier: &signature.Slack{
				Secret: []byte(webhookSecret),
				Now:    func() time.Time { return fixedTime },
			},
			header: slackHeaders(body, fixedTime),
			want:   http.StatusAccepted,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h, tags := webhookHarness(t, func(in *Ingress) { in.Verifier = tt.verifier })
			res := h.postWebhook(t, "github", body, tt.header)
			defer res.Body.Close()
			if res.StatusCode != tt.want {
				out, _ := io.ReadAll(res.Body)
				t.Fatalf("status: got %d (%s), want %d", res.StatusCode, out, tt.want)
			}
			wantCalls := 0
			if tt.want == http.StatusAccepted {
				wantCalls = 1
			}
			if got := tags.called(); got != wantCalls {
				t.Fatalf("body interpreted %d times, want %d", got, wantCalls)
			}
		})
	}
}

func slackHeaders(body string, at time.Time) map[string]string {
	timestamp := strconv.FormatInt(at.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(webhookSecret))
	fmt.Fprintf(mac, "v0:%s:%s", timestamp, body)
	return map[string]string{
		signature.HeaderSlackTimestamp: timestamp,
		signature.HeaderSlackSignature: "v0=" + hex.EncodeToString(mac.Sum(nil)),
	}
}

// A connector handshake is an authentic request that raises no run: it is
// answered with exactly what the connector asked for.
func TestWebhookEchoesAConnectorHandshake(t *testing.T) {
	h, _ := webhookHarness(t, nil)
	body := `{"type":"url_verification","challenge":"abc123"}`
	// Reconfigure the conversion through the same ingress the harness built.
	in := h.server.ingress["github"]
	in.Tags = func(_ http.Header, _ []byte) (Inbound, error) {
		return Inbound{Reply: []byte("abc123")}, nil
	}
	h.server.ingress["github"] = in

	res := h.postWebhook(t, "github", body, map[string]string{
		signature.HeaderGitHubSignature: githubSignature(body),
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", res.StatusCode)
	}
	out, _ := io.ReadAll(res.Body)
	if string(out) != "abc123" {
		t.Fatalf("want the challenge echoed, got %q", out)
	}
	if got := len(h.invoker.accepted()); got != 0 {
		t.Fatalf("a handshake raised %d tags", got)
	}
}

// An endpoint nobody registered is absent, and an oversized body is refused
// before it can be buffered whole.
func TestWebhookRefusesUnknownConnectorsAndOversizedBodies(t *testing.T) {
	h, _ := webhookHarness(t, nil)

	t.Run("unknown connector", func(t *testing.T) {
		res := h.postWebhook(t, "jira", `{}`, nil)
		defer res.Body.Close()
		if res.StatusCode != http.StatusNotFound {
			t.Fatalf("status: got %d, want 404", res.StatusCode)
		}
	})

	t.Run("oversized body", func(t *testing.T) {
		small := newHarness(t, func(cfg *Config) {
			cfg.MaxBodyBytes = 16
			cfg.Ingress = []Ingress{{
				Connector: "github",
				Tenant:    tenantAcme,
				Verifier:  signature.NewGitHub(webhookSecret),
				Tags:      func(http.Header, []byte) (Inbound, error) { return Inbound{}, nil },
			}}
		})
		body := strings.Repeat("x", 64)
		res := small.postWebhook(t, "github", body, map[string]string{
			signature.HeaderGitHubSignature: githubSignature(body),
		})
		defer res.Body.Close()
		if res.StatusCode != http.StatusRequestEntityTooLarge {
			t.Fatalf("status: got %d, want 413", res.StatusCode)
		}
	})
}

// A tag the connector produced but the core cannot run is refused, and nothing
// is invoked.
func TestWebhookRefusesAnUnrunnableTag(t *testing.T) {
	h, tags := webhookHarness(t, nil)
	tags.inbound = Inbound{Tags: []envelope.Tag{{Agent: "docs-bot"}}} // no id, no origin
	body := `{"action":"opened"}`

	res := h.postWebhook(t, "github", body, map[string]string{
		signature.HeaderGitHubSignature: githubSignature(body),
	})
	defer res.Body.Close()

	if res.StatusCode != http.StatusBadRequest {
		t.Fatalf("status: got %d, want 400", res.StatusCode)
	}
	if got := len(h.invoker.accepted()); got != 0 {
		t.Fatalf("an unrunnable tag reached the runtime %d times", got)
	}
}
