package server

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

// Tenant is an authorization scope, and the whole point of deriving it from the
// credential is that a body cannot claim it.
//
// The protobuf contract has no tenant field on any request, so the only way to
// try is to hand-roll a JSON body with one. Connect's JSON codec discards
// unknown fields, and the handler assigns the tenant rather than merging it, so
// the attempt has to fail twice over. These tests go through raw HTTP for
// exactly that reason: a generated client could not express the attack.
func TestTenantCannotBeForgedFromTheRequestBody(t *testing.T) {
	t.Run("invoke", func(t *testing.T) {
		h := newHarness(t, nil)
		body := `{"id":"slack-evt-1","agent":"docs-bot","origin":"slack","text":"hi","tenant":"` + tenantOther + `"}`
		res, got := h.postJSON(t, "/mandatum.v1.InvokeService/Invoke", tokenAcme, body)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("invoke: status %d: %s", res.StatusCode, got)
		}
		accepted := h.invoker.accepted()
		if len(accepted) != 1 {
			t.Fatalf("want one accepted tag, got %d", len(accepted))
		}
		if accepted[0].Tenant != tenantAcme {
			t.Fatalf("the body forged a tenant: runtime saw %q, credential was %q", accepted[0].Tenant, tenantAcme)
		}
	})

	t.Run("publish", func(t *testing.T) {
		h := newHarness(t, nil)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		watch, err := h.bus.Subscribe(ctx, envelope.Subscription{Topic: topic.MustParse("agent:docs-bot:run_1")})
		if err != nil {
			t.Fatalf("watch: %v", err)
		}
		defer func() { _ = watch.Close() }()

		body := `{"event":{"topic":"agent:docs-bot:run_1","kind":"delta.text","tenant":"` + tenantOther + `"}}`
		res, got := h.postJSON(t, "/mandatum.v1.BusService/Publish", tokenAcme, body)
		if res.StatusCode != http.StatusOK {
			t.Fatalf("publish: status %d: %s", res.StatusCode, got)
		}
		e, err := watch.Recv(ctx)
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		if e.Tenant != tenantAcme {
			t.Fatalf("the body forged a tenant: bus saw %q, credential was %q", e.Tenant, tenantAcme)
		}
	})

	t.Run("sse query", func(t *testing.T) {
		h := newHarness(t, nil)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		h.publish(event(tenantOther, "run_other", 1, envelope.KindText, `{"text":"secret"}`))
		h.publish(event(tenantAcme, "run_acme", 1, envelope.KindText, `{"text":"mine"}`))

		// There is no tenant parameter, and adding one changes nothing: the
		// subscription's scope comes from the credential.
		res, err := h.get(ctx, "/v1/sse?topic=agent:docs-bot&tenant="+tenantOther, tokenAcme)
		if err != nil {
			t.Fatalf("sse: %v", err)
		}
		defer res.Body.Close()

		frames := readFrames(t, res.Body, 2)
		if !strings.Contains(frames[1], `"tenant":"`+tenantAcme+`"`) {
			t.Fatalf("want only the caller's own events, got frame %q", frames[1])
		}
		if strings.Contains(strings.Join(frames, ""), "secret") {
			t.Fatalf("another tenant's payload reached the stream: %v", frames)
		}
	})
}

// postJSON speaks the Connect protocol's JSON form directly, which is how a
// caller would hand-roll a body the generated types cannot express.
func (h *harness) postJSON(t *testing.T, procedure, token, body string) (*http.Response, string) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, h.http.URL+procedure, strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderAuthorization, "Bearer "+token)
	res, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatalf("post %s: %v", procedure, err)
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return res, string(out)
}
