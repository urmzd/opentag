package server

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"connectrpc.com/connect"

	opentagv1 "github.com/urmzd/opentag/gen/opentag/v1"
)

// Authentication is a property of the transport, not of a handler that remembers
// to check: every procedure, unary and streaming, refuses a caller it cannot
// identify.
func TestEveryProcedureRefusesAnUnidentifiedCaller(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	credentials := map[string]http.Header{
		"no header":     {},
		"wrong scheme":  headerOf(HeaderAuthorization, "Basic "+tokenAcme),
		"empty token":   headerOf(HeaderAuthorization, "Bearer "),
		"unknown token": headerOf(HeaderAuthorization, "Bearer nope"),
		"two headers":   headerOf(HeaderAuthorization, "Bearer "+tokenAcme, HeaderAuthorization, "Bearer "+tokenOther),
	}
	// Clients with no credential interceptor, so the test sets the header itself
	// — including setting it twice, which an interceptor could not.
	agents := h.agentClient("")
	events := h.busClient("")
	for name, header := range credentials {
		t.Run(name, func(t *testing.T) {
			unary := connect.NewRequest(&opentagv1.GetAgentRequest{Name: "docs-bot"})
			copyHeader(unary.Header(), header)
			_, err := agents.GetAgent(ctx, unary)
			requireCode(t, err, connect.CodeUnauthenticated)

			streaming := connect.NewRequest(&opentagv1.SubscribeRequest{
				Subscription: &opentagv1.Subscription{Topic: "agent"},
			})
			copyHeader(streaming.Header(), header)
			stream, err := events.Subscribe(ctx, streaming)
			if err == nil {
				stream.Receive()
				err = stream.Err()
				_ = stream.Close()
			}
			requireCode(t, err, connect.CodeUnauthenticated)
		})
	}
}

// headerOf builds a header from key and value pairs, adding rather than setting
// so a duplicated header can be expressed, and going through Add so the keys are
// canonicalised exactly as net/http would canonicalise them on the wire.
func headerOf(pairs ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(pairs); i += 2 {
		h.Add(pairs[i], pairs[i+1])
	}
	return h
}

func copyHeader(dst, src http.Header) {
	for k, values := range src {
		for _, v := range values {
			dst.Add(k, v)
		}
	}
}

// A bearer credential is matched in constant time over its digest, and an
// identity is only ever what the table said.
func TestTokensResolveOnlyRegisteredCredentials(t *testing.T) {
	auth, err := NewTokens(map[string]Identity{
		tokenAcme: {Tenant: tenantAcme, Subject: subjectAcme},
	})
	if err != nil {
		t.Fatalf("NewTokens: %v", err)
	}
	tests := []struct {
		name   string
		header http.Header
		want   Identity
		fails  bool
	}{
		{
			name:   "registered",
			header: headerOf(HeaderAuthorization, "Bearer "+tokenAcme),
			want:   Identity{Tenant: tenantAcme, Subject: subjectAcme},
		},
		{
			name:   "scheme is case insensitive",
			header: headerOf(HeaderAuthorization, "bearer "+tokenAcme),
			want:   Identity{Tenant: tenantAcme, Subject: subjectAcme},
		},
		{
			name:   "a prefix of a valid token",
			header: headerOf(HeaderAuthorization, "Bearer "+tokenAcme[:4]),
			fails:  true,
		},
		{
			name:   "no credential",
			header: http.Header{},
			fails:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := auth.Authenticate(context.Background(), tt.header)
			if tt.fails {
				if !errors.Is(err, ErrUnauthenticated) {
					t.Fatalf("want an unauthenticated error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("authenticate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("identity: got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TrustedHeader accepts exactly one value. A request carrying the header twice is
// refused rather than resolved, because picking a copy is the shape of a
// header-smuggling bug: the gateway that set one and the client that set the
// other must never be able to disagree about which is authoritative.
func TestTrustedHeaderAcceptsExactlyOneValue(t *testing.T) {
	auth := TrustedHeader{SubjectHeader: "X-OpenTag-Subject"}
	tests := []struct {
		name   string
		header http.Header
		want   Identity
		fails  bool
	}{
		{
			name:   "one value",
			header: headerOf(HeaderTenant, tenantAcme, "X-OpenTag-Subject", subjectAcme),
			want:   Identity{Tenant: tenantAcme, Subject: subjectAcme},
		},
		{
			name:   "absent",
			header: http.Header{},
			fails:  true,
		},
		{
			name:   "empty",
			header: headerOf(HeaderTenant, ""),
			fails:  true,
		},
		{
			name:   "two values",
			header: headerOf(HeaderTenant, tenantAcme, HeaderTenant, tenantOther),
			fails:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := auth.Authenticate(context.Background(), tt.header)
			if tt.fails {
				if !errors.Is(err, ErrUnauthenticated) {
					t.Fatalf("want an unauthenticated error, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("authenticate: %v", err)
			}
			if got != tt.want {
				t.Fatalf("identity: got %+v, want %+v", got, tt.want)
			}
		})
	}
}

// A handler reached without an identity refuses rather than assuming a scope.
// The only way there is a misconfigured route, and guessing there would mean
// guessing an authorization scope.
func TestAHandlerWithoutAnIdentityRefuses(t *testing.T) {
	if _, err := identityOf(context.Background()); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("want an unauthenticated error, got %v", err)
	}
	svc := &agentService{store: newFakeStore(), log: discardLogger()}
	_, err := svc.GetAgent(context.Background(), connect.NewRequest(&opentagv1.GetAgentRequest{Name: "docs-bot"}))
	requireCode(t, err, connect.CodeUnauthenticated)
}
