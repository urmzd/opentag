package cli

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"connectrpc.com/connect"

	"github.com/urmzd/mandatum/gen/mandatum/v1/mandatumv1connect"
)

// clients are the three Connect stubs a client command talks through. They are
// built together because they share one HTTP client and one credential, and
// because a command that has resolved a server has already paid for all three:
// there is no cost to holding a stub you do not call.
type clients struct {
	agents mandatumv1connect.AgentServiceClient
	bus    mandatumv1connect.BusServiceClient
	invoke mandatumv1connect.InvokeServiceClient
}

// dial resolves --server and --token into stubs.
//
// The HTTP client carries no timeout, deliberately. Two of these endpoints are
// server-streaming and open-ended, and a client timeout applies to the whole
// response body, so any value at all would cut a subscription at that age. A
// caller that wants a deadline sets one on its context, which bounds the
// request without lying about when a stream should end.
func dial() (*clients, error) {
	base := strings.TrimRight(strings.TrimSpace(serverFlag), "/")
	if base == "" {
		return nil, usagef("--server is empty; set it or $%s", EnvServer)
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, usagef("--server %q is not a URL: %v", serverFlag, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, usagef("--server %q must be http or https", serverFlag)
	}
	if u.Host == "" {
		return nil, usagef("--server %q has no host", serverFlag)
	}

	hc := &http.Client{Transport: bearer{token: strings.TrimSpace(tokenFlag), next: http.DefaultTransport}}
	return &clients{
		agents: mandatumv1connect.NewAgentServiceClient(hc, base),
		bus:    mandatumv1connect.NewBusServiceClient(hc, base),
		invoke: mandatumv1connect.NewInvokeServiceClient(hc, base),
	}, nil
}

// bearer presents the caller's credential on every request.
//
// It is a RoundTripper rather than a connect.Interceptor because the SSE
// endpoint is plain HTTP and would otherwise need a second, separately
// maintained copy of the same three lines. An empty token sends no header at
// all, so a single-tenant deployment with an open authenticator needs no
// configuration and a misconfigured one fails as unauthenticated rather than as
// a malformed header.
type bearer struct {
	token string
	next  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	if b.token == "" {
		return b.next.RoundTrip(r)
	}
	// A RoundTripper may not modify the request it is given.
	clone := r.Clone(r.Context())
	clone.Header.Set("Authorization", "Bearer "+b.token)
	return b.next.RoundTrip(clone)
}

// connectErr annotates a failed call without losing its Connect code, which is
// what exitCodeFor classifies on. fmt.Errorf with %w preserves the code because
// connect.CodeOf unwraps.
func connectErr(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", op, err)
}

// codeName renders a Connect code for a diagnostic. It exists so that a
// streaming command can report why a stream ended without the caller having to
// read an error string twice.
func codeName(err error) string {
	return connect.CodeOf(err).String()
}
