package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/urmzd/dispatch/pkg/metrics"
)

// ErrUnauthenticated reports a caller whose credential is missing, malformed,
// or unknown. All three are one error: telling a caller which of the three it
// was turns the endpoint into an oracle for valid credentials.
var ErrUnauthenticated = errors.New("server: unauthenticated")

// HeaderAuthorization carries the bearer credential.
const HeaderAuthorization = "Authorization"

// HeaderTenant is the header TrustedHeader reads. It is only ever trusted
// behind a gateway that sets it; see TrustedHeader.
const HeaderTenant = "X-OpenTag-Tenant"

// Identity is what a credential resolves to: the authorization scope the
// request runs under, plus who is asking.
//
// Tenant is the scope. It is stamped onto every Tag, every published Event, and
// every Event delivered to a subscriber, and it is never read from a request
// body. Subject is the caller, recorded as a revision's author so that
// authorship is a property of the credential rather than a field the caller
// fills in.
type Identity struct {
	Tenant  string
	Subject string
}

// Authenticator derives an identity from a request's headers.
//
// It takes the whole header rather than a token string because the credential
// is not always a bearer token: a browser opening an SSE stream cannot set
// headers on an EventSource, so a deployment fronting this server with a
// session cookie authenticates from Cookie, and the same interface serves both.
//
// Implementations must return an error wrapping ErrUnauthenticated for every
// failure, and must never return a zero Identity with a nil error: an empty
// tenant is a real scope (a single-tenant deployment), not a signal.
type Authenticator interface {
	Authenticate(ctx context.Context, h http.Header) (Identity, error)
}

// identityKey is the context key under which a request's identity travels. It
// is unexported so nothing outside this package can put an identity into a
// context, which is the only reason a handler is allowed to trust one.
type identityKey struct{}

// withIdentity returns ctx carrying id.
func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityKey{}, id)
}

// IdentityFrom returns the identity the interceptor put on ctx.
//
// A handler that finds none must refuse rather than assume a tenant: the only
// way to reach a handler without one is a misconfigured route, and guessing
// there would mean guessing an authorization scope.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(Identity)
	return id, ok
}

// identityOf is the handler-side accessor: it converts the absence of an
// identity into the error a handler can return directly.
func identityOf(ctx context.Context) (Identity, error) {
	id, ok := IdentityFrom(ctx)
	if !ok {
		return Identity{}, fmt.Errorf("%w: request reached a handler without an identity", ErrUnauthenticated)
	}
	return id, nil
}

// Tokens is a complete bearer-token authenticator: a fixed table of tokens,
// each resolving to one identity. It is the whole authentication story for a
// single-binary deployment, and it is what the tests use.
//
// Tokens are compared by constant-time equality over their SHA-256 digests, not
// looked up in a map keyed by the token. A map lookup is fast, but it makes the
// comparison data-dependent, and this endpoint can be probed as often as an
// attacker likes. Hashing also keeps the raw secret out of the struct once
// construction is done.
type Tokens struct {
	entries []tokenEntry
}

type tokenEntry struct {
	digest   [sha256.Size]byte
	identity Identity
}

// NewTokens returns an authenticator over a token-to-identity table. An empty
// token is rejected: it would authenticate a request that presented no
// credential at all.
func NewTokens(table map[string]Identity) (*Tokens, error) {
	t := &Tokens{entries: make([]tokenEntry, 0, len(table))}
	for token, id := range table {
		if token == "" {
			return nil, fmt.Errorf("%w: empty bearer token in the credential table", ErrInvalid)
		}
		t.entries = append(t.entries, tokenEntry{digest: sha256.Sum256([]byte(token)), identity: id})
	}
	return t, nil
}

// Authenticate implements Authenticator. It accepts "Authorization: Bearer
// <token>", case-insensitively on the scheme.
func (t *Tokens) Authenticate(_ context.Context, h http.Header) (Identity, error) {
	values := h.Values(HeaderAuthorization)
	if len(values) != 1 {
		return Identity{}, fmt.Errorf("%w: expected exactly one %s header, got %d", ErrUnauthenticated, HeaderAuthorization, len(values))
	}
	scheme, token, found := strings.Cut(values[0], " ")
	if !found || !strings.EqualFold(strings.TrimSpace(scheme), "bearer") || strings.TrimSpace(token) == "" {
		return Identity{}, fmt.Errorf("%w: %s is not a bearer credential", ErrUnauthenticated, HeaderAuthorization)
	}
	digest := sha256.Sum256([]byte(strings.TrimSpace(token)))

	// Every entry is compared, and the match is remembered rather than
	// returned, so the work done does not depend on which token was presented
	// or on whether one matched at all.
	var found2 bool
	var id Identity
	for _, e := range t.entries {
		if subtle.ConstantTimeCompare(digest[:], e.digest[:]) == 1 {
			id, found2 = e.identity, true
		}
	}
	if !found2 {
		return Identity{}, fmt.Errorf("%w: unknown bearer credential", ErrUnauthenticated)
	}
	return id, nil
}

// TrustedHeader reads the tenant from a header a gateway set.
//
// It exists because a real deployment usually terminates authentication in
// front of this server — an ingress that validates an OIDC token and forwards
// the claim it derived — and because the browser path has no other option. It
// is only safe under one condition, and the condition is absolute: the gateway
// must STRIP the header from every inbound request before setting it. If a
// client can set it, this authenticator hands out any tenant on request.
//
// Nothing here can check that condition, which is why it is a separate type
// with this comment rather than a fallback inside Tokens.
type TrustedHeader struct {
	// Header carries the tenant. Empty means HeaderTenant.
	Header string
	// SubjectHeader carries the caller identity, if the gateway forwards one.
	// Empty means the subject is unknown, which is honest: a revision author
	// recorded as "" is better than one attributed to a guess.
	SubjectHeader string
}

// Authenticate implements Authenticator.
func (t TrustedHeader) Authenticate(_ context.Context, h http.Header) (Identity, error) {
	name := t.Header
	if name == "" {
		name = HeaderTenant
	}
	values := h.Values(name)
	if len(values) != 1 || values[0] == "" {
		return Identity{}, fmt.Errorf("%w: expected exactly one non-empty %s header", ErrUnauthenticated, name)
	}
	id := Identity{Tenant: values[0]}
	if t.SubjectHeader != "" {
		id.Subject = h.Get(t.SubjectHeader)
	}
	return id, nil
}

// authInterceptor authenticates every Connect call, unary and streaming alike,
// and puts the resulting identity on the context.
//
// It is an interceptor rather than a per-handler call so that adding a
// procedure cannot add an unauthenticated procedure: the check is a property of
// the transport, not of the handler that happens to remember it.
type authInterceptor struct {
	auth Authenticator
}

func (i authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ctx, err := i.identify(ctx, req.Header())
		if err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

// WrapStreamingClient is a no-op: this interceptor authenticates callers, and
// on the client side there is no caller to authenticate.
func (i authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i authInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		ctx, err := i.identify(ctx, conn.RequestHeader())
		if err != nil {
			return err
		}
		return next(ctx, conn)
	}
}

func (i authInterceptor) identify(ctx context.Context, h http.Header) (context.Context, error) {
	id, err := i.auth.Authenticate(ctx, h)
	if err != nil {
		return ctx, connect.NewError(connect.CodeUnauthenticated, err)
	}
	return withIdentity(ctx, id), nil
}

// observeInterceptor counts calls by procedure and outcome. It records the
// Connect code rather than an HTTP status because that is what the handler
// actually returned, and a streaming call has no single status.
type observeInterceptor struct {
	metrics metrics.Recorder
}

func (i observeInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		res, err := next(ctx, req)
		i.record(req.Spec().Procedure, err)
		return res, err
	}
}

func (i observeInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (i observeInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		err := next(ctx, conn)
		i.record(conn.Spec().Procedure, err)
		return err
	}
}

func (i observeInterceptor) record(procedure string, err error) {
	code := "ok"
	if err != nil {
		code = connect.CodeOf(err).String()
	}
	i.metrics.Count(MetricRequests, 1,
		metrics.Label{Key: "procedure", Value: procedure},
		metrics.Label{Key: "code", Value: code},
	)
}
