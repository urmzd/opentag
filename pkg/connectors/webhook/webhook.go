// Package webhook is the outbound peer in the mesh: a sink, and only a sink.
//
// # Why there is no Trigger here
//
// An outbound POST raises nothing. This connector takes events off the bus and
// puts them on somebody else's HTTP endpoint; nothing ever comes back that could
// become a Tag. Inbound webhooks are the other direction entirely and belong to
// whichever connector owns the payload shape (see pkg/connectors/slack and
// pkg/connectors/github, and internal/server for the ingress that fronts them).
// pkg/connector.RolesOf derives the single face from the type, so a tag claiming
// origin "webhook" has nowhere to have come from.
//
// # The exception to the one-message rule
//
// Every human-facing sink in this package family keeps one message per run and
// edits it, because a Slack channel that got one message per token would be
// unusable (see pkg/connectors/internal/sink). This sink does the opposite: it
// POSTs every event it is routed, as its own request.
//
// That is not an inconsistency, it is the same rule applied to a different
// reader. Editing exists so a human is not flooded; a receiver here is a
// program, and a program wants the stream, not a document that keeps changing
// underneath it. Idempotency moves with it: instead of re-rendering a document
// so that redelivery is a no-op by construction, this sink stamps every request
// with a delivery id — "<run>/<seq>" — that is identical across every retry of
// the same event, and the receiver dedupes on it. A route that only wants the
// outcome asks for Kinds: ["lifecycle.completed"] and receives exactly one POST
// per run.
//
// # Signing, and doing better than we are done by
//
// Every request is signed with HMAC-SHA256 over a base string that includes a
// timestamp, which is the scheme pkg/signature's Slack verifier checks. See
// Sign: the timestamp is what gives a receiver a replay window, and it is the
// thing GitHub's inbound scheme does not give us. Outbound we choose, so we
// choose the stronger one.
//
// # Failure policy
//
// A POST is bounded by a timeout, retried a few times with exponential backoff
// while the failure could plausibly clear, and abandoned immediately when it
// cannot. "Cannot" is a 4xx other than 408 and 429: a 400 or a 404 is our
// payload or our URL, and repeating it only spends the receiver's quota and the
// router's lane. Those come back wrapped in connector.ErrUndeliverable, which
// pkg/router classifies as permanent and does not retry either.
//
// The retry here is the inner one and it is deliberately short. The router
// retries too, on a longer cycle, and the division is that this loop absorbs a
// receiver's momentary hiccup within one delivery attempt while the router's
// handles an outage. Both are bounded; neither blocks another run, because a
// router lane is per (run, target).
package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Name is the address scheme this connector owns.
const Name = "webhook"

// Defaults for one delivery.
const (
	// DefaultTimeout bounds a single POST. A sink is called from a delivery loop
	// with other runs to serve, so a receiver that accepts a connection and then
	// stops reading must not hold it.
	DefaultTimeout = 10 * time.Second

	// DefaultAttempts is how many times one event is POSTed before the delivery
	// is handed back to the router as failed. Three is enough to ride out a
	// restart behind a load balancer and few enough that a receiver that is
	// genuinely down is not hammered.
	DefaultAttempts = 3

	// DefaultBackoff is the wait before the second attempt; it doubles.
	DefaultBackoff = 200 * time.Millisecond

	// DefaultMaxBackoff caps the wait between attempts.
	DefaultMaxBackoff = 2 * time.Second

	// maxErrorBody bounds how much of a failing response is carried into the
	// error. Enough for a receiver's JSON error object, not enough to log its
	// error page.
	maxErrorBody = 4 << 10
)

// Endpoint is one receiver: where to POST, and what to sign with.
type Endpoint struct {
	// BaseURL is the root the address path is appended to, without a trailing
	// slash. "https://hooks.example.com/mandatum" plus webhook://acme/deploys
	// posts to "https://hooks.example.com/mandatum/deploys".
	BaseURL string

	// Secret signs requests to this receiver. Required: an unsigned webhook
	// tells its receiver nothing about who sent it, and a receiver that accepts
	// unsigned events accepts them from anyone who learned the URL.
	//
	// It is per endpoint rather than per deployment because two receivers must
	// not be able to forge each other, which one shared secret would let them do.
	Secret string

	// Header is sent on every request to this endpoint, for a receiver that also
	// wants an API key or a routing header of its own. It cannot override the
	// signature headers.
	Header http.Header
}

// Config is everything the webhook connector needs.
type Config struct {
	// Endpoints maps an address workspace to the receiver it names. A target
	// whose workspace is not here is refused with ErrUndeliverable rather than
	// guessed at: the workspace is an indirection precisely so that a route
	// written in an agent spec cannot name an arbitrary URL on the internet.
	Endpoints map[string]Endpoint

	// HTTP performs the requests. Nil means a client with no timeout of its own,
	// because each attempt is bounded by a context instead.
	HTTP *http.Client

	// Timeout bounds one attempt. Zero means DefaultTimeout.
	Timeout time.Duration

	// Attempts is the maximum number of POSTs for one event. Zero means
	// DefaultAttempts; one disables retrying here and leaves it to the router.
	Attempts int

	// Backoff is the wait before the second attempt, doubling to MaxBackoff.
	// Zero means DefaultBackoff.
	Backoff time.Duration

	// MaxBackoff caps the wait. Zero means DefaultMaxBackoff.
	MaxBackoff time.Duration

	// Now is the clock used for the signed timestamp. Nil means time.Now.
	Now func() time.Time
}

// Connector is the outbound webhook peer. It implements pkg/connector's Sink
// face and no other.
type Connector struct {
	endpoints  map[string]Endpoint
	http       *http.Client
	timeout    time.Duration
	attempts   int
	backoff    time.Duration
	maxBackoff time.Duration
	now        func() time.Time
}

// Compile-time proof of the faces. The absence of Trigger and Actor is the
// declaration that matters: this connector raises nothing and acts on nothing.
var (
	_ connector.Sink      = (*Connector)(nil)
	_ connector.Connector = (*Connector)(nil)
)

// New returns a webhook connector.
//
// Every endpoint is validated here, at startup, because the two ways to get one
// wrong — a base URL that is not absolute, and a missing secret — both fail at
// delivery time otherwise, which is hours later and one run at a time.
func New(cfg Config) (*Connector, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, fmt.Errorf("webhook: config needs at least one endpoint")
	}
	c := &Connector{
		endpoints:  make(map[string]Endpoint, len(cfg.Endpoints)),
		http:       cfg.HTTP,
		timeout:    cfg.Timeout,
		attempts:   cfg.Attempts,
		backoff:    cfg.Backoff,
		maxBackoff: cfg.MaxBackoff,
		now:        cfg.Now,
	}
	for workspace, endpoint := range cfg.Endpoints {
		if err := address.ValidWorkspace(workspace); err != nil {
			return nil, fmt.Errorf("webhook: endpoint %q cannot be an address workspace: %w", workspace, err)
		}
		if endpoint.Secret == "" {
			return nil, fmt.Errorf("webhook: endpoint %q has no signing secret, and an unsigned webhook proves nothing about its sender", workspace)
		}
		u, err := url.Parse(endpoint.BaseURL)
		if err != nil || !u.IsAbs() || u.Host == "" {
			return nil, fmt.Errorf("webhook: endpoint %q base url %q is not an absolute url", workspace, endpoint.BaseURL)
		}
		endpoint.BaseURL = strings.TrimRight(endpoint.BaseURL, "/")
		c.endpoints[workspace] = endpoint
	}

	if c.http == nil {
		c.http = &http.Client{}
	}
	if c.timeout <= 0 {
		c.timeout = DefaultTimeout
	}
	if c.attempts <= 0 {
		c.attempts = DefaultAttempts
	}
	if c.backoff <= 0 {
		c.backoff = DefaultBackoff
	}
	if c.maxBackoff <= 0 {
		c.maxBackoff = DefaultMaxBackoff
	}
	if c.now == nil {
		c.now = time.Now
	}
	return c, nil
}

// Name implements connector.Connector.
func (c *Connector) Name() string { return Name }

// Deliver implements connector.Sink: it POSTs one event to target's receiver.
//
// The body is marshalled once and every attempt sends exactly those bytes, since
// the signature covers them. Only the timestamp and the digest are recomputed
// per attempt, so a retry that goes out minutes later still lands inside the
// receiver's replay window.
func (c *Connector) Deliver(ctx context.Context, target address.Address, e envelope.Event) error {
	endpoint, uri, err := c.resolve(target)
	if err != nil {
		return err
	}
	body, err := json.Marshal(BodyOf(e))
	if err != nil {
		return fmt.Errorf("webhook: encode event %d of run %s: %w", e.Seq, e.RunID, err)
	}

	for attempt := 1; ; attempt++ {
		err := c.post(ctx, endpoint, uri, e, body)
		switch {
		case err == nil:
			return nil
		case ctx.Err() != nil:
			// The caller went away or is shutting down. Report the cause rather
			// than the symptom, and do not sleep on the way out.
			return fmt.Errorf("webhook: post %s: %w", uri, errors.Join(err, ctx.Err()))
		case errors.Is(err, connector.ErrUndeliverable), attempt >= c.attempts:
			return err
		}

		timer := time.NewTimer(c.backoffFor(attempt))
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("webhook: post %s: %w", uri, errors.Join(err, ctx.Err()))
		}
	}
}

// post performs one attempt.
func (c *Connector) post(ctx context.Context, endpoint Endpoint, uri string, e envelope.Event, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, uri, bytes.NewReader(body))
	if err != nil {
		// A URL that cannot be turned into a request will not become one on the
		// second try either.
		return fmt.Errorf("%w: webhook: build post %s: %w", connector.ErrUndeliverable, uri, err)
	}
	// The endpoint's own headers go on first, so the signature headers below
	// cannot be overwritten by configuration.
	for k, vs := range endpoint.Header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set(HeaderDelivery, DeliveryID(e))
	req.Header.Set(HeaderEvent, string(e.Kind))
	req.Header.Set(HeaderTopic, e.Topic.String())
	sign(req.Header, endpoint.Secret, c.now(), body)

	resp, err := c.http.Do(req)
	if err != nil {
		// A transport failure is transient by default: a refused connection is a
		// receiver restarting, and a timeout is a receiver that is slow.
		return fmt.Errorf("webhook: post %s: %w", uri, err)
	}
	defer func() {
		// Drain before closing so the connection returns to the pool; a busy run
		// posts hundreds of events to the same host.
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
		_ = resp.Body.Close()
	}()

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return nil
	}
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	detail := fmt.Sprintf("post %s: %d %s", uri, resp.StatusCode, http.StatusText(resp.StatusCode))
	if text := strings.TrimSpace(string(snippet)); text != "" {
		detail += ": " + text
	}
	if retryable(resp.StatusCode) {
		return fmt.Errorf("webhook: %s", detail)
	}
	// Permanent. Saying so is what stops the router from spending its own retry
	// budget on a request the receiver has already judged.
	return fmt.Errorf("%w: webhook: %s", connector.ErrUndeliverable, detail)
}

// retryable reports whether repeating a request that got this status could
// plausibly succeed.
//
// 408 and 429 are the two 4xx that are about timing rather than about the
// request: "you were slow" and "you were fast". Everything else in the 4xx range
// is a statement about the request itself, and the request will be identical
// next time.
func retryable(status int) bool {
	return status == http.StatusRequestTimeout ||
		status == http.StatusTooManyRequests ||
		status >= 500
}

// backoffFor returns the wait before the attempt after this one, doubling from
// the base and capped. It doubles by multiplication rather than by shifting so a
// high attempt count cannot overflow into a negative duration.
func (c *Connector) backoffFor(attempt int) time.Duration {
	wait := c.backoff
	for i := 1; i < attempt; i++ {
		if wait >= c.maxBackoff/2 {
			return c.maxBackoff
		}
		wait *= 2
	}
	return wait
}

// resolve turns a target address into the endpoint that serves it and the URL to
// POST to.
//
// The path is appended escaped, segment by segment, from the address's decoded
// segments — so a target carrying a "/" inside one segment cannot climb out of
// the endpoint's base path. Query parameters on the address are passed through,
// because a receiver may use them to route.
func (c *Connector) resolve(target address.Address) (Endpoint, string, error) {
	if target.Connector != Name {
		return Endpoint{}, "", fmt.Errorf("%w: %s is not a webhook address", connector.ErrUndeliverable, target)
	}
	endpoint, ok := c.endpoints[target.Workspace]
	if !ok {
		return Endpoint{}, "", fmt.Errorf("%w: %s names workspace %q, which this connector has no endpoint for",
			connector.ErrUndeliverable, target, target.Workspace)
	}

	var b strings.Builder
	b.WriteString(endpoint.BaseURL)
	for _, segment := range target.Path {
		b.WriteByte('/')
		b.WriteString(url.PathEscape(segment))
	}
	if len(target.Params) > 0 {
		q := url.Values{}
		for k, v := range target.Params {
			q.Set(k, v)
		}
		b.WriteByte('?')
		b.WriteString(q.Encode())
	}
	return endpoint, b.String(), nil
}
