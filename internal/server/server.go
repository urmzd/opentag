// Package server is mandatum's transport edge.
//
// It owns three things, and deliberately nothing else:
//
//   - The wire, in both directions. The generated mandatum.v1 contract on one
//     side, the domain types (envelope, topic, address) on the other, with the
//     translation confined to convert.go so no handler has to think about it.
//   - Who the caller is. Every request's tenant is derived from its credential
//     here and stamped onto everything that leaves.
//   - How long a stream lives. Client disconnect, server drain, and a consumer
//     that falls behind all end a stream at this layer rather than somewhere
//     deep in a handler.
//
// What it does not own is what keeps it small. It does not decide what an agent
// is (a Store does), where events come from or go (the bus and the router do),
// or how a run executes (the runtime does). Each of those arrives as a narrow
// interface declared here, so the edge compiles and is testable before any of
// them exist, and so a fake is always one struct literal away.
//
// # Tenant
//
// Tenant is an authorization scope, never a value the wire may claim. No
// request message in mandatum.v1 carries one. This package derives it from the
// caller's credential (see Authenticator) and overwrites the field on
// everything it forwards: the Tag handed to the Invoker, the Event handed to
// the bus, and the Event handed back to a subscriber. A client that hand-rolls
// a JSON body with a tenant in it does not get that tenant, and a subscriber
// receives only events whose tenant equals its own. The check happens on the
// read path as well as the write path, because the bus does not authorize — it
// addresses.
//
// A webhook is the one caller with no credential to present: its shared secret
// is its credential. So a webhook's tenant comes from the Ingress it was
// registered under (see webhook.go), never from the payload, which is
// attacker-shaped by definition.
//
// # Streams
//
// Three endpoints are long-lived: BusService.Subscribe, InvokeService.
// InvokeStream, and the SSE endpoint. Each derives its context from
// streamContext, which ends when the client goes away or when the server starts
// draining, so a handler watches one thing and Shutdown can wait for all of
// them. Nothing here holds a goroutine that outlives the request: the SSE pump
// is the only goroutine any handler starts, and it exits when the stream it
// reads is closed.
//
// # HTTP/2
//
// Connect's server-streaming works over HTTP/1.1, so the handler this package
// returns serves SSE, JSON, and Connect streaming on a plain listener. gRPC
// clients need HTTP/2, which means either TLS (Serve over a tls.Listener) or an
// h2c wrapper supplied by the caller — this package adds no dependency for it.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"github.com/urmzd/dispatch/pkg/metrics"
	"github.com/urmzd/mandatum/gen/mandatum/v1/mandatumv1connect"
	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

// Errors a dependency returns to tell the edge which answer to give. They are
// declared here, rather than imported from the packages that will implement
// these interfaces, so the edge does not depend on those packages: a Store or a
// runtime translates its own sentinels into these once, at the seam, and every
// handler maps them to a status code identically.
//
// An error that matches none of them is reported as an internal error, and its
// text is logged rather than returned: an unclassified failure is the one most
// likely to be quoting a database, a file path, or another tenant's data.
var (
	// ErrNotFound reports an agent, revision, or run that does not exist, or
	// that the caller's tenant may not see. The two are one error on purpose:
	// distinguishing them would confirm existence across a tenant boundary.
	ErrNotFound = errors.New("server: not found")

	// ErrExists reports a create against a name that is already taken.
	ErrExists = errors.New("server: already exists")

	// ErrConflict reports a failed optimistic concurrency check: the agent
	// moved on since the revision the caller believed was current.
	ErrConflict = errors.New("server: conflict")

	// ErrInvalid reports a request the edge or a dependency will not accept.
	ErrInvalid = errors.New("server: invalid")

	// ErrUnavailable reports a dependency that is temporarily unable to serve,
	// which is the one class of failure a client should retry unchanged.
	ErrUnavailable = errors.New("server: unavailable")
)

// Defaults for Config. Each is the value a deployment can leave alone.
const (
	// DefaultHeartbeat is how often an idle SSE stream emits a comment. It is
	// well inside the 30 to 60 seconds that proxies and load balancers
	// typically use for an idle-connection timeout, because a stream that is
	// silent while an agent thinks is the normal case, not an error.
	DefaultHeartbeat = 15 * time.Second

	// DefaultRetry is the reconnection delay advertised to SSE clients.
	DefaultRetry = 3 * time.Second

	// DefaultMaxBodyBytes caps a webhook body. Every surface mandatum ingests
	// from sends payloads far below this, and the cap exists because the body
	// must be buffered whole before it can be authenticated.
	DefaultMaxBodyBytes = 1 << 20

	// DefaultShutdownGrace bounds how long Serve waits for in-flight streams
	// and requests to end after the context is cancelled.
	DefaultShutdownGrace = 15 * time.Second
)

// Metric names this package records through a metrics.Recorder.
const (
	MetricRequests    = "mandatum_server_requests_total"
	MetricStreams     = "mandatum_server_streams_total"
	MetricStreamsOpen = "mandatum_server_streams_open"
	MetricEvents      = "mandatum_server_events_streamed_total"
	MetricWebhooks    = "mandatum_server_webhooks_total"
	MetricInvocations = "mandatum_server_invocations_total"
	MetricUp          = "mandatum_server_up"
)

// Config assembles a Server from the pieces a deployment has. Every dependency
// is optional except Auth, and a service whose dependency is absent answers
// CodeUnimplemented rather than disappearing: a client discovering a feature is
// off should get an answer, not a 404 it has to interpret.
//
// Auth is mandatory because there is no safe default for it. A server with no
// authenticator would have no tenant, and no tenant means every subscriber sees
// every event.
type Config struct {
	// Auth derives the caller's identity from request headers. Required.
	Auth Authenticator

	// Store is the agent control plane. Nil disables AgentService.
	Store Store

	// Bus is the broker. Nil disables BusService, the SSE endpoint, and
	// InvokeService.InvokeStream.
	Bus bus.Bus

	// Invoker accepts tags. Nil disables Invoke, InvokeStream, and webhook
	// ingress.
	Invoker Invoker

	// Runs reads durable run records. Nil disables GetRun.
	Runs RunReader

	// Ingress registers one inbound webhook endpoint per connector, served at
	// POST /v1/webhooks/{connector}.
	Ingress []Ingress

	// Heartbeat is the SSE idle comment interval. Zero takes
	// DefaultHeartbeat.
	Heartbeat time.Duration

	// Retry is the reconnection delay advertised to SSE clients. Zero takes
	// DefaultRetry.
	Retry time.Duration

	// MaxBodyBytes caps a webhook body. Zero takes DefaultMaxBodyBytes.
	MaxBodyBytes int64

	// ShutdownGrace bounds Serve's drain. Zero takes DefaultShutdownGrace.
	ShutdownGrace time.Duration

	// Metrics receives counters and gauges. Nil discards them.
	Metrics metrics.Recorder

	// Logger receives internal failures, which are logged here rather than
	// returned to the caller. Nil discards them.
	Logger *slog.Logger

	// Now is the clock seam, so a test can assert a stamped timestamp instead
	// of tolerating one. Nil uses time.Now.
	Now func() time.Time
}

// Server is the HTTP surface: the Connect services, the SSE endpoint, webhook
// ingress, health, and metrics, sharing one authenticator and one drain
// signal.
type Server struct {
	auth    Authenticator
	bus     bus.Bus
	ingress map[string]Ingress
	mux     *http.ServeMux

	heartbeat time.Duration
	retry     time.Duration
	maxBody   int64
	grace     time.Duration

	metrics metrics.Recorder
	log     *slog.Logger
	now     func() time.Time

	// drain is cancelled by Shutdown. Every long-lived handler derives its
	// context from it, which is what turns "stop serving" into "every stream
	// returns" without the handlers knowing about shutdown at all.
	drain     context.Context
	endDrain  context.CancelFunc
	draining  atomic.Bool
	closeOnce sync.Once

	openStreams atomic.Int64

	mu   sync.Mutex
	http *http.Server
}

// New validates cfg and wires the routes. It fails only on a configuration a
// deployment could not have meant: no authenticator, a webhook with no
// verifier, two webhooks for one connector.
func New(cfg Config) (*Server, error) {
	if cfg.Auth == nil {
		return nil, fmt.Errorf("%w: server needs an authenticator; without one every subscriber would see every tenant", ErrInvalid)
	}
	s := &Server{
		auth:      cfg.Auth,
		bus:       cfg.Bus,
		ingress:   make(map[string]Ingress, len(cfg.Ingress)),
		mux:       http.NewServeMux(),
		heartbeat: orDuration(cfg.Heartbeat, DefaultHeartbeat),
		retry:     orDuration(cfg.Retry, DefaultRetry),
		maxBody:   orInt64(cfg.MaxBodyBytes, DefaultMaxBodyBytes),
		grace:     orDuration(cfg.ShutdownGrace, DefaultShutdownGrace),
		metrics:   cfg.Metrics,
		log:       cfg.Logger,
		now:       cfg.Now,
	}
	if s.metrics == nil {
		s.metrics = metrics.Nop()
	}
	if s.log == nil {
		s.log = slog.New(slog.DiscardHandler)
	}
	if s.now == nil {
		s.now = func() time.Time { return time.Now().UTC() }
	}
	s.drain, s.endDrain = context.WithCancel(context.Background())

	for _, in := range cfg.Ingress {
		if err := in.validate(); err != nil {
			return nil, err
		}
		if _, dup := s.ingress[in.Connector]; dup {
			return nil, fmt.Errorf("%w: two ingress registrations for connector %q", ErrInvalid, in.Connector)
		}
		s.ingress[in.Connector] = in
	}

	// Order matters: the first interceptor is the outermost, so observation wraps
	// authentication and a refused credential is counted rather than invisible.
	// A spike of unauthenticated calls is exactly the thing an operator needs to
	// be able to see.
	opts := connect.WithInterceptors(
		observeInterceptor{metrics: s.metrics},
		authInterceptor{auth: cfg.Auth},
	)
	s.mux.Handle(mandatumv1connect.NewAgentServiceHandler(
		&agentService{store: cfg.Store, log: s.log}, opts))
	s.mux.Handle(mandatumv1connect.NewBusServiceHandler(
		&busService{bus: cfg.Bus, srv: s, log: s.log}, opts))
	s.mux.Handle(mandatumv1connect.NewInvokeServiceHandler(
		&invokeService{invoker: cfg.Invoker, runs: cfg.Runs, bus: cfg.Bus, srv: s, log: s.log}, opts))

	s.mux.HandleFunc("GET /v1/sse", s.handleSSE)
	s.mux.HandleFunc("POST /v1/webhooks/{connector}", s.handleWebhook(cfg.Invoker))
	s.mux.HandleFunc("GET /healthz", s.handleHealth)
	s.mux.HandleFunc("GET /metrics", s.handleMetrics)

	s.metrics.Gauge(MetricUp, 1)
	return s, nil
}

// Handler returns the routed HTTP surface. It is the whole server: Serve is a
// convenience around it, and a deployment that already owns an http.Server can
// mount this instead.
func (s *Server) Handler() http.Handler { return s.mux }

// Serve serves on ln until ctx is cancelled, then drains: streams are told to
// end, in-flight requests are given ShutdownGrace to finish, and Serve returns
// when the listener is closed.
//
// There is no write timeout, deliberately. A write deadline applies to the
// whole response, so any value would cut every stream at that age; idle streams
// are kept honest by the SSE heartbeat and by the client's own context instead.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	hs := &http.Server{
		Handler:           s.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}
	s.mu.Lock()
	s.http = hs
	s.mu.Unlock()

	serveErr := make(chan error, 1)
	go func() { serveErr <- hs.Serve(ln) }()

	select {
	case err := <-serveErr:
		if err == nil || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("server: serve: %w", err)
	case <-ctx.Done():
	}

	grace, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.grace)
	defer cancel()
	err := s.Shutdown(grace)
	<-serveErr // Serve always returns once Shutdown has closed the listener.
	return err
}

// Shutdown stops accepting work and drains what is in flight. It cancels every
// stream context first, so the long-lived handlers return, and only then waits
// for the HTTP server: waiting first would wait forever, because an SSE stream
// has no natural end.
//
// It is idempotent, and safe to call whether or not Serve was ever called.
func (s *Server) Shutdown(ctx context.Context) error {
	s.draining.Store(true)
	s.closeOnce.Do(s.endDrain)

	s.mu.Lock()
	hs := s.http
	s.mu.Unlock()
	if hs == nil {
		return nil
	}
	if err := hs.Shutdown(ctx); err != nil {
		return fmt.Errorf("server: shutdown: %w", err)
	}
	return nil
}

// streamContext derives the context a long-lived handler runs under: it ends
// when the client goes away, when the handler returns, or when the server
// starts draining. Returning one context rather than a context and a channel is
// what keeps every stream loop selecting on a single thing.
func (s *Server) streamContext(parent context.Context) (context.Context, func()) {
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(s.drain, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// streamOpened records a stream starting and returns the function that records
// it ending. The gauge is set from a counter rather than incremented because
// metrics.Recorder has no add-to-gauge, and a set from an atomic is exact.
func (s *Server) streamOpened(transport string) func() {
	s.metrics.Count(MetricStreams, 1, metrics.Label{Key: "transport", Value: transport})
	s.metrics.Gauge(MetricStreamsOpen, float64(s.openStreams.Add(1)))
	return func() {
		s.metrics.Gauge(MetricStreamsOpen, float64(s.openStreams.Add(-1)))
	}
}

// handleHealth reports liveness, and reports it as unavailable while draining
// so a load balancer stops sending new streams to an instance whose existing
// ones are still finishing.
func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if s.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "draining")
		return
	}
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "ok")
}

// Snapshotter is the optional extension a metrics.Recorder implements when it
// can be scraped. metrics.Memory does; a recorder that forwards to a real
// backend does not, and /metrics then serves only liveness.
type Snapshotter interface {
	Snapshot() map[string]float64
}

// handleMetrics serves the recorder's snapshot in Prometheus text format. The
// series names come from the recorder already shaped as name{label="value"},
// so this is a sort and a print, and no exposition library is needed for it.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	snap, ok := s.metrics.(Snapshotter)
	if !ok {
		fmt.Fprintf(w, "# %s is not scrapeable; metrics are forwarded elsewhere\n", MetricUp)
		fmt.Fprintf(w, "%s 1\n", MetricUp)
		return
	}
	values := snap.Snapshot()
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(w, "%s %v\n", name, values[name])
	}
}

// fail turns a dependency's error into the error a client sees. Anything the
// sentinels do not classify is logged and reported as an internal error with no
// detail, because an unclassified failure is the one most likely to be quoting
// something the caller should not read.
func fail(log *slog.Logger, op string, err error) error {
	code := codeOf(err)
	if code == connect.CodeInternal {
		log.Error("server: request failed", "op", op, "error", err)
		return connect.NewError(code, fmt.Errorf("%s: internal error", op))
	}
	return connect.NewError(code, fmt.Errorf("%s: %w", op, err))
}

// codeOf classifies an error. The domain packages' ErrInvalid sentinels are
// included so that a malformed topic or address reaches the client as an
// invalid argument rather than as an internal error.
func codeOf(err error) connect.Code {
	switch {
	case errors.Is(err, ErrUnauthenticated):
		return connect.CodeUnauthenticated
	case errors.Is(err, ErrNotFound):
		return connect.CodeNotFound
	case errors.Is(err, ErrExists):
		return connect.CodeAlreadyExists
	case errors.Is(err, ErrConflict):
		return connect.CodeAborted
	case errors.Is(err, ErrUnavailable):
		return connect.CodeUnavailable
	case errors.Is(err, ErrUnimplemented):
		return connect.CodeUnimplemented
	case errors.Is(err, bus.ErrSlowConsumer):
		return connect.CodeResourceExhausted
	case errors.Is(err, ErrInvalid),
		errors.Is(err, envelope.ErrInvalid),
		errors.Is(err, topic.ErrInvalid),
		errors.Is(err, address.ErrInvalid),
		errors.Is(err, bus.ErrInvalid):
		return connect.CodeInvalidArgument
	case errors.Is(err, context.Canceled):
		return connect.CodeCanceled
	case errors.Is(err, context.DeadlineExceeded):
		return connect.CodeDeadlineExceeded
	default:
		return connect.CodeInternal
	}
}

// ErrUnimplemented reports a service whose dependency was not configured.
var ErrUnimplemented = errors.New("server: not implemented in this deployment")

// httpStatus maps a Connect code onto the status the plain HTTP endpoints
// return. The SSE and webhook endpoints are not Connect procedures, so they
// cannot borrow Connect's own mapping.
func httpStatus(code connect.Code) int {
	switch code {
	case connect.CodeInvalidArgument, connect.CodeOutOfRange, connect.CodeFailedPrecondition:
		return http.StatusBadRequest
	case connect.CodeUnauthenticated:
		return http.StatusUnauthorized
	case connect.CodePermissionDenied:
		return http.StatusForbidden
	case connect.CodeNotFound:
		return http.StatusNotFound
	case connect.CodeAlreadyExists, connect.CodeAborted:
		return http.StatusConflict
	case connect.CodeResourceExhausted:
		return http.StatusTooManyRequests
	case connect.CodeUnimplemented:
		return http.StatusNotImplemented
	case connect.CodeUnavailable:
		return http.StatusServiceUnavailable
	case connect.CodeDeadlineExceeded:
		return http.StatusGatewayTimeout
	case connect.CodeCanceled:
		// 499 is nginx's, not the IANA registry's, but it is the only status
		// that says "the client left" and it never reaches that client anyway.
		return 499
	default:
		return http.StatusInternalServerError
	}
}

// httpFail answers a plain HTTP request. Internal failures are logged and
// answered without detail, exactly as on the Connect path.
func (s *Server) httpFail(w http.ResponseWriter, op string, err error) {
	code := codeOf(err)
	status := httpStatus(code)
	if code == connect.CodeInternal {
		s.log.Error("server: request failed", "op", op, "error", err)
		http.Error(w, op+": internal error", status)
		return
	}
	http.Error(w, op+": "+err.Error(), status)
}

func orDuration(d, fallback time.Duration) time.Duration {
	if d <= 0 {
		return fallback
	}
	return d
}

func orInt64(n, fallback int64) int64 {
	if n <= 0 {
		return fallback
	}
	return n
}
