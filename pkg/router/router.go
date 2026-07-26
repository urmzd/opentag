// Package router is the delivery half of the mesh: it turns "the agent said
// something" into "the surface shows it".
//
// Delivery is a subscription, not a return value. A run publishes its events to
// the bus once and hands nothing back to whoever tagged it; the router is just
// another subscriber, and it renders what it sees onto whichever surfaces the
// run's routes named. That indirection is the whole reason origin and
// destination are independent: a tag raised in GitHub can answer into Jira
// because nothing on the path from trigger to sink ever holds both ends at
// once. They share a run id and nothing else.
//
// The router owns exactly one piece of state, the map from a run to the routes
// it was accepted with, and it is deliberately not durable. The bus owns
// sequencing and retention, the connector owns how a surface is written, and
// duraturo's ledger owns what happened. Losing a router loses in-flight
// renders, not runs: the runtime registers a run's routes at accept time and
// registers them again when it replays.
//
// # Lanes, and why they exist
//
// Delivery is N:M. One event fans out to every route that wants its kind, and
// one sink serves every run that names it. Both directions want concurrency,
// and exactly one pairing must not have it: events for one run going to one
// target are carried by a single goroutine reading a FIFO queue — a lane — so
// they arrive in Seq order. That is not fussiness. A Slack sink renders a run
// by editing one message, and two edits that overtake each other leave the
// wrong final text on the surface permanently, long after the run that
// produced them is forgotten. Across lanes there is no order to keep and none
// is imposed, so a stalled Jira sink cannot delay a webhook and one run's slow
// surface cannot delay another run's.
//
// Two routes naming the same target are one lane, and an event both of them
// select is delivered once. The target is the surface, and rendering it twice
// would show the work twice.
//
// # Transient and permanent
//
// The bus is at-least-once and sinks are required to tolerate redelivery, but
// at-least-once still has to mean at least once: a sink that returns a 503 is
// retried with bounded exponential backoff. What is never retried is a failure
// no amount of waiting can fix — connector.ErrUndeliverable and
// connector.ErrUnsupported (a cron schedule has no surface to render into, and
// a route to one is not a fault to recover from), connector.ErrNotFound, and a
// panic, which is a defect in the sink rather than congestion in front of it.
// A permanent failure retires that one delivery; the lane moves on to the next
// event rather than stranding the run behind a target that will never accept
// it.
//
// # Failure is loud
//
// Every delivery this package gives up on is logged at error level with the
// run, the target, the sequence, the kind and the attempt count, because a
// silently dropped delivery is the worst failure this package could have: the
// agent did the work, the ledger says so, and the human waiting on the surface
// sees nothing. Events for runs this router has no routes for are the one
// exception and are logged at debug level — on a multi-node deployment every
// router sees every other node's runs, and treating that as an error would
// bury the real ones.
package router

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/bus"
	"github.com/urmzd/opentag/pkg/connector"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/topic"
)

// Errors reported by the router. Callers match with errors.Is.
var (
	// ErrInvalid reports a registration the router cannot serve.
	ErrInvalid = errors.New("router: invalid")
	// ErrPanic reports a sink that panicked. It is permanent: a panic is a
	// defect in the sink, not congestion in front of it, and retrying a defect
	// only multiplies it.
	ErrPanic = errors.New("router: sink panicked")
	// ErrRunning reports a second concurrent Run on one router.
	ErrRunning = errors.New("router: already running")
)

// Defaults for the options below.
const (
	// DefaultBacklog is the per-lane queue depth: how many events for one
	// (run, target) pair may be waiting while that sink works.
	DefaultBacklog = 256
	// DefaultConcurrency bounds Deliver calls in flight across every lane.
	DefaultConcurrency = 64
	// DefaultAttempts is how many times one event is offered to one sink
	// before the delivery is retired, counting the first try.
	DefaultAttempts = 4
	// DefaultBackoff is the wait before the second attempt; it doubles from
	// there.
	DefaultBackoff = 100 * time.Millisecond
	// DefaultMaxBackoff caps the wait between attempts.
	DefaultMaxBackoff = 5 * time.Second
	// DefaultTimeout bounds one Deliver call. It exists so a sink that ignores
	// its context cannot hold a lane, or shutdown, open forever.
	DefaultTimeout = 30 * time.Second
)

// Resolver resolves a delivery target to the sink that can render onto it.
// *connector.Registry is what a deployment normally passes; the interface
// exists so a router can be pointed at a resolver that decides per tenant or
// per workspace without the registry growing that concern.
type Resolver interface {
	Sink(target address.Address) (connector.Sink, error)
}

var _ Resolver = (*connector.Registry)(nil)

// Option configures a Router. Non-positive and nil values are ignored, so a
// caller can pass a zero-valued config through without erasing the defaults.
type Option func(*Router)

// WithTopic scopes the subscription the router serves (default the root topic,
// every agent). A deployment that shards runs across nodes gives each node the
// topic it owns, and the runs it does not own never reach its dispatch loop.
func WithTopic(t topic.Topic) Option {
	return func(r *Router) {
		if !t.IsZero() {
			r.topic = t
		}
	}
}

// WithBacklog sets the per-lane queue depth (default 256). It is the only
// back-pressure a sink gets: when a lane's queue is full the event is dropped
// for that target and logged at error level, because the alternative is one
// unresponsive surface stalling every other surface and every other run.
func WithBacklog(n int) Option {
	return func(r *Router) {
		if n > 0 {
			r.backlog = n
		}
	}
}

// WithConcurrency bounds Deliver calls in flight across all lanes (default 64).
// A lane holds a slot only for one attempt and never across a backoff wait, so
// a sink that is retrying does not occupy the budget while it waits.
func WithConcurrency(n int) Option {
	return func(r *Router) {
		if n > 0 {
			r.concurrency = n
		}
	}
}

// WithAttempts sets how many times one event is offered to one sink before the
// delivery is retired, counting the first try (default 4). A permanent failure
// ends the delivery regardless of what is left.
func WithAttempts(n int) Option {
	return func(r *Router) {
		if n > 0 {
			r.attempts = n
		}
	}
}

// WithBackoff sets the wait before the second attempt and the cap it doubles
// towards (defaults 100ms and 5s). The schedule carries no jitter: lanes are
// independent and start their attempts at whatever moment their own event
// arrived, so there is no herd to spread.
func WithBackoff(base, max time.Duration) Option {
	return func(r *Router) {
		if base > 0 {
			r.backoff = base
		}
		if max > 0 {
			r.maxBackoff = max
		}
	}
}

// WithTimeout bounds one Deliver call (default 30s). Zero or negative disables
// it, which makes the router's shutdown only as prompt as its slowest sink.
func WithTimeout(d time.Duration) Option {
	return func(r *Router) { r.timeout = d }
}

// WithLogger sets the logger (default slog.Default). Permanent failures are
// logged at error level and dropped events for unrouted runs at debug level,
// so a deployment that wants to see the latter must enable debug.
func WithLogger(l *slog.Logger) Option {
	return func(r *Router) {
		if l != nil {
			r.log = l
		}
	}
}

// Router subscribes to the bus and delivers each event to the sinks a run's
// routes selected. One Router serves every run on a node; it is safe for
// concurrent use, and Register and Release may be called before, during and
// after Run.
type Router struct {
	bus   bus.Bus
	sinks Resolver
	log   *slog.Logger

	topic       topic.Topic
	backlog     int
	concurrency int
	attempts    int
	backoff     time.Duration
	maxBackoff  time.Duration
	timeout     time.Duration

	// sem bounds Deliver calls in flight across every lane.
	sem chan struct{}

	// mu guards runs, live, running and every lane's queue. Offers are made
	// under it so that a lane cannot be closed by Release between the lookup
	// and the send.
	mu   sync.Mutex
	runs map[string]*run
	// live is every surface a lane goroutine is still working on, keyed by
	// (run, target), and it outlives r.runs: a released or re-registered run
	// drops its lanes from st.lanes immediately, while the goroutines drain on.
	// A new lane for a surface already in here chains behind the one it finds,
	// which is what keeps one surface to one goroutine across replacement.
	live    map[string]*lane
	running bool
	// wg tracks lane goroutines. Add is only ever called from the dispatch
	// loop, and running stays true until Wait has returned, so no other Run
	// can be dispatching while this one waits.
	wg sync.WaitGroup
}

// New returns a router that reads b and delivers through the sinks that resolve
// names. It starts nothing: call Run.
func New(b bus.Bus, sinks Resolver, opts ...Option) *Router {
	if b == nil || sinks == nil {
		panic("router: New requires a bus and a resolver")
	}
	r := &Router{
		bus:         b,
		sinks:       sinks,
		log:         slog.Default(),
		topic:       topic.All(),
		backlog:     DefaultBacklog,
		concurrency: DefaultConcurrency,
		attempts:    DefaultAttempts,
		backoff:     DefaultBackoff,
		maxBackoff:  DefaultMaxBackoff,
		timeout:     DefaultTimeout,
		runs:        make(map[string]*run),
		live:        make(map[string]*lane),
	}
	for _, opt := range opts {
		opt(r)
	}
	r.sem = make(chan struct{}, r.concurrency)
	return r
}

// run is one run's delivery state: the routes it was accepted with, the lanes
// serving them, and how far its stream has been dispatched.
type run struct {
	bound []binding
	lanes map[string]*lane
	// seq is the highest sequence dispatched for this run. The bus is
	// at-least-once and a resubscribe replays whatever it still retains, so
	// the router keeps the per-run cursor the bus tells consumers to keep.
	seq uint64
}

// binding is one route with its target already resolved. Resolving at
// registration rather than per event keeps a misconfigured route to one log
// line instead of one per event, and keeps the dispatch loop free of a lookup
// that cannot change while the run lives.
type binding struct {
	route envelope.Route
	sink  connector.Sink
}

// Register records the routes a run delivers on. The runtime calls it at accept
// time, before it publishes the run's first event: an event that arrives for a
// run with no routes has nowhere to go and is dropped.
//
// Registering a run again replaces its routes and keeps its cursor, which is
// what replay needs — duraturo re-runs a workflow from the top, and the routes
// it re-registers are the same ones it pinned. Lanes for targets that are no
// longer routed drain and exit.
//
// A route whose target no sink can receive is dropped here and logged, not
// returned as an error: it is permanent and knowable now (a cron schedule has
// no surface), and a run whose events cannot be rendered anywhere still runs
// and still publishes to the bus. Register reports ErrInvalid only for input
// the router could never make sense of: no run id, or a route with no target.
func (r *Router) Register(runID string, routes []envelope.Route) error {
	if runID == "" {
		return fmt.Errorf("%w: register with no run id", ErrInvalid)
	}
	bound := make([]binding, 0, len(routes))
	for i, route := range routes {
		if err := route.Validate(); err != nil {
			return fmt.Errorf("%w: run %q route %d: %w", ErrInvalid, runID, i, err)
		}
		sink, err := r.sinks.Sink(route.Target)
		if err != nil {
			r.log.Error("router: route has no sink, its events cannot be delivered",
				"run", runID,
				"target", route.Target.String(),
				"connector", route.Target.Connector,
				"error", err)
			continue
		}
		bound = append(bound, binding{route: route, sink: sink})
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.runs[runID]
	if st == nil {
		st = &run{lanes: make(map[string]*lane)}
		r.runs[runID] = st
	}
	st.bound = bound
	for key, ln := range st.lanes {
		if !routed(bound, key) {
			ln.close()
			delete(st.lanes, key)
		}
	}
	return nil
}

// Release forgets a run's routes. Its lanes drain: whatever is already queued
// is still delivered, and each lane's goroutine exits once it is. Release does
// not wait for that — a finished run must not be able to block on the surface
// it was rendering into — but Run does not return until every lane it started
// has exited, so a Run that has returned has left nothing behind.
//
// Events that arrive for the run after Release are dropped, which is the
// correct reading of a released run: nothing is listening for it here any more.
func (r *Router) Release(runID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st := r.runs[runID]
	if st == nil {
		return
	}
	delete(r.runs, runID)
	for _, ln := range st.lanes {
		ln.close()
	}
}

// Run subscribes to the bus and dispatches until ctx is cancelled. It returns
// nil on a clean stop and an error only if the subscription itself failed in a
// way resubscribing cannot fix.
//
// Falling behind the bus is not such a failure: ErrSlowConsumer is answered by
// resubscribing, and the per-run cursor the router keeps means the replay that
// follows costs duplicates the bus already asked consumers to tolerate rather
// than double deliveries.
//
// Only one Run may be active per router; a second returns ErrRunning. When Run
// returns, every lane goroutine it started has exited.
func (r *Router) Run(ctx context.Context) error {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return ErrRunning
	}
	r.running = true
	r.mu.Unlock()

	defer func() {
		// Cancellation abandons what is queued rather than draining it: the
		// context the lanes deliver under is this one, so a drain would only
		// produce failed attempts. Each lane says what it dropped.
		//
		// The lanes are forgotten as well as closed. A run stays registered
		// across a restart — its routes are still pinned and its cursor is
		// still valid — but its lanes belong to the Run that started them, and
		// a later Run must build its own rather than reuse a closed queue.
		r.mu.Lock()
		for _, st := range r.runs {
			for key, ln := range st.lanes {
				ln.close()
				delete(st.lanes, key)
			}
		}
		r.mu.Unlock()

		// The router stays claimed until every lane has exited. Clearing it
		// before the wait would let a second Run start dispatching — and so
		// call r.wg.Add — while this one is inside r.wg.Wait, which is a
		// WaitGroup misuse and would leave this Run waiting on lanes it never
		// started.
		r.wg.Wait()
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}
		stream, err := r.bus.Subscribe(ctx, envelope.Subscription{Topic: r.topic})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("router: subscribe %s: %w", r.topic, err)
		}
		err = r.consume(ctx, stream)
		_ = stream.Close()

		switch {
		case ctx.Err() != nil:
			return nil
		case errors.Is(err, bus.ErrClosed):
			// Nobody here closed it, so something else decided delivery is
			// over. Say so: a router that stops quietly looks exactly like an
			// agent with nothing to say.
			r.log.Warn("router: bus stream ended, delivery has stopped",
				"topic", r.topic.String(), "error", err)
			return nil
		case errors.Is(err, bus.ErrSlowConsumer):
			r.log.Warn("router: fell behind the bus, resubscribing",
				"topic", r.topic.String(), "error", err)
		default:
			return fmt.Errorf("router: recv on %s: %w", r.topic, err)
		}
	}
}

// consume reads one subscription to exhaustion. It returns the error that ended
// it, which is always non-nil: a stream ends by failing.
func (r *Router) consume(ctx context.Context, stream bus.Stream) error {
	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			return err
		}
		r.dispatch(ctx, e)
	}
}

// dispatch hands one event to every lane that wants it. It holds r.mu for the
// whole fan-out: the offers are non-blocking sends, and doing them under the
// same lock that Release closes lanes with is what makes "deliver to a lane
// that was just released" impossible rather than merely unlikely.
func (r *Router) dispatch(ctx context.Context, e envelope.Event) {
	runID := e.RunID
	if runID == "" {
		runID = e.Topic.RunID() // the bus fills this in, but do not depend on it
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	st := r.runs[runID]
	if st == nil {
		r.log.Debug("router: event for a run this router has no routes for, dropped",
			"run", runID, "agent", e.Agent, "seq", e.Seq, "kind", string(e.Kind))
		return
	}
	if e.Seq != 0 && e.Seq <= st.seq {
		return // a redelivery or a replay: already dispatched
	}
	st.seq = e.Seq

	var offered []*lane
	for _, b := range st.bound {
		if !b.route.Wants(e.Kind) {
			continue
		}
		ln := r.laneFor(ctx, st, runID, b)
		if contains(offered, ln) {
			continue // two routes, one surface, one delivery
		}
		offered = append(offered, ln)
		ln.offer(e)
	}
}

// laneFor returns the lane for b's target, starting it on the first event that
// selects it: a run that never emits a kind a route wants never costs a
// goroutine. A lane whose predecessor on the same surface is still draining
// chains behind it, so the surface keeps one writer. Callers hold r.mu.
func (r *Router) laneFor(ctx context.Context, st *run, runID string, b binding) *lane {
	key := b.route.Target.String()
	if ln := st.lanes[key]; ln != nil {
		return ln
	}
	ln := &lane{
		r:      r,
		key:    surface(runID, key),
		runID:  runID,
		target: b.route.Target,
		sink:   b.sink,
		queue:  make(chan envelope.Event, r.backlog),
		done:   make(chan struct{}),
	}
	if prev := r.live[ln.key]; prev != nil {
		ln.after = prev.done
	}
	r.live[ln.key] = ln
	st.lanes[key] = ln
	r.wg.Add(1)
	go ln.serve(ctx)
	return ln
}

// surface names one (run, target) pair. The separator is a byte no address and
// no run id can carry, so two surfaces cannot collide by concatenation.
func surface(runID, target string) string { return runID + "\x00" + target }

// acquire takes a slot in the delivery budget.
func (r *Router) acquire(ctx context.Context) error {
	select {
	case r.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Router) release() { <-r.sem }

// routed reports whether key is still the target of one of these bindings.
func routed(bound []binding, key string) bool {
	for _, b := range bound {
		if b.route.Target.String() == key {
			return true
		}
	}
	return false
}

// contains reports whether ln has already been offered this event. Runs carry a
// handful of routes, so a scan beats a set.
func contains(lanes []*lane, ln *lane) bool {
	for _, have := range lanes {
		if have == ln {
			return true
		}
	}
	return false
}
