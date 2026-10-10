// Package bus is the broker: a run publishes its events once, and every
// consumer that asked for them receives them. It is the only thing between a
// producer and a consumer, and the single coupling point between the runtime
// that produces events and everything that watches them — a sink rendering
// into Slack, an operator tailing one run, a dashboard aggregating an agent.
//
// The bus carries envelope.Event and nothing else. What it deliberately does
// not do is as much of the design as what it does:
//
//   - It does not deliver. Turning an event into a comment, a Jira transition
//     or a webhook POST is the router's concern, decided by the Routes pinned
//     on the run. The router is just another subscriber, which is why origin
//     and destination stay independent.
//   - It does not authorize. Tenant is an authorization scope carried by the
//     caller's credential and applied above; a Subscription names a topic,
//     never a tenant.
//   - It is not the record. duraturo's ledger is truth. The bus retains a
//     bounded, recent window per run so a consumer can reconnect without
//     missing the last few seconds — not so it can reconstruct a run from the
//     beginning. A consumer that needs the whole history reads the ledger and
//     joins the bus for the live tail.
//
// # Sequencing
//
// Event.Seq orders events within a run and starts at 1. The bus assigns it at
// publish time when the caller leaves it 0, and honours it when the caller
// sets it — duraturo replays a workflow from the top, so the runtime must be
// able to re-publish the same event with the same number and get the same
// stream back. Re-publishing a sequence a run has already reached is an
// idempotent no-op, not a duplicate: within a run sequences only ever move
// forward, so anything at or below the run's high-water mark has already been
// seen by every subscriber that was there to see it.
//
// Ordering is total within a run and unspecified across runs. Nothing needs
// more: a consumer watching an agent is watching several independent runs, and
// there is no global clock worth pretending to.
//
// # Catch-up and the live tail
//
// Subscription.From is a resume cursor. Subscribe delivers the retained events
// after From first, in order, and then continues live with no gap and no
// duplicate at the seam. Backends achieve that by making the snapshot and the
// join to the fan-out one atomic step, never a snapshot followed by a
// subscribe — the window between the two is the classic way to lose an event.
//
// # Delivery
//
// At-least-once. A consumer may see the same (RunID, Seq) more than once —
// after its own reconnect, after a slow-consumer restart, or because a
// producer replayed — so consumers must be idempotent. Seq is what makes that
// cheap: remember the last sequence you processed per run and drop anything at
// or below it.
//
// # Slow consumers
//
// A slow consumer never slows a publisher and never slows another consumer.
// That is the promise. Every subscriber reads through its own bounded buffer
// (WithBacklog), and how a backend behaves when that buffer fills depends on
// whether it can push back on its own storage: Memory fills the buffer from
// inside the publisher's critical section and so cannot wait — it terminates
// the subscriber, whose Recv drains what it already holds and then fails with
// ErrSlowConsumer — while Redis is already holding the events and simply stops
// reading until the consumer catches up.
//
// So a consumer must be ready for ErrSlowConsumer, and the recovery is always
// the same: resubscribe from the last sequence it processed. Termination is
// chosen over dropping the oldest event because this stream carries a resume
// cursor. A consumer told "you fell behind" recovers exactly or discovers the
// gap; a consumer silently handed a stream with a hole in it cannot tell the
// difference between "the agent said nothing" and "the agent said something I
// never saw".
//
// # Retention
//
// Each run keeps a ring of its most recent events (see WithRetention). When
// the ring wraps the oldest events are dropped, so a subscription resuming
// from a cursor older than the ring receives what is still there rather than
// what it asked for. That is the deliberate consequence of the bus not being
// the record: it is sized for reconnects, and anything wanting more reads the
// ledger.
package bus

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/urmzd/mandatum/pkg/envelope"
)

// Errors returned by every backend. Callers match with errors.Is.
var (
	// ErrInvalid reports an event or subscription the bus cannot carry.
	ErrInvalid = errors.New("bus: invalid")
	// ErrClosed reports a Recv on a stream that was closed, either by Close
	// or by cancelling the context it was subscribed with.
	ErrClosed = errors.New("bus: stream closed")
	// ErrSlowConsumer reports a subscriber terminated for not keeping up.
	// Recovery is to resubscribe from the last sequence processed.
	ErrSlowConsumer = errors.New("bus: slow consumer")
)

// Bus is the broker. Implementations must pass the conformance suite in
// pkg/bus/bustest; both backends in this package are held to it.
type Bus interface {
	// Publish records e and fans it out to every matching subscriber. It
	// assigns e.Seq when the caller left it 0, and treats a sequence the run
	// has already reached as an idempotent no-op. It never blocks on a
	// consumer.
	Publish(ctx context.Context, e envelope.Event) error

	// Subscribe returns a stream of the events sub accepts: everything
	// retained after sub.From first, then the live tail, with no gap and no
	// duplicate at the seam. Cancelling ctx closes the stream and releases
	// everything it holds, exactly as Close does.
	Subscribe(ctx context.Context, sub envelope.Subscription) (Stream, error)
}

// Stream is one consumer's view of the bus. It serves a single goroutine:
// Recv is not safe to call concurrently with itself, while Close is safe from
// anywhere and idempotent.
type Stream interface {
	// Recv returns the next event, blocking until one arrives, ctx is done,
	// or the stream ends. It returns ErrClosed after Close or after the
	// subscription's context was cancelled, and ErrSlowConsumer when this
	// consumer fell too far behind.
	Recv(ctx context.Context) (envelope.Event, error)

	// Close ends the stream and releases its resources. It is idempotent and
	// safe to call while Recv is blocked.
	Close() error
}

// Defaults for the options below.
const (
	// DefaultRetention is the per-run ring depth: enough to cover a
	// reconnect on a chatty run, small enough that ten thousand runs in
	// memory stay affordable.
	DefaultRetention = 1024
	// DefaultBacklog is the per-subscriber buffer depth.
	DefaultBacklog = 256
	// DefaultMaxRuns bounds how many runs Memory retains at once.
	DefaultMaxRuns = 4096
	// DefaultKeyPrefix namespaces every Redis key the bus owns.
	DefaultKeyPrefix = "mandatum"
	// DefaultPollInterval bounds how long a Redis subscriber blocks in one
	// XREAD, and so how quickly it notices a new run or a closed stream.
	DefaultPollInterval = 250 * time.Millisecond
	// DefaultIndexTTL is how long a run stays discoverable to prefix
	// subscribers on Redis after its last event.
	DefaultIndexTTL = 24 * time.Hour
)

// options configures a backend. Retention and Backlog are the contract every
// backend implements; the rest are Redis's.
type options struct {
	retention int
	backlog   int
	maxRuns   int
	prefix    string
	poll      time.Duration
	indexTTL  time.Duration
}

func defaults() options {
	return options{
		retention: DefaultRetention,
		backlog:   DefaultBacklog,
		maxRuns:   DefaultMaxRuns,
		prefix:    DefaultKeyPrefix,
		poll:      DefaultPollInterval,
		indexTTL:  DefaultIndexTTL,
	}
}

func newOptions(opts []Option) options {
	o := defaults()
	for _, fn := range opts {
		fn(&o)
	}
	return o
}

// Option configures a bus backend. Non-positive and empty values are ignored
// so that a caller can pass a zero-valued config through without erasing the
// defaults.
type Option func(*options)

// WithRetention sets how many events each run retains (default 1024). This is
// the reconnect window: a subscriber resuming from a cursor older than the
// oldest retained event gets what is left, not what it asked for.
func WithRetention(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.retention = n
		}
	}
}

// WithBacklog sets the per-subscriber buffer depth (default 256). A subscriber
// that lets it fill is terminated with ErrSlowConsumer rather than being
// allowed to slow the publisher down.
func WithBacklog(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.backlog = n
		}
	}
}

// WithMaxRuns bounds how many runs Memory retains, evicting the run whose last
// event is oldest (default 4096). Redis ignores it: there, run lifetime is the
// key's, bounded by WithIndexTTL and by whatever eviction policy the server is
// configured with.
func WithMaxRuns(n int) Option {
	return func(o *options) {
		if n > 0 {
			o.maxRuns = n
		}
	}
}

// WithKeyPrefix namespaces the Redis keys the bus owns (default "mandatum").
// Memory ignores it.
func WithKeyPrefix(p string) Option {
	return func(o *options) {
		if p != "" {
			o.prefix = p
		}
	}
}

// WithPollInterval sets how long a Redis subscriber blocks in one XREAD
// (default 250ms). It bounds how quickly a prefix subscriber notices a run
// that started after it subscribed, and how long Close waits for the reader to
// come back. Memory ignores it.
func WithPollInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.poll = d
		}
	}
}

// WithIndexTTL sets how long a run stays discoverable to Redis prefix
// subscribers after its last event (default 24h). Memory ignores it.
func WithIndexTTL(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.indexTTL = d
		}
	}
}

// normalize validates e and fills in what the topic already says, so that a
// producer cannot publish an event whose fields disagree with the address it
// is published to. Every backend runs it before touching storage, which is
// what keeps ErrInvalid identical across them.
func normalize(e *envelope.Event) error {
	if e.Topic.IsZero() {
		return fmt.Errorf("%w: event has no topic", ErrInvalid)
	}
	if e.Topic.RunID() == "" {
		return fmt.Errorf("%w: %q is not run-scoped: events publish to agent:<name>:<run> so that every prefix subscriber sees them", ErrInvalid, e.Topic)
	}
	switch {
	case e.RunID == "":
		e.RunID = e.Topic.RunID()
	case e.RunID != e.Topic.RunID():
		return fmt.Errorf("%w: run id %q disagrees with topic %q", ErrInvalid, e.RunID, e.Topic)
	}
	switch {
	case e.Agent == "":
		e.Agent = e.Topic.Name()
	case e.Agent != e.Topic.Name():
		return fmt.Errorf("%w: agent %q disagrees with topic %q", ErrInvalid, e.Agent, e.Topic)
	}
	if e.Kind == "" {
		return fmt.Errorf("%w: event has no kind, and every route and filter selects on it", ErrInvalid)
	}
	if e.At.IsZero() {
		e.At = time.Now().UTC()
	}
	return nil
}

// validate checks a subscription is addressable.
func validate(sub envelope.Subscription) error {
	if sub.Topic.IsZero() {
		return fmt.Errorf("%w: subscription has no topic", ErrInvalid)
	}
	return nil
}
