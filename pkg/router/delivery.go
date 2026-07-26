package router

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/connector"
	"github.com/urmzd/opentag/pkg/envelope"
)

// lane is the delivery path for one (run, target) pair: a bounded FIFO queue
// and the single goroutine that drains it. One goroutine is the entire ordering
// guarantee — events enter the queue in the order the dispatch loop read them
// from the bus, which is Seq order within a run, and leave it one at a time.
//
// Retries happen inside the lane, so an event being retried holds the lane and
// the events behind it wait. That is deliberate: a sink that renders a run by
// editing one message must not receive delta 8 while delta 7 is still being
// retried. A delivery that is retired for good releases the lane rather than
// stranding the run behind a target that will never accept it.
//
// A closed lane keeps draining, so "one goroutine per surface" has to survive a
// lane being replaced: Release then Register, a route retired and routed again,
// or a Run restarting all build a second lane for a surface whose first lane is
// still working. The replacement therefore chains behind its predecessor —
// after is the predecessor's done — and delivers nothing until the lane it
// replaced has exited. Chaining rather than reusing, because the queue of a
// closed lane can no longer be sent on; waiting in serve rather than in
// dispatch, because dispatch holds r.mu and must never block on a sink.
type lane struct {
	r      *Router
	key    string // (run, target): identifies the surface across replacements
	runID  string
	target address.Address
	sink   connector.Sink

	queue chan envelope.Event
	// closed guards the queue against a second close: Release and the shutdown
	// path can both reach a lane, and both are cheap enough to be idempotent.
	closed sync.Once
	// after is the predecessor lane on this surface, nil if there was none.
	// done is closed once this lane's goroutine has exited, which is what a
	// successor waits on.
	after <-chan struct{}
	done  chan struct{}
}

// offer queues e without ever blocking. A full queue means this sink is not
// keeping up, and the event is dropped for this target only: the alternative is
// letting one unresponsive surface stall every other surface and every other
// run. The drop is logged at error level, because a delivery nobody knows was
// lost is worse than one that was. Callers hold r.mu.
func (l *lane) offer(e envelope.Event) {
	// One copy per lane: a sink that mutates the payload it was handed cannot
	// reach what another sink sees.
	e.Payload = bytes.Clone(e.Payload)
	select {
	case l.queue <- e:
	default:
		l.r.log.Error("router: delivery dropped, sink is not keeping up",
			l.attrs(e, "queue", cap(l.queue))...)
	}
}

// close stops the lane once it has drained. It is idempotent and never blocks.
// Callers hold r.mu.
func (l *lane) close() {
	l.closed.Do(func() { close(l.queue) })
}

// serve drains the queue until it is closed and empty, or until ctx is
// cancelled. Cancellation abandons what is left — the sinks deliver under this
// same context, so there is nothing left to deliver with — and says how much.
//
// It delivers nothing until the lane it replaced has exited, so a surface is
// never rendered by two goroutines at once and the events queued behind a
// replaced lane still reach the sink before the ones queued after it.
func (l *lane) serve(ctx context.Context) {
	defer l.r.wg.Done()
	defer l.exit()
	if l.after != nil {
		select {
		case <-l.after:
		case <-ctx.Done():
			l.abandon()
			return
		}
	}
	for {
		if ctx.Err() != nil {
			l.abandon()
			return
		}
		select {
		case e, ok := <-l.queue:
			if !ok {
				return // released and drained
			}
			l.deliver(ctx, e)
		case <-ctx.Done():
			l.abandon()
			return
		}
	}
}

// exit releases the surface. It forgets this lane unless a successor has
// already claimed the surface — that successor is waiting on done and must stay
// findable — and then releases whoever is waiting.
func (l *lane) exit() {
	l.r.mu.Lock()
	if l.r.live[l.key] == l {
		delete(l.r.live, l.key)
	}
	l.r.mu.Unlock()
	close(l.done)
}

// abandon reports what shutdown cost this lane. It is a warning rather than an
// error: the events are still on the bus and still in the ledger, and a router
// that starts again re-renders them.
func (l *lane) abandon() {
	if n := len(l.queue); n > 0 {
		l.r.log.Warn("router: undelivered events abandoned at shutdown",
			"run", l.runID,
			"target", l.target.String(),
			"connector", l.target.Connector,
			"pending", n)
	}
}

// deliver renders one event, retrying transient failures with bounded
// exponential backoff. It returns when the event has been delivered or when
// retrying it is pointless, never before.
func (l *lane) deliver(ctx context.Context, e envelope.Event) {
	for attempt := 1; ; attempt++ {
		err := l.attempt(ctx, e)
		switch {
		case err == nil:
			if attempt > 1 {
				l.r.log.Info("router: delivery succeeded after retrying",
					l.attrs(e, "attempts", attempt)...)
			}
			return

		case ctx.Err() != nil:
			// Shutdown, not a sink failure: whoever cancelled knows why.
			l.r.log.Warn("router: delivery abandoned at shutdown",
				l.attrs(e, "attempts", attempt, "error", err)...)
			return

		case permanent(err):
			l.retire(e, err, attempt, "permanent")
			return

		case attempt >= l.r.attempts:
			l.retire(e, err, attempt, "attempts exhausted")
			return
		}

		wait := l.r.backoffFor(attempt)
		l.r.log.Warn("router: delivery failed, retrying",
			l.attrs(e, "attempt", attempt, "retry_in", wait.String(), "error", err)...)
		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
		}
	}
}

// attempt is one Deliver call: it takes a slot in the delivery budget, bounds
// the call, and converts a panicking sink into a failed delivery instead of a
// dead router. The slot is released before any backoff wait, so a retrying sink
// does not hold budget while it waits.
func (l *lane) attempt(ctx context.Context, e envelope.Event) (err error) {
	if err := l.r.acquire(ctx); err != nil {
		return err
	}
	defer l.r.release()

	call := ctx
	if l.r.timeout > 0 {
		var cancel context.CancelFunc
		call, cancel = context.WithTimeout(ctx, l.r.timeout)
		defer cancel()
	}

	defer func() {
		if v := recover(); v != nil {
			err = &panicError{value: v, stack: debug.Stack()}
		}
	}()
	return l.sink.Deliver(call, l.target, e)
}

// retire logs a delivery the router is giving up on. This is the log line that
// matters: the agent did the work, the ledger says so, and the surface will
// never show it, so everything needed to find out why is on one line.
func (l *lane) retire(e envelope.Event, err error, attempts int, reason string) {
	attrs := l.attrs(e, "attempts", attempts, "reason", reason, "error", err)
	var p *panicError
	if errors.As(err, &p) {
		attrs = append(attrs, "stack", string(p.stack))
	}
	l.r.log.Error("router: delivery failed, giving up", attrs...)
}

// attrs is the identity of one delivery: enough to find the run in the ledger,
// the event on the bus, and the surface it was meant for.
func (l *lane) attrs(e envelope.Event, extra ...any) []any {
	attrs := []any{
		"run", l.runID,
		"agent", e.Agent,
		"rev", e.Rev,
		"seq", e.Seq,
		"kind", string(e.Kind),
		"target", l.target.String(),
		"connector", l.target.Connector,
	}
	return append(attrs, extra...)
}

// backoffFor returns the wait before the attempt after this one, doubling from
// the base and capped. It doubles by multiplication rather than by shifting so
// a high attempt count cannot overflow into a negative duration.
func (r *Router) backoffFor(attempt int) time.Duration {
	wait := r.backoff
	for i := 1; i < attempt; i++ {
		if wait >= r.maxBackoff/2 {
			return r.maxBackoff
		}
		wait *= 2
	}
	return wait
}

// permanent reports whether err describes a condition no amount of waiting can
// fix. Everything else is transient, which is the safe default: retrying an
// error that turns out to be permanent costs a few attempts, while treating a
// transient error as permanent costs the delivery.
func permanent(err error) bool {
	for _, sentinel := range []error{
		connector.ErrUndeliverable, // not a surface this connector can reach
		connector.ErrUnsupported,   // this connector cannot receive at all
		connector.ErrNotFound,      // no such connector in this deployment
		ErrPanic,                   // a defect in the sink, not congestion
		envelope.ErrInvalid,        // unrenderable, and the event will not change
	} {
		if errors.Is(err, sentinel) {
			return true
		}
	}
	return false
}

// panicError is a recovered sink panic, carrying the stack so the log line that
// retires the delivery can name the line that broke.
type panicError struct {
	value any
	stack []byte
}

func (p *panicError) Error() string { return fmt.Sprintf("router: sink panicked: %v", p.value) }

// Unwrap makes a recovered panic match ErrPanic, so classification stays in one
// place and a caller can errors.Is for it.
func (p *panicError) Unwrap() error { return ErrPanic }
