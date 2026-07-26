// Package sink is the delivery engine every human-facing connector shares: one
// message per run, edited as the run proceeds.
//
// # Why one message and not many
//
// This is the most consequential product decision in the connector layer. An
// agent emits hundreds of text deltas per answer. Posting one Slack message or
// one GitHub comment per delta would make any real channel unusable within a
// single run, and because delivery is at-least-once, a redelivery would post
// every line a second time. So a run owns exactly one message on each surface
// it delivers to, and every subsequent event edits that message.
//
// Editing turns idempotency from bookkeeping into arithmetic. The engine never
// applies a diff to a surface: it re-renders the whole accumulated document (see
// internal/render) and replaces the body. The same set of events therefore
// always produces the same bytes, in any arrival order, however many times each
// one arrives — and when the re-render produces bytes identical to what was last
// written, the engine skips the API call entirely, which is the common case for
// a redelivered event.
//
// # Coalescing, and why there is no goroutine
//
// Editing per token would replace a flood of messages with a flood of API calls
// straight into a rate limit. The engine coalesces: an edit goes out at most
// once per Interval, and a run whose events have not been flushed is left dirty
// until the next event or the next Flush.
//
// Three cases jump the interval, because each is a moment a human is waiting on:
// the first render of a run (so a mention is acknowledged immediately), a
// terminal status (so the final answer is never left truncated), and a park (so
// a run waiting on input says so).
//
// The engine runs no background goroutine. It has a Flush the caller schedules,
// and a clock seam instead of a timer. A connector that owns a ticker would own
// a lifecycle, a shutdown ordering and a leak; the router already has a loop,
// and tests get determinism for free.
//
// # What is deliberately not solved here
//
// The map from run to native message handle is in memory. A process restart
// forgets which message a run owned, and the next event posts a second message
// that the run then edits. Making that survive a restart needs a durable handle
// store, which is a decision about the deployment's storage rather than about
// rendering; it is left to the integrator. Nothing else about correctness
// depends on it: within a process the invariant holds absolutely.
package sink

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/connectors/internal/render"
	"github.com/urmzd/opentag/pkg/envelope"
)

// Defaults for the engine.
const (
	// DefaultInterval is the coalescing window. It is chosen against the
	// tightest of the surfaces: Slack documents roughly one request per second
	// per channel for chat.update, and a second of latency on a streaming answer
	// reads as live.
	DefaultInterval = 1 * time.Second

	// DefaultMaxRuns bounds how many (run, target) pairs the engine remembers.
	// Evicting a pair forgets its message handle, so a redelivery arriving after
	// eviction posts a second message; the bound is set high enough that this
	// requires a redelivery thousands of runs stale.
	DefaultMaxRuns = 4096
)

// Surface is the native side of run-scoped rendering: post the one message a
// run owns, then edit it.
//
// handle is whatever the surface needs to address that message again — a Slack
// message timestamp, a GitHub comment id, a Jira comment id — and is opaque to
// the engine.
type Surface interface {
	Create(ctx context.Context, target address.Address, body string) (handle string, err error)
	Update(ctx context.Context, target address.Address, handle, body string) error
}

// Renderer turns a document into the surface's markup. It must be pure: the
// engine calls it whenever it needs bytes and compares the result with what it
// last wrote, so a renderer that varies on anything but its input (a timestamp
// of its own, say) would defeat the "skip the call when nothing changed" path.
type Renderer func(render.View) string

// Engine keeps one message per (run, target) and edits it as events arrive. It
// is safe for concurrent use, and deliveries to different runs do not block each
// other while their surfaces are being written.
type Engine struct {
	surface  Surface
	render   Renderer
	interval time.Duration
	maxRuns  int
	now      func() time.Time

	mu    sync.Mutex
	byKey map[string]*list.Element
	order *list.List // front is most recently used
}

// Option configures an Engine.
type Option func(*Engine)

// WithInterval sets the coalescing window. A non-positive interval flushes on
// every event, which is what a test wants and no production surface does.
func WithInterval(d time.Duration) Option {
	return func(e *Engine) { e.interval = d }
}

// WithMaxRuns bounds how many (run, target) pairs are remembered.
func WithMaxRuns(n int) Option {
	return func(e *Engine) {
		if n > 0 {
			e.maxRuns = n
		}
	}
}

// WithClock replaces the clock. It is the seam that lets a test exercise
// coalescing without sleeping.
func WithClock(now func() time.Time) Option {
	return func(e *Engine) {
		if now != nil {
			e.now = now
		}
	}
}

// New returns an engine that draws with r onto s.
func New(s Surface, r Renderer, opts ...Option) *Engine {
	e := &Engine{
		surface:  s,
		render:   r,
		interval: DefaultInterval,
		maxRuns:  DefaultMaxRuns,
		now:      time.Now,
		byKey:    make(map[string]*list.Element),
		order:    list.New(),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// state is one run's rendering on one target.
type state struct {
	key    string
	target address.Address

	// mu serializes rendering and writing for this one message, so two
	// concurrent deliveries cannot both decide to create it. It is separate
	// from the engine lock so that a slow surface write blocks one run rather
	// than every run.
	mu     sync.Mutex
	doc    *render.Doc
	handle string
	// written is the body last accepted by the surface. Comparing against it is
	// what makes a redelivery free.
	written   string
	dirty     bool
	lastFlush time.Time
}

// Deliver applies e and writes the run's message if it is due.
func (eng *Engine) Deliver(ctx context.Context, target address.Address, e envelope.Event) error {
	if eng.render == nil || eng.surface == nil {
		return fmt.Errorf("sink: engine is not configured with a surface and a renderer")
	}
	st := eng.state(key(e.RunID, target), target)

	st.mu.Lock()
	defer st.mu.Unlock()

	changed := st.doc.Apply(e)
	if !changed && !st.dirty {
		// The event is a redelivery, or a kind this surface does not render,
		// and nothing is owed to the surface. This is the cheapest possible
		// path and the one a resubscribing consumer takes most often.
		return nil
	}
	st.dirty = true

	view := st.doc.View()
	if !eng.due(st, view) {
		return nil
	}
	return eng.write(ctx, st, view)
}

// Flush writes every run whose events have not reached its surface yet. A
// caller with its own ticker calls this to bound how long a dirty run can sit;
// without it, a run that stops emitting is written no later than its terminal
// event, which is the case that matters.
func (eng *Engine) Flush(ctx context.Context) error {
	eng.mu.Lock()
	states := make([]*state, 0, len(eng.byKey))
	for _, el := range eng.byKey {
		states = append(states, el.Value.(*state))
	}
	eng.mu.Unlock()

	var errs []error
	for _, st := range states {
		st.mu.Lock()
		if st.dirty {
			if err := eng.write(ctx, st, st.doc.View()); err != nil {
				errs = append(errs, err)
			}
		}
		st.mu.Unlock()
	}
	return errors.Join(errs...)
}

// due decides whether this event's render goes out now or waits for the next
// one. See the package doc for the three cases that jump the interval.
func (eng *Engine) due(st *state, v render.View) bool {
	switch {
	case st.handle == "":
		return true
	case v.Terminal(), v.Status == render.StatusParked:
		return true
	case eng.interval <= 0:
		return true
	default:
		return !eng.now().Before(st.lastFlush.Add(eng.interval))
	}
}

// write renders the document and puts it on the surface.
//
// On failure the state stays dirty and written stays as it was, so the next
// delivery or Flush retries. That is the only error policy that is compatible
// with at-least-once: a dropped edit would leave the surface showing a prefix of
// the answer forever.
func (eng *Engine) write(ctx context.Context, st *state, v render.View) error {
	body := eng.render(v)
	if body == "" {
		return nil
	}
	if st.handle != "" && body == st.written {
		st.dirty = false
		return nil
	}

	if st.handle == "" {
		handle, err := eng.surface.Create(ctx, st.target, body)
		if err != nil {
			return fmt.Errorf("sink: create message for run %s on %s: %w", v.RunID, st.target, err)
		}
		st.handle = handle
	} else if err := eng.surface.Update(ctx, st.target, st.handle, body); err != nil {
		return fmt.Errorf("sink: update message %s for run %s on %s: %w", st.handle, v.RunID, st.target, err)
	}

	st.written = body
	st.dirty = false
	st.lastFlush = eng.now()
	return nil
}

// state returns the run's rendering state, creating it if this is the first
// event, and evicting the least recently used one when the bound is reached.
func (eng *Engine) state(k string, target address.Address) *state {
	eng.mu.Lock()
	defer eng.mu.Unlock()

	if el, ok := eng.byKey[k]; ok {
		eng.order.MoveToFront(el)
		return el.Value.(*state)
	}
	st := &state{key: k, target: target, doc: render.New()}
	eng.byKey[k] = eng.order.PushFront(st)
	for eng.order.Len() > eng.maxRuns {
		oldest := eng.order.Back()
		eng.order.Remove(oldest)
		delete(eng.byKey, oldest.Value.(*state).key)
	}
	return st
}

// Handle returns the native message identifier a run owns on target, and
// whether the engine has one. It exists for tests and for operators asking
// "which message is this run writing to"; the delivery path does not use it.
func (eng *Engine) Handle(runID string, target address.Address) (string, bool) {
	eng.mu.Lock()
	el, ok := eng.byKey[key(runID, target)]
	eng.mu.Unlock()
	if !ok {
		return "", false
	}
	st := el.Value.(*state)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.handle, st.handle != ""
}

// key identifies one message. Both halves are needed: a run delivering to two
// Slack threads owns one message in each, and two runs in the same thread own
// one each.
func key(runID string, target address.Address) string {
	var b strings.Builder
	b.Grow(len(runID) + 1 + 48)
	b.WriteString(runID)
	b.WriteByte(0)
	b.WriteString(target.String())
	return b.String()
}
