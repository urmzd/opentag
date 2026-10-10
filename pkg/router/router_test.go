package router_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/router"
	"github.com/urmzd/mandatum/pkg/topic"
)

// Targets used throughout. Each one belongs to a different connector, so a
// route's target names both the surface and the sink that renders it.
const (
	slackTarget  = "slack://T01/C02?thread=1699123456.001"
	githubTarget = "github://acme/mandatum/issues/7"
	hookTarget   = "webhook://acme/deploys"
	cronTarget   = "cron://acme/nightly"
)

const (
	// waitLong bounds something that must happen. Generous for -race on a
	// loaded machine.
	waitLong = 10 * time.Second
	// waitIdle bounds a probe that must find nothing: long enough that a
	// delivery which was merely late would still show up and fail the test.
	waitIdle = 250 * time.Millisecond
)

func TestEventReachesOnlyTheRoutesThatWantItsKind(t *testing.T) {
	slack, github, hook := newSink("slack"), newSink("github"), newSink("webhook")
	h := start(t, registry(t, slack, github, hook))

	if err := h.Register("run_1", []envelope.Route{
		{Target: address.MustParse(slackTarget)},                                        // every kind
		{Target: address.MustParse(githubTarget), Kinds: []string{"delta"}},             // a family
		{Target: address.MustParse(hookTarget), Kinds: []string{"lifecycle.completed"}}, // one kind
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	published := []envelope.Kind{envelope.KindText, envelope.KindThinking, envelope.KindCompleted}
	for i, kind := range published {
		h.publish(t, event("docs-bot", "run_1", uint64(i+1), kind))
	}

	want := []struct {
		sink  *sink
		kinds []envelope.Kind
	}{
		{slack, published},
		{github, []envelope.Kind{envelope.KindText, envelope.KindThinking}},
		{hook, []envelope.Kind{envelope.KindCompleted}},
	}
	for _, w := range want {
		w.sink.waitFor(t, len(w.kinds))
	}
	// Everything wanted has arrived; give anything unwanted a chance to show up
	// before claiming the subset was exact.
	time.Sleep(waitIdle)
	for _, w := range want {
		if got := kindsOf(w.sink.events()); !equalKinds(got, w.kinds) {
			t.Errorf("%s received %v, want %v", w.sink.Name(), got, w.kinds)
		}
	}
}

func TestTwoRoutesToOneTargetDeliverOnce(t *testing.T) {
	slack := newSink("slack")
	h := start(t, registry(t, slack))

	// Both routes name the same surface, and both select the first event.
	if err := h.Register("run_1", []envelope.Route{
		{Target: address.MustParse(slackTarget), Kinds: []string{"delta"}},
		{Target: address.MustParse(slackTarget), Kinds: []string{"delta.text", "lifecycle"}},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	h.publish(t, event("docs-bot", "run_1", 2, envelope.KindCompleted))

	slack.waitFor(t, 2)
	time.Sleep(waitIdle)
	if got := slack.tries(); got != 2 {
		t.Errorf("sink was called %d times, want 2: one surface is one delivery", got)
	}
}

func TestOrderIsPreservedPerRunAndTargetWhenTheSinkIsSlow(t *testing.T) {
	const events = 25

	slack := newSink("slack")
	// Later events render faster than earlier ones, so anything that delivered
	// a lane concurrently would reorder rather than merely interleave.
	slack.hook = func(ctx context.Context, _ int, e envelope.Event) error {
		time.Sleep(time.Duration(events+1-int(e.Seq)) * time.Millisecond)
		return nil
	}
	h := start(t, registry(t, slack))

	if err := h.Register("run_1", []envelope.Route{{Target: address.MustParse(slackTarget)}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for seq := uint64(1); seq <= events; seq++ {
		h.publish(t, event("docs-bot", "run_1", seq, envelope.KindText))
	}

	got := seqsOf(slack.waitFor(t, events))
	for i, seq := range got {
		if seq != uint64(i+1) {
			t.Fatalf("sequences arrived as %v, want 1..%d in order", got, events)
		}
	}
}

func TestASlowSinkDoesNotDelayAnother(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once

	slow, fast := newSink("slack"), newSink("webhook")
	slow.hook = func(ctx context.Context, _ int, _ envelope.Event) error {
		once.Do(func() { close(entered) })
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h := start(t, registry(t, slow, fast))

	if err := h.Register("run_1", []envelope.Route{
		{Target: address.MustParse(slackTarget)},
		{Target: address.MustParse(hookTarget)},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	h.publish(t, event("docs-bot", "run_1", 2, envelope.KindCompleted))

	// The barrier, not a sleep: the fast sink must finish both events while the
	// slow one is provably still inside its first.
	select {
	case <-entered:
	case <-time.After(waitLong):
		t.Fatal("the slow sink was never called")
	}
	fast.waitFor(t, 2)
	if got := slow.count(); got != 0 {
		t.Errorf("the slow sink completed %d deliveries while still blocked", got)
	}

	close(release)
	if got := seqsOf(slow.waitFor(t, 2)); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Errorf("the slow sink received %v, want [1 2]", got)
	}
}

func TestTransientFailuresAreRetriedAndPermanentOnesAreNot(t *testing.T) {
	transient := errors.New("service unavailable")

	cases := []struct {
		name      string
		fail      func(attempt int) error
		wantTries int
		wantGot   int
		wantLog   string
	}{
		{
			name:      "a transient failure is retried until it succeeds",
			fail:      func(attempt int) error { return failUntil(attempt, 3, transient) },
			wantTries: 3,
			wantGot:   1,
			wantLog:   "delivery succeeded after retrying",
		},
		{
			name:      "a transient failure that never clears exhausts its attempts",
			fail:      func(int) error { return transient },
			wantTries: 3,
			wantGot:   0,
			wantLog:   "attempts exhausted",
		},
		{
			name:      "an undeliverable target is not retried",
			fail:      func(int) error { return fmt.Errorf("no such thread: %w", connector.ErrUndeliverable) },
			wantTries: 1,
			wantGot:   0,
			wantLog:   "giving up",
		},
		{
			name:      "an unsupported surface is not retried",
			fail:      func(int) error { return fmt.Errorf("no surface: %w", connector.ErrUnsupported) },
			wantTries: 1,
			wantGot:   0,
			wantLog:   "giving up",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			slack := newSink("slack")
			slack.hook = func(_ context.Context, attempt int, _ envelope.Event) error { return tc.fail(attempt) }
			h := start(t, registry(t, slack), router.WithAttempts(3))

			if err := h.Register("run_1", []envelope.Route{{Target: address.MustParse(slackTarget)}}); err != nil {
				t.Fatalf("Register: %v", err)
			}
			h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))

			slack.waitTries(t, tc.wantTries)
			h.logs.waitFor(t, tc.wantLog)
			// The attempt count must be exact, not "at least": a permanent
			// failure that gets retried once more is the bug this protects.
			time.Sleep(waitIdle)
			if got := slack.tries(); got != tc.wantTries {
				t.Errorf("sink was called %d times, want %d", got, tc.wantTries)
			}
			if got := slack.count(); got != tc.wantGot {
				t.Errorf("sink delivered %d events, want %d", got, tc.wantGot)
			}
		})
	}
}

func TestAPanickingSinkIsContainedAndTheRestStillDeliver(t *testing.T) {
	boom, slack := newSink("webhook"), newSink("slack")
	boom.hook = func(context.Context, int, envelope.Event) error { panic("sink is broken") }
	h := start(t, registry(t, boom, slack))

	if err := h.Register("run_1", []envelope.Route{
		{Target: address.MustParse(hookTarget)},
		{Target: address.MustParse(slackTarget)},
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// The panic must not take the event's other delivery with it, and must not
	// take the router down: a later event still gets through.
	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	slack.waitFor(t, 1)
	h.publish(t, event("docs-bot", "run_1", 2, envelope.KindCompleted))
	slack.waitFor(t, 2)

	h.logs.waitFor(t, "delivery failed, giving up", "panicked", "stack=")
	time.Sleep(waitIdle)
	// Once per event: a panic is a defect, not congestion, so it is not retried.
	if got := boom.tries(); got != 2 {
		t.Errorf("the panicking sink was called %d times, want 2 (one per event, no retries)", got)
	}
}

func TestQueueOverflowIsDroppedLoudlyAndNeverBlocksAnotherRun(t *testing.T) {
	release := make(chan struct{})
	stuck, fast := newSink("slack"), newSink("webhook")
	stuck.hook = func(ctx context.Context, _ int, _ envelope.Event) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	h := start(t, registry(t, stuck, fast), router.WithBacklog(1))
	defer close(release)

	for _, spec := range []struct {
		runID  string
		target string
	}{{"run_stuck", slackTarget}, {"run_fast", hookTarget}} {
		if err := h.Register(spec.runID, []envelope.Route{{Target: address.MustParse(spec.target)}}); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}

	// Far more than the unresponsive run's lane can hold, so it overflows, and
	// the dispatch loop must not stall on the drops.
	for seq := uint64(1); seq <= 10; seq++ {
		h.publish(t, event("docs-bot", "run_stuck", seq, envelope.KindText))
	}
	h.logs.waitFor(t, "delivery dropped, sink is not keeping up", "run_stuck")

	// The healthy run keeps being delivered, one event at a time so that the
	// deliberately tiny queue is never what limits it.
	for seq := uint64(1); seq <= 5; seq++ {
		h.publish(t, event("docs-bot", "run_fast", seq, envelope.KindText))
		fast.waitFor(t, int(seq))
	}
	if h.logs.contains("run=run_fast") {
		t.Error("the healthy run lost an event to the unresponsive one")
	}
}

func TestEventForAnUnroutedRunIsDroppedAndLogged(t *testing.T) {
	slack := newSink("slack")
	h := start(t, registry(t, slack))

	// No Register: the run belongs to some other node, or to nobody.
	h.publish(t, event("docs-bot", "run_elsewhere", 1, envelope.KindText))

	h.logs.waitFor(t, "no routes for", "run_elsewhere")
	if got := slack.tries(); got != 0 {
		t.Errorf("sink was called %d times for an unrouted run, want 0", got)
	}
}

func TestReleasedRunStopsDeliveringAndLeavesNoGoroutines(t *testing.T) {
	slack, github := newSink("slack"), newSink("github")
	h := start(t, registry(t, slack, github))

	routes := []envelope.Route{
		{Target: address.MustParse(slackTarget)},
		{Target: address.MustParse(githubTarget)},
	}
	// One run stays registered for the whole test, so the baseline includes a
	// live router with live lanes and only the released ones can move it.
	if err := h.Register("run_keep", routes[:1]); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_keep", 1, envelope.KindText))
	slack.waitFor(t, 1)
	base := runtime.NumGoroutine()

	released := []string{"run_a", "run_b", "run_c", "run_d", "run_e"}
	for _, runID := range released {
		if err := h.Register(runID, routes); err != nil {
			t.Fatalf("Register: %v", err)
		}
		h.publish(t, event("docs-bot", runID, 1, envelope.KindText))
	}
	slack.waitFor(t, 1+len(released))
	github.waitFor(t, len(released))
	if got := runtime.NumGoroutine(); got <= base {
		t.Fatalf("goroutines %d did not grow past the %d baseline: the lanes never started", got, base)
	}

	for _, runID := range released {
		h.Release(runID)
	}
	waitGoroutines(t, base)

	// A released run has nobody listening for it here any more.
	before := slack.tries()
	h.publish(t, event("docs-bot", "run_a", 2, envelope.KindCompleted))
	h.logs.waitFor(t, "no routes for", "run_a")
	if got := slack.tries(); got != before {
		t.Errorf("sink was called %d times after Release, want %d", got, before)
	}
}

func TestCancellingRunLeavesNoGoroutines(t *testing.T) {
	base := runtime.NumGoroutine()

	slack := newSink("slack")
	h := start(t, registry(t, slack))
	if err := h.Register("run_1", []envelope.Route{{Target: address.MustParse(slackTarget)}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	slack.waitFor(t, 1)

	h.stop(t) // Run returns only once every lane it started has exited.
	waitGoroutines(t, base)
}

func TestARouteNothingCanReceiveIsDroppedAtRegistration(t *testing.T) {
	slack := newSink("slack")
	// cron raises tags and has no surface to render into, so a route to it can
	// never be delivered and is not worth discovering per event.
	h := start(t, registry(t, slack, schedule{"cron"}))

	if err := h.Register("run_1", []envelope.Route{
		{Target: address.MustParse(cronTarget)},
		{Target: address.MustParse(slackTarget)},
		{Target: address.MustParse("jira://acme/PROJ-5")}, // no such connector here
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.logs.waitFor(t, "route has no sink", cronTarget, "jira://acme/PROJ-5")

	// The routes that can be served still are.
	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	slack.waitFor(t, 1)
}

func TestRegisterRejectsWhatItCouldNeverServe(t *testing.T) {
	cases := []struct {
		name   string
		runID  string
		routes []envelope.Route
	}{
		{name: "no run id", runID: "", routes: []envelope.Route{{Target: address.MustParse(slackTarget)}}},
		{name: "a route with no target", runID: "run_1", routes: []envelope.Route{{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := start(t, registry(t, newSink("slack")))
			err := h.Register(tc.runID, tc.routes)
			if !errors.Is(err, router.ErrInvalid) {
				t.Fatalf("Register(%q) = %v, want router.ErrInvalid", tc.runID, err)
			}
		})
	}
}

func TestReRegisteringKeepsTheCursorAndRetiresRoutesThatWentAway(t *testing.T) {
	slack, github := newSink("slack"), newSink("github")
	h := start(t, registry(t, slack, github))

	slackRoute := envelope.Route{Target: address.MustParse(slackTarget)}
	githubRoute := envelope.Route{Target: address.MustParse(githubTarget)}
	if err := h.Register("run_1", []envelope.Route{slackRoute, githubRoute}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	slack.waitFor(t, 1)
	github.waitFor(t, 1)

	// Replay re-registers, this time without the GitHub route.
	if err := h.Register("run_1", []envelope.Route{slackRoute}); err != nil {
		t.Fatalf("re-Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_1", 2, envelope.KindText))
	slack.waitFor(t, 2)
	time.Sleep(waitIdle)
	if got := github.count(); got != 1 {
		t.Errorf("the retired route received %d events, want 1", got)
	}
}

func TestFallingBehindTheBusResubscribesAndDoesNotRedeliver(t *testing.T) {
	// The first stream ends the way a slow consumer is told to reconnect; the
	// second replays from the start of what the bus still retains, which is
	// exactly the duplication a per-run cursor has to absorb.
	first := []envelope.Event{
		event("docs-bot", "run_1", 1, envelope.KindText),
		event("docs-bot", "run_1", 2, envelope.KindText),
	}
	second := []envelope.Event{
		event("docs-bot", "run_1", 1, envelope.KindText),
		event("docs-bot", "run_1", 2, envelope.KindText),
		event("docs-bot", "run_1", 3, envelope.KindText),
		event("docs-bot", "run_1", 4, envelope.KindCompleted),
	}
	gate := make(chan struct{})
	b := &scriptedBus{
		gate: gate,
		scripts: []script{
			{events: first, err: fmt.Errorf("behind: %w", bus.ErrSlowConsumer)},
			{events: second},
		},
	}

	slack := newSink("slack")
	h := startOn(t, b, registry(t, slack))
	if err := h.Register("run_1", []envelope.Route{{Target: address.MustParse(slackTarget)}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	close(gate) // the run is routed: let the streams run

	got := seqsOf(slack.waitFor(t, 4))
	time.Sleep(waitIdle)
	if want := []uint64{1, 2, 3, 4}; !equalSeqs(seqsOf(slack.events()), want) {
		t.Errorf("sink received %v, want %v exactly once each", got, want)
	}
	if n := b.subscribes(); n != 2 {
		t.Errorf("the router subscribed %d times, want 2 (one resubscribe)", n)
	}
	h.logs.waitFor(t, "fell behind the bus, resubscribing")
}

func TestRunRestartsWithoutReusingRetiredLanes(t *testing.T) {
	// A run stays registered across a restart, so the second Run finds run
	// state whose lanes belong to the first one. It must build its own: the
	// cursor survives a restart, a queue does not.
	slack := newSink("slack")
	b := bus.NewMemory()
	r := router.New(b, registry(t, slack), router.WithLogger(newLogs().logger()))
	if err := r.Register("run_1", []envelope.Route{{Target: address.MustParse(slackTarget)}}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	for round := 1; round <= 2; round++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- r.Run(ctx) }()

		if err := b.Publish(context.Background(), event("docs-bot", "run_1", uint64(round), envelope.KindText)); err != nil {
			t.Fatalf("round %d: publish: %v", round, err)
		}
		// The catch-up window replays the earlier rounds; the cursor the run
		// kept is what stops them being delivered twice.
		if got := seqsOf(slack.waitFor(t, round)); len(got) != round {
			t.Fatalf("round %d: sink received %v, want %d events", round, got, round)
		}

		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("round %d: Run returned %v, want nil", round, err)
			}
		case <-time.After(waitLong):
			t.Fatalf("round %d: Run did not return after cancellation", round)
		}
	}
}

func TestASecondRunIsRefused(t *testing.T) {
	slack := newSink("slack")
	h := start(t, registry(t, slack))

	// A delivery is the proof that the first Run is up: asking before it has
	// claimed the router would only test the test's own scheduling.
	if err := h.Register("run_1", []envelope.Route{{Target: address.MustParse(slackTarget)}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	slack.waitFor(t, 1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := h.Run(ctx); !errors.Is(err, router.ErrRunning) {
		t.Fatalf("second Run = %v, want router.ErrRunning", err)
	}
}

// --- harness ---------------------------------------------------------------

// harness is a running router with the bus it reads and the log it wrote, so a
// test can assert on delivery and on what was said about the deliveries that
// failed.
type harness struct {
	*router.Router
	bus    bus.Bus
	logs   *logs
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

// start brings up a router over a memory bus and stops it when the test ends.
func start(t *testing.T, sinks router.Resolver, opts ...router.Option) *harness {
	t.Helper()
	return startOn(t, bus.NewMemory(), sinks, opts...)
}

// startOn is start over a given bus. Backoff is compressed so a retry property
// does not cost a second of wall clock, and the logger captures everything
// including debug.
func startOn(t *testing.T, b bus.Bus, sinks router.Resolver, opts ...router.Option) *harness {
	t.Helper()
	h := &harness{
		bus:  b,
		logs: newLogs(),
		done: make(chan error, 1),
	}
	settings := append([]router.Option{
		router.WithLogger(h.logs.logger()),
		router.WithBackoff(time.Millisecond, 2*time.Millisecond),
	}, opts...)
	h.Router = router.New(b, sinks, settings...)

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- h.Run(ctx) }()
	t.Cleanup(func() { h.stop(t) })
	return h
}

// stop cancels the router and waits for Run, which is the join point for every
// goroutine the router started.
func (h *harness) stop(t *testing.T) {
	t.Helper()
	h.once.Do(func() {
		h.cancel()
		select {
		case err := <-h.done:
			if err != nil {
				t.Errorf("Run returned %v, want nil on cancellation", err)
			}
		case <-time.After(waitLong):
			t.Error("Run did not return after cancellation")
		}
	})
}

func (h *harness) publish(t *testing.T, e envelope.Event) {
	t.Helper()
	if err := h.bus.Publish(context.Background(), e); err != nil {
		t.Fatalf("publish %s seq %d: %v", e.Kind, e.Seq, err)
	}
}

func event(agent, runID string, seq uint64, kind envelope.Kind) envelope.Event {
	tp, err := topic.Run(agent, runID)
	if err != nil {
		panic(err)
	}
	return envelope.Event{
		Seq:     seq,
		Topic:   tp,
		RunID:   runID,
		Tenant:  "acme",
		Agent:   agent,
		Rev:     3,
		Origin:  "github",
		Kind:    kind,
		Payload: []byte(fmt.Sprintf("%s/%d", runID, seq)),
		At:      time.Now().UTC(),
	}
}

func failUntil(attempt, ok int, err error) error {
	if attempt < ok {
		return err
	}
	return nil
}

// --- fakes -----------------------------------------------------------------

// sink is a connector that records what it rendered and lets a test decide how
// each attempt goes.
type sink struct {
	name string
	// hook runs before the event is recorded, so an event a hook fails on is
	// counted as an attempt and never as a delivery.
	hook func(ctx context.Context, attempt int, e envelope.Event) error

	mu       sync.Mutex
	attempts int
	got      []envelope.Event
}

func newSink(name string) *sink { return &sink{name: name} }

func (s *sink) Name() string { return s.name }

func (s *sink) Deliver(ctx context.Context, target address.Address, e envelope.Event) error {
	s.mu.Lock()
	s.attempts++
	attempt := s.attempts
	s.mu.Unlock()

	if s.hook != nil {
		if err := s.hook(ctx, attempt, e); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, e)
	return nil
}

func (s *sink) events() []envelope.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]envelope.Event(nil), s.got...)
}

func (s *sink) count() int { return len(s.events()) }

func (s *sink) tries() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// waitFor blocks until n events have been delivered and returns them.
func (s *sink) waitFor(t *testing.T, n int) []envelope.Event {
	t.Helper()
	deadline := time.Now().Add(waitLong)
	for {
		if got := s.events(); len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s received %d events, want %d", s.name, s.count(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitTries blocks until the sink has been called n times.
func (s *sink) waitTries(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(waitLong)
	for s.tries() < n {
		if time.Now().After(deadline) {
			t.Fatalf("%s was called %d times, want %d", s.name, s.tries(), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// schedule is a trigger-only connector: it can raise tags and has no surface to
// deliver to, which is the case the router must not retry.
type schedule struct{ name string }

func (s schedule) Name() string { return s.name }

func (s schedule) Ingest(ctx context.Context, _ chan<- envelope.Tag) error {
	<-ctx.Done()
	return ctx.Err()
}

func registry(t *testing.T, cs ...connector.Connector) *connector.Registry {
	t.Helper()
	reg := connector.NewRegistry()
	for _, c := range cs {
		if err := reg.Register(c); err != nil {
			t.Fatalf("register connector %s: %v", c.Name(), err)
		}
	}
	return reg
}

// script is one subscription's worth of behaviour: events to hand over, then an
// error to end with. A nil error means the stream blocks until it is closed.
type script struct {
	events []envelope.Event
	err    error
}

// scriptedBus hands out prepared streams, so a test can make the router fall
// behind and see events again without depending on a real backend's retention.
// Its streams hold every event until gate is closed, so a test can register a
// run's routes before the first event arrives.
type scriptedBus struct {
	scripts []script
	gate    <-chan struct{}

	mu    sync.Mutex
	calls int
}

func (b *scriptedBus) Publish(context.Context, envelope.Event) error { return nil }

func (b *scriptedBus) Subscribe(_ context.Context, _ envelope.Subscription) (bus.Stream, error) {
	b.mu.Lock()
	i := b.calls
	b.calls++
	b.mu.Unlock()

	s := &scriptedStream{gate: b.gate, closed: make(chan struct{})}
	if i < len(b.scripts) {
		s.script = b.scripts[i]
	}
	return s, nil
}

func (b *scriptedBus) subscribes() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

type scriptedStream struct {
	script script
	gate   <-chan struct{}
	once   sync.Once
	closed chan struct{}
}

func (s *scriptedStream) Recv(ctx context.Context) (envelope.Event, error) {
	if s.gate != nil {
		select {
		case <-s.gate:
		case <-ctx.Done():
			return envelope.Event{}, bus.ErrClosed
		}
	}
	if len(s.script.events) > 0 {
		e := s.script.events[0]
		s.script.events = s.script.events[1:]
		return e, nil
	}
	if s.script.err != nil {
		err := s.script.err
		s.script.err = nil
		return envelope.Event{}, err
	}
	select {
	case <-s.closed:
	case <-ctx.Done():
	}
	return envelope.Event{}, bus.ErrClosed
}

func (s *scriptedStream) Close() error {
	s.once.Do(func() { close(s.closed) })
	return nil
}

// --- log capture -----------------------------------------------------------

// logs collects what the router said. Lanes log concurrently, so writes are
// serialised.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func newLogs() *logs { return &logs{} }

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func (l *logs) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func (l *logs) contains(want string) bool { return strings.Contains(l.text(), want) }

// waitFor blocks until every fragment has been logged. A missing fragment is a
// failure in itself: a delivery this package gave up on must never be silent.
func (l *logs) waitFor(t *testing.T, fragments ...string) {
	t.Helper()
	deadline := time.Now().Add(waitLong)
	for {
		missing := ""
		for _, want := range fragments {
			if !l.contains(want) {
				missing = want
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("log never mentioned %q; log was:\n%s", missing, l.text())
		}
		time.Sleep(time.Millisecond)
	}
}

// --- assertions ------------------------------------------------------------

// waitGoroutines waits for the goroutine count to fall back to want, which is
// how a leak shows up: the count that grew when lanes started never comes down.
func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(waitLong)
	for {
		got := runtime.NumGoroutine()
		if got <= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines still running, want at most %d\n%s", got, want, stacks())
		}
		time.Sleep(time.Millisecond)
	}
}

func stacks() string {
	buf := make([]byte, 1<<16)
	return string(buf[:runtime.Stack(buf, true)])
}

func kindsOf(events []envelope.Event) []envelope.Kind {
	out := make([]envelope.Kind, 0, len(events))
	for _, e := range events {
		out = append(out, e.Kind)
	}
	return out
}

func seqsOf(events []envelope.Event) []uint64 {
	out := make([]uint64, 0, len(events))
	for _, e := range events {
		out = append(out, e.Seq)
	}
	return out
}

func equalKinds(got, want []envelope.Kind) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func equalSeqs(got, want []uint64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
