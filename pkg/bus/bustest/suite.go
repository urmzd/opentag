// Package bustest is the conformance suite every bus backend must pass. Run
// exercises the whole contract — sequence assignment, idempotent replay, prefix
// routing, filter narrowing, the catch-up/live seam under concurrent publish,
// slow-consumer isolation, resource release, and retention — so that a backend
// is held to the same promises whichever process it runs in.
//
// Two properties are deliberately not asserted, because the bus does not
// promise them: ordering between different runs, and which events survive
// retention beyond "the newest ones". Tests that depended on either would
// encode a guarantee consumers must not rely on.
//
// Timings are event-driven where possible: subtests wait on channels with
// generous deadlines rather than sleeping fixed amounts, so the suite stays
// reliable under -race on a loaded machine.
package bustest

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

const (
	// waitLong bounds things that must happen: an event that was published
	// must arrive. Generous for loaded -race machines and for a backend that
	// polls.
	waitLong = 10 * time.Second
	// waitIdle bounds a probe that must find nothing. It has to outlast a
	// backend's poll interval, or an event that is merely late would look
	// like an event that was correctly filtered out.
	waitIdle = 500 * time.Millisecond
	// waitPublish bounds one Publish call. It is the slow-consumer property:
	// a publisher is never made to wait for a consumer.
	waitPublish = 2 * time.Second
)

// Config is the configuration a factory must apply to the bus it returns. Both
// fields are part of the contract, and several subtests are only meaningful if
// they are honoured exactly.
type Config struct {
	// Retention is how many events each run keeps.
	Retention int
	// Backlog is the per-subscriber buffer depth.
	Backlog int
}

// Factory returns a fresh, empty bus configured per cfg. It must isolate each
// call: no bus may see another's events, or the suite's subtests will see each
// other's.
type Factory func(t *testing.T, cfg Config) bus.Bus

// Run exercises a Bus implementation against the full contract.
func Run(t *testing.T, newBus Factory) {
	t.Run("EveryConsumerReceivesEveryEventInSequenceOrder", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})

		// One subscriber at each level of the namespace: all three cover the
		// run the events are published to.
		streams := map[string]bus.Stream{}
		for _, at := range []string{"agent", "agent:docs-bot", "agent:docs-bot:run_1"} {
			streams[at] = subscribe(t, b, on(at))
		}

		payload := []byte("hello")
		first := text("docs-bot", "run_1", 1)
		first.Payload = payload
		publish(t, b, first)
		publish(t, b, text("docs-bot", "run_1", 2), text("docs-bot", "run_1", 3))
		// The bus owns its copy: mutating the caller's slice afterwards must
		// not reach a consumer.
		payload[0] = 'X'

		for at, s := range streams {
			got := recvN(t, s, 3)
			if diff := seqsOf(got); !slices.Equal(diff, []uint64{1, 2, 3}) {
				t.Errorf("subscriber on %s got sequences %v, want [1 2 3]", at, diff)
			}
			if string(got[0].Payload) != "hello" {
				t.Errorf("subscriber on %s got payload %q, want %q", at, got[0].Payload, "hello")
			}
			if got[0].At.IsZero() {
				t.Errorf("subscriber on %s got an event with no timestamp", at)
			}
		}
	})

	t.Run("SequenceIsAssignedFromOneWithinEachRun", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})

		// Seq 0 means "assign": each run numbers from 1 independently.
		for range 3 {
			publish(t, b, text("docs-bot", "run_1", 0))
		}
		for range 2 {
			publish(t, b, text("docs-bot", "run_2", 0))
		}

		s := subscribe(t, b, on("agent:docs-bot"))
		byRun := map[string][]uint64{}
		for _, e := range recvN(t, s, 5) {
			byRun[e.RunID] = append(byRun[e.RunID], e.Seq)
		}
		if want := []uint64{1, 2, 3}; !slices.Equal(byRun["run_1"], want) {
			t.Errorf("run_1 sequences = %v, want %v", byRun["run_1"], want)
		}
		if want := []uint64{1, 2}; !slices.Equal(byRun["run_2"], want) {
			t.Errorf("run_2 sequences = %v, want %v", byRun["run_2"], want)
		}
	})

	t.Run("PrefixSubscriptionCoversEveryRunBeneathItAndNothingElse", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})
		s := subscribe(t, b, on("agent:docs-bot"))

		publish(t, b,
			text("docs-bot", "run_1", 1),
			text("docs-bot", "run_2", 1),
			text("triage-bot", "run_3", 1),
		)

		var runs []string
		for _, e := range recvN(t, s, 2) {
			runs = append(runs, e.RunID)
		}
		slices.Sort(runs)
		if want := []string{"run_1", "run_2"}; !slices.Equal(runs, want) {
			t.Errorf("received runs %v, want %v", runs, want)
		}
		// A sibling agent's run is not beneath this topic, however similar
		// its name.
		recvIdle(t, s)
	})

	t.Run("FilterNarrowsWithoutChangingTopic", func(t *testing.T) {
		// The corpus is published first, so every case reads it out of
		// retention: the filter is the only variable.
		corpus := []envelope.Event{
			revised(text("docs-bot", "run_a", 1), 1, "slack"),
			revised(ev("docs-bot", "run_a", 2, envelope.KindCitation), 1, "slack"),
			revised(ev("docs-bot", "run_a", 3, envelope.KindCompleted), 1, "slack"),
			revised(text("docs-bot", "run_b", 1), 2, "github"),
			revised(ev("docs-bot", "run_b", 2, envelope.KindCitation), 2, "github"),
		}

		cases := []struct {
			name string
			sub  envelope.Subscription
			want []string
		}{
			{"everything", on("agent:docs-bot"), []string{"run_a#1", "run_a#2", "run_a#3", "run_b#1", "run_b#2"}},
			{"kind family", filtered("agent:docs-bot", envelope.Filter{Kinds: []string{"delta"}}), []string{"run_a#1", "run_a#2", "run_b#1", "run_b#2"}},
			{"exact kind", filtered("agent:docs-bot", envelope.Filter{Kinds: []string{"delta.citation"}}), []string{"run_a#2", "run_b#2"}},
			{"two kinds", filtered("agent:docs-bot", envelope.Filter{Kinds: []string{"delta.text", "lifecycle.completed"}}), []string{"run_a#1", "run_a#3", "run_b#1"}},
			{"revision", filtered("agent:docs-bot", envelope.Filter{Rev: 2}), []string{"run_b#1", "run_b#2"}},
			{"origin", filtered("agent:docs-bot", envelope.Filter{Origin: "slack"}), []string{"run_a#1", "run_a#2", "run_a#3"}},
			{"run", filtered("agent:docs-bot", envelope.Filter{RunID: "run_b"}), []string{"run_b#1", "run_b#2"}},
			{"run and kind", filtered("agent:docs-bot", envelope.Filter{RunID: "run_a", Kinds: []string{"lifecycle"}}), []string{"run_a#3"}},
		}

		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				b := newBus(t, Config{Retention: 64, Backlog: 64})
				publish(t, b, corpus...)

				s := subscribe(t, b, tc.sub)
				got := labelsOf(recvN(t, s, len(tc.want)))
				slices.Sort(got)
				if !slices.Equal(got, tc.want) {
					t.Errorf("received %v, want %v", got, tc.want)
				}
				recvIdle(t, s)
			})
		}
	})

	t.Run("FromResumesAfterASequenceWithinEachRun", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})
		publish(t, b,
			text("docs-bot", "run_a", 1),
			text("docs-bot", "run_a", 2),
			text("docs-bot", "run_b", 1),
			text("docs-bot", "run_b", 2),
		)

		s := subscribe(t, b, envelope.Subscription{Topic: topic.MustParse("agent:docs-bot"), From: 1})
		got := labelsOf(recvN(t, s, 2))
		slices.Sort(got)
		if want := []string{"run_a#2", "run_b#2"}; !slices.Equal(got, want) {
			t.Errorf("received %v, want %v: From resumes within each run", got, want)
		}
		recvIdle(t, s)
	})

	t.Run("CatchUpJoinsTheLiveTailWithoutGapOrDuplicate", func(t *testing.T) {
		// The seam between retained history and the live tail is the classic
		// place to lose or double an event, so it is exercised with
		// publishing in flight and with subscribers that join at different
		// points.
		const (
			history = 100
			last    = 300
		)
		b := newBus(t, Config{Retention: 2048, Backlog: 2048})

		early := subscribe(t, b, on("agent:docs-bot")) // before anything exists
		if err := publishRange(t.Context(), b, "docs-bot", "run_1", 1, history); err != nil {
			t.Fatal(err)
		}

		publishErr := make(chan error, 1)
		go func() {
			publishErr <- publishRange(context.WithoutCancel(t.Context()), b, "docs-bot", "run_1", history+1, last)
		}()

		// Joining mid-flight, at cursors that fall before, at, and after the
		// history that existed when publishing resumed: the events at or
		// before From must not arrive, the ones after must all arrive exactly
		// once.
		froms := []uint64{50, history, history + 40}
		mids := make([]bus.Stream, len(froms))
		for i, from := range froms {
			mids[i] = subscribe(t, b, envelope.Subscription{Topic: topic.MustParse("agent:docs-bot"), From: from})
		}

		if err := <-publishErr; err != nil {
			t.Fatal(err)
		}
		wantContiguous(t, seqsOf(recvN(t, early, last)), 1, last)
		for i, from := range froms {
			wantContiguous(t, seqsOf(recvN(t, mids[i], int(last-from))), from+1, last)
		}
	})

	t.Run("ReplayingASequenceIsNotADuplicate", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})
		publish(t, b, text("docs-bot", "run_1", 1), text("docs-bot", "run_1", 2), text("docs-bot", "run_1", 3))

		// A replaying producer re-publishes what the run already emitted.
		// The stream must not grow, and the retained event must not change.
		tampered := text("docs-bot", "run_1", 2)
		tampered.Payload = []byte("tampered")
		publish(t, b, text("docs-bot", "run_1", 1), tampered, text("docs-bot", "run_1", 3))

		// Assignment continues from the high-water mark, not from the number
		// of events accepted.
		publish(t, b, text("docs-bot", "run_1", 0))

		s := subscribe(t, b, on("agent:docs-bot:run_1"))
		got := recvN(t, s, 4)
		wantContiguous(t, seqsOf(got), 1, 4)
		if string(got[1].Payload) != "run_1#2" {
			t.Errorf("sequence 2 payload = %q, want the original %q", got[1].Payload, "run_1#2")
		}
		recvIdle(t, s)
	})

	t.Run("SlowConsumerWedgesNeitherThePublisherNorItsPeers", func(t *testing.T) {
		const total = 500
		// A backlog far smaller than the burst, and retention far larger, so
		// a consumer that is terminated for falling behind can always resume.
		b := newBus(t, Config{Retention: 4096, Backlog: 32})

		stalled := subscribe(t, b, on("agent:docs-bot")) // never read from
		healthy := newTracker(t, b, on("agent:docs-bot"))

		slowest := time.Duration(0)
		for seq := uint64(1); seq <= total; seq++ {
			started := time.Now()
			if err := b.Publish(t.Context(), text("docs-bot", "run_1", seq)); err != nil {
				t.Fatalf("Publish %d: %v", seq, err)
			}
			if took := time.Since(started); took > slowest {
				slowest = took
			}
		}
		if slowest > waitPublish {
			t.Errorf("slowest Publish took %v: a stalled consumer must not slow the publisher", slowest)
		}

		// The healthy consumer sees every event, resubscribing from its own
		// cursor if the bus terminated it for falling behind — the documented
		// recovery, and the reason termination is safe.
		wantContiguous(t, healthy.wait(t, total), 1, total)

		// The stalled consumer is allowed to be terminated, but never to be
		// handed a stream with a hole in it.
		got, err := drain(stalled, total)
		if err != nil && !errors.Is(err, bus.ErrSlowConsumer) {
			t.Fatalf("stalled consumer Recv error = %v, want nil or bus.ErrSlowConsumer", err)
		}
		if len(got) > 0 {
			wantContiguous(t, got, 1, got[len(got)-1])
		}
	})

	t.Run("RetentionKeepsTheNewestEventsPerRun", func(t *testing.T) {
		const (
			retention = 8
			published = 20
		)
		b := newBus(t, Config{Retention: retention, Backlog: 64})
		if err := publishRange(t.Context(), b, "docs-bot", "run_wraps", 1, published); err != nil {
			t.Fatal(err)
		}
		if err := publishRange(t.Context(), b, "docs-bot", "run_small", 1, 3); err != nil {
			t.Fatal(err)
		}

		// Retention is per run: wrapping one run's ring does not touch
		// another's, and what survives is the newest.
		s := subscribe(t, b, on("agent:docs-bot"))
		byRun := map[string][]uint64{}
		for _, e := range recvN(t, s, retention+3) {
			byRun[e.RunID] = append(byRun[e.RunID], e.Seq)
		}
		wantContiguous(t, byRun["run_wraps"], published-retention+1, published)
		wantContiguous(t, byRun["run_small"], 1, 3)
		recvIdle(t, s)
	})

	t.Run("ClosedStreamReportsClosed", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})
		s := subscribe(t, b, on("agent:docs-bot"))
		publish(t, b, text("docs-bot", "run_1", 1))
		recvN(t, s, 1)

		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatalf("second Close: %v, want nil: Close is idempotent", err)
		}
		if _, err := s.Recv(t.Context()); !errors.Is(err, bus.ErrClosed) {
			t.Fatalf("Recv after Close = %v, want bus.ErrClosed", err)
		}

		// A Recv already blocked when Close lands must come back too. It
		// subscribes to an agent with no history, so that it really is
		// blocked and not draining a snapshot.
		blocked := subscribe(t, b, on("agent:quiet-bot"))
		errc := make(chan error, 1)
		go func() {
			_, err := blocked.Recv(context.WithoutCancel(t.Context()))
			errc <- err
		}()
		time.Sleep(50 * time.Millisecond) // let Recv block first
		if err := blocked.Close(); err != nil {
			t.Fatalf("Close while Recv blocked: %v", err)
		}
		select {
		case err := <-errc:
			if !errors.Is(err, bus.ErrClosed) {
				t.Fatalf("blocked Recv = %v, want bus.ErrClosed", err)
			}
		case <-time.After(waitLong):
			t.Fatal("blocked Recv did not return after Close")
		}
	})

	t.Run("CancellingTheSubscriptionClosesTheStream", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})
		ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
		s, err := b.Subscribe(ctx, on("agent:docs-bot"))
		if err != nil {
			cancel()
			t.Fatalf("Subscribe: %v", err)
		}
		defer func() { _ = s.Close() }()

		errc := make(chan error, 1)
		go func() {
			_, err := s.Recv(context.WithoutCancel(t.Context()))
			errc <- err
		}()
		time.Sleep(50 * time.Millisecond) // let Recv block first
		cancel()

		select {
		case err := <-errc:
			if !errors.Is(err, bus.ErrClosed) {
				t.Fatalf("Recv after the subscription context was cancelled = %v, want bus.ErrClosed", err)
			}
		case <-time.After(waitLong):
			t.Fatal("cancelling the subscription context did not unblock Recv")
		}
	})

	t.Run("RecvHonoursItsOwnContext", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})
		s := subscribe(t, b, on("agent:docs-bot"))

		ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
		errc := make(chan error, 1)
		go func() {
			_, err := s.Recv(ctx)
			errc <- err
		}()
		time.Sleep(50 * time.Millisecond) // let Recv block first
		cancel()

		select {
		case err := <-errc:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Recv error = %v, want context.Canceled", err)
			}
		case <-time.After(waitLong):
			t.Fatal("cancelling Recv's context did not unblock it")
		}
		// The stream itself is still usable: only that Recv was cancelled.
		publish(t, b, text("docs-bot", "run_1", 1))
		recvN(t, s, 1)
	})

	t.Run("SubscriptionsLeaveNoGoroutinesBehind", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})

		// One warm-up cycle first: whatever the backend starts lazily (a
		// connection pool, a reaper) must not be counted as a leak.
		cycle(t, b, false)
		cycle(t, b, true)
		runtime.GC()
		baseline := runtime.NumGoroutine()

		for range 20 {
			cycle(t, b, false) // released by Close
			cycle(t, b, true)  // released by cancelling the context
		}

		deadline := time.Now().Add(waitLong)
		for {
			runtime.GC()
			got := runtime.NumGoroutine()
			if got <= baseline {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("goroutines = %d after 40 subscribe/release cycles, want <= %d", got, baseline)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	t.Run("UnpublishableEventIsRejected", func(t *testing.T) {
		b := newBus(t, Config{Retention: 64, Backlog: 64})

		noRunID := text("docs-bot", "run_1", 1)
		noRunID.Topic = topic.MustParse("agent:docs-bot")
		wrongRun := text("docs-bot", "run_1", 1)
		wrongRun.RunID = "run_2"
		wrongAgent := text("docs-bot", "run_1", 1)
		wrongAgent.Agent = "triage-bot"
		noKind := text("docs-bot", "run_1", 1)
		noKind.Kind = ""

		cases := []struct {
			name string
			e    envelope.Event
		}{
			{"no topic", envelope.Event{Kind: envelope.KindText}},
			{"topic is not run-scoped", noRunID},
			{"run id disagrees with topic", wrongRun},
			{"agent disagrees with topic", wrongAgent},
			{"no kind", noKind},
		}
		for _, tc := range cases {
			if err := b.Publish(t.Context(), tc.e); !errors.Is(err, bus.ErrInvalid) {
				t.Errorf("Publish(%s) = %v, want bus.ErrInvalid", tc.name, err)
			}
		}
		if _, err := b.Subscribe(t.Context(), envelope.Subscription{}); !errors.Is(err, bus.ErrInvalid) {
			t.Errorf("Subscribe with no topic = %v, want bus.ErrInvalid", err)
		}
	})
}

// tracker is a consumer that keeps up: it reads as fast as it can and, if the
// bus terminates it for falling behind, resubscribes from the last sequence it
// processed. That is the recovery every consumer of an at-least-once bus is
// required to implement, and it is why terminating a slow consumer is safe.
//
// It tracks a single run, because resuming from a cursor is only unambiguous
// for one.
type tracker struct {
	seqs chan uint64
	errc chan error
	done chan struct{}
}

func newTracker(t *testing.T, b bus.Bus, sub envelope.Subscription) *tracker {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	tr := &tracker{
		seqs: make(chan uint64, 8192),
		errc: make(chan error, 1),
		done: make(chan struct{}),
	}
	go func() {
		defer close(tr.done)
		for {
			s, err := b.Subscribe(ctx, sub)
			if err != nil {
				tr.fail(fmt.Errorf("resubscribe from %d: %w", sub.From, err))
				return
			}
			for {
				e, err := s.Recv(ctx)
				if err != nil {
					_ = s.Close()
					if errors.Is(err, bus.ErrSlowConsumer) {
						break // resubscribe from the cursor we reached
					}
					tr.fail(err)
					return
				}
				sub.From = e.Seq
				select {
				case tr.seqs <- e.Seq:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	t.Cleanup(func() { cancel(); <-tr.done })
	return tr
}

func (tr *tracker) fail(err error) {
	select {
	case tr.errc <- err:
	default:
	}
}

// wait collects n sequences, or fails.
func (tr *tracker) wait(t *testing.T, n int) []uint64 {
	t.Helper()
	got := make([]uint64, 0, n)
	deadline := time.After(waitLong)
	for len(got) < n {
		select {
		case seq := <-tr.seqs:
			got = append(got, seq)
		case err := <-tr.errc:
			t.Fatalf("consumer failed after %d of %d events: %v", len(got), n, err)
		case <-tr.done:
			t.Fatalf("consumer stopped after %d of %d events", len(got), n)
		case <-deadline:
			t.Fatalf("consumer received %d of %d events before the deadline", len(got), n)
		}
	}
	return got
}

// cycle subscribes, moves one event through the stream, and releases it either
// by cancelling its context or by closing it.
func cycle(t *testing.T, b bus.Bus, byCancel bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.WithoutCancel(t.Context()))
	defer cancel()

	s, err := b.Subscribe(ctx, on("agent:docs-bot"))
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	publish(t, b, text("docs-bot", "run_cycle", 0))
	recvN(t, s, 1)
	if byCancel {
		cancel()
		return
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// on is a subscription to a topic, unfiltered, from the beginning of what the
// bus holds.
func on(t string) envelope.Subscription {
	return envelope.Subscription{Topic: topic.MustParse(t)}
}

// filtered is a subscription to a topic, narrowed.
func filtered(t string, f envelope.Filter) envelope.Subscription {
	return envelope.Subscription{Topic: topic.MustParse(t), Filter: f}
}

// ev builds an event whose payload identifies it, so a test can tell which
// event it received and not merely how many. A seq of 0 asks the bus to assign
// one.
func ev(agent, runID string, seq uint64, kind envelope.Kind) envelope.Event {
	return envelope.Event{
		Seq:     seq,
		Topic:   topic.MustParse("agent:" + agent + ":" + runID),
		RunID:   runID,
		Tenant:  "acme",
		Agent:   agent,
		Rev:     1,
		Origin:  "slack",
		Kind:    kind,
		Payload: []byte(label(runID, seq)),
	}
}

// text is a streaming-output event, the common case.
func text(agent, runID string, seq uint64) envelope.Event {
	return ev(agent, runID, seq, envelope.KindText)
}

// revised pins an event to an agent revision and an origin.
func revised(e envelope.Event, rev int, origin string) envelope.Event {
	e.Rev, e.Origin = rev, origin
	return e
}

func label(runID string, seq uint64) string {
	return fmt.Sprintf("%s#%d", runID, seq)
}

// publish publishes each event or fails the test.
func publish(t *testing.T, b bus.Bus, events ...envelope.Event) {
	t.Helper()
	for _, e := range events {
		if err := b.Publish(t.Context(), e); err != nil {
			t.Fatalf("Publish(%s, seq %d): %v", e.Topic, e.Seq, err)
		}
	}
}

// publishRange publishes one event per sequence in [from, to]. It returns an
// error rather than failing the test, so it can be called from a goroutine.
func publishRange(ctx context.Context, b bus.Bus, agent, runID string, from, to uint64) error {
	for seq := from; seq <= to; seq++ {
		if err := b.Publish(ctx, text(agent, runID, seq)); err != nil {
			return fmt.Errorf("publish %s: %w", label(runID, seq), err)
		}
	}
	return nil
}

// subscribe subscribes or fails the test, and closes the stream afterwards.
func subscribe(t *testing.T, b bus.Bus, sub envelope.Subscription) bus.Stream {
	t.Helper()
	s, err := b.Subscribe(context.WithoutCancel(t.Context()), sub)
	if err != nil {
		t.Fatalf("Subscribe(%s): %v", sub.Topic, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// recvN receives exactly n events or fails the test.
func recvN(t *testing.T, s bus.Stream, n int) []envelope.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), waitLong)
	defer cancel()

	got := make([]envelope.Event, 0, n)
	for range n {
		e, err := s.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv %d of %d: %v", len(got)+1, n, err)
		}
		got = append(got, e)
	}
	return got
}

// recvIdle asserts nothing more arrives. It is how a test proves an event was
// filtered out rather than merely late.
func recvIdle(t *testing.T, s bus.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), waitIdle)
	defer cancel()

	e, err := s.Recv(ctx)
	if err == nil {
		t.Fatalf("Recv returned %s, want nothing", label(e.RunID, e.Seq))
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Recv error = %v, want context.DeadlineExceeded", err)
	}
}

// drain reads up to n events and stops at the first error, returning both.
func drain(s bus.Stream, n int) ([]uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), waitLong)
	defer cancel()

	var got []uint64
	for range n {
		e, err := s.Recv(ctx)
		if err != nil {
			return got, err
		}
		got = append(got, e.Seq)
	}
	return got, nil
}

func seqsOf(events []envelope.Event) []uint64 {
	out := make([]uint64, len(events))
	for i, e := range events {
		out[i] = e.Seq
	}
	return out
}

func labelsOf(events []envelope.Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = label(e.RunID, e.Seq)
	}
	return out
}

// wantContiguous asserts got is exactly first..last ascending: no gap, no
// duplicate, no reordering.
func wantContiguous(t *testing.T, got []uint64, first, last uint64) {
	t.Helper()
	want := make([]uint64, 0, last-first+1)
	for seq := first; seq <= last; seq++ {
		want = append(want, seq)
	}
	if slices.Equal(got, want) {
		return
	}
	t.Fatalf("sequences = %s, want %d..%d contiguous", summarize(got), first, last)
}

// summarize renders a long sequence list as runs, so a failure reads as "1..99,
// 101..200" instead of two hundred numbers.
func summarize(seqs []uint64) string {
	if len(seqs) == 0 {
		return "[]"
	}
	var b strings.Builder
	start := seqs[0]
	for i := 1; i <= len(seqs); i++ {
		if i < len(seqs) && seqs[i] == seqs[i-1]+1 {
			continue
		}
		if b.Len() > 0 {
			b.WriteString(", ")
		}
		if start == seqs[i-1] {
			fmt.Fprintf(&b, "%d", start)
		} else {
			fmt.Fprintf(&b, "%d..%d", start, seqs[i-1])
		}
		if i < len(seqs) {
			start = seqs[i]
		}
	}
	return "[" + b.String() + "]"
}
