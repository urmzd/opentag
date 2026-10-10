package router_test

import (
	"context"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Probe: Release then Register of the same run must not put two goroutines on
// one (run, target) surface at once.
func TestProbeReleaseThenRegisterKeepsOneLane(t *testing.T) {
	gate := make(chan struct{})
	slack := newSink("slack")
	slack.hook = func(ctx context.Context, attempt int, e envelope.Event) error {
		if e.Seq == 1 {
			select {
			case <-gate:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	h := start(t, registry(t, slack))

	routes := []envelope.Route{{Target: address.MustParse(slackTarget)}}
	if err := h.Register("run_1", routes); err != nil {
		t.Fatalf("Register: %v", err)
	}
	for seq := uint64(1); seq <= 5; seq++ {
		h.publish(t, event("docs-bot", "run_1", seq, envelope.KindText))
	}
	slack.waitTries(t, 1) // lane is inside seq 1

	h.Release("run_1")
	if err := h.Register("run_1", routes); err != nil {
		t.Fatalf("re-Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_1", 6, envelope.KindText))

	close(gate)
	got := seqsOf(slack.waitFor(t, 6))
	t.Logf("order = %v", got)
	for i, seq := range got {
		if seq != uint64(i+1) {
			t.Fatalf("delivery order to one surface = %v, want ascending", got)
		}
	}
}

// Probe: a second Run that starts while the first Run's shutdown is still
// waiting on its lanes.
func TestProbeConcurrentRunRestart(t *testing.T) {
	gate := make(chan struct{})
	defer close(gate)
	slack := newSink("slack")
	slack.hook = func(ctx context.Context, _ int, _ envelope.Event) error {
		select {
		case <-gate:
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
		return nil
	}
	h := start(t, registry(t, slack))
	if err := h.Register("run_1", []envelope.Route{{Target: address.MustParse(slackTarget)}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.publish(t, event("docs-bot", "run_1", 1, envelope.KindText))
	slack.waitTries(t, 1)

	// Cancel and immediately race a new Run against the shutdown's wg.Wait.
	h.cancel()
	for range 200 {
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = h.Run(ctx) }()
		time.Sleep(50 * time.Microsecond)
		cancel()
	}
	// stop is the harness's join point and the only reader of h.done: waiting
	// on the channel here instead would consume the result the cleanup is
	// about to wait for, and the cleanup would then block until its own
	// deadline and fail a test that had already passed.
	h.stop(t)
}
