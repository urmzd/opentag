package main

// This file holds everything the story in main.go needs but is not itself the
// story: two fake connectors, an executor, a corpus, and a bus wrapper that can
// be told to fail once.
//
// They are fakes only in the sense that they do not open sockets. Each one
// implements exactly the interface the real thing implements — connector.Sink,
// runtime.Executor, agentrt.Retriever, bus.Bus — so nothing in main.go is
// simplified for the demo. Swapping pkg/connectors/slack in for slackSink is a
// constructor change and nothing else.

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	saigetypes "github.com/urmzd/saige/agent/types"
	ragtypes "github.com/urmzd/saige/rag/types"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/agentrt"
	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/bus"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/runtime"
)

// ── the transcript ──────────────────────────────────────────────────
//
// The two sinks below are handed the same run's events on two independent
// lanes. pkg/router guarantees order within one (run, target) pair and
// deliberately imposes none across targets, so a slow Jira sink cannot delay a
// webhook. That is the right property for a delivery system and the wrong
// property for a document: printing straight from Deliver makes the two
// surfaces interleave differently on every run.
//
// So the sinks RECORD what they wrote to their surface, keyed by the sequence
// that caused the write, and main prints the settled transcript in
// (seq, surface) order once the run is terminal. Nothing about what each sink
// received, or when, is changed by this — only when the ink dries.

type transcript struct {
	mu    sync.Mutex
	lines []line
}

type line struct {
	seq     uint64
	surface string
	text    string
}

// record appends one line of surface output attributed to an event.
func (t *transcript) record(seq uint64, surface, format string, args ...any) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lines = append(t.lines, line{seq: seq, surface: surface, text: fmt.Sprintf(format, args...)})
}

// flush prints everything recorded so far and forgets it. The sort is stable,
// so several lines written by one sink for one event keep the order the sink
// wrote them in.
func (t *transcript) flush() {
	t.mu.Lock()
	defer t.mu.Unlock()
	slices.SortStableFunc(t.lines, func(a, b line) int {
		if c := cmp.Compare(a.seq, b.seq); c != 0 {
			return c
		}
		return cmp.Compare(a.surface, b.surface)
	})
	for _, l := range t.lines {
		fmt.Println(l.text)
	}
	t.lines = nil
}

// ── the Slack sink ──────────────────────────────────────────────────
//
// A chat sink renders a run by posting ONE message and editing it as the answer
// arrives, which is why pkg/router carries events for one (run, target) pair on
// a single goroutine: two edits that overtake each other would leave the wrong
// final text on the surface permanently.
//
// This fake keeps the same shape. It accumulates deltas into a message body and
// prints each edit, so the terminal shows what a Slack thread would show.

type slackSink struct {
	out *transcript

	mu      sync.Mutex
	threads map[string]*slackThread // run id -> the message being edited
}

type slackThread struct {
	target string
	edits  int
	body   strings.Builder
	notes  []payload.Citation
	events int
	done   bool
}

func newSlackSink(out *transcript) *slackSink {
	return &slackSink{out: out, threads: make(map[string]*slackThread)}
}

// Name is the address scheme this connector answers to: slack://... targets
// resolve here. It is the whole of connector.Connector.
func (s *slackSink) Name() string { return "slack" }

// Deliver implements connector.Sink. It is handed every event whose kind the
// route selected — for this demo, every kind at all.
func (s *slackSink) Deliver(_ context.Context, target address.Address, e envelope.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	th := s.threads[e.RunID]
	if th == nil {
		th = &slackThread{target: target.String()}
		s.threads[e.RunID] = th
	}
	th.events++

	switch e.Kind {
	case envelope.KindAccepted:
		th.edits++
		s.out.record(e.Seq, "slack", "    slack   post   %s", th.target)
		s.out.record(e.Seq, "slack", "    slack   edit %d  %s", th.edits, "_docs-bot is looking into this..._")

	case envelope.KindCitation:
		c, err := payload.Decode[payload.Citation](e)
		if err != nil {
			return err
		}
		th.notes = append(th.notes, c)

	case envelope.KindText:
		t, err := payload.Decode[payload.Text](e)
		if err != nil {
			return err
		}
		th.body.WriteString(t.Text)
		th.edits++
		s.out.record(e.Seq, "slack", "    slack   edit %d  %s", th.edits, quote(th.body.String()))

	case envelope.KindCompleted:
		th.done = true
		s.out.record(e.Seq, "slack", "    slack   edit %d  (final, with %d footnote(s))", th.edits+1, len(th.notes))
		for _, c := range th.notes {
			s.out.record(e.Seq, "slack", "              %s %s", c.Label, c.Source.SourceURI)
		}
	}
	return nil
}

// delivered reports how many events this sink was handed for a run, and whether
// the run's terminal event arrived.
func (s *slackSink) delivered(runID string) (n int, done bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	th := s.threads[runID]
	if th == nil {
		return 0, false
	}
	return th.events, th.done
}

// ── the webhook sink ────────────────────────────────────────────────
//
// The other half of the N:M claim. Same run, same bus, same moment — but this
// route asked for lifecycle.completed and nothing else, so this sink never
// learns that deltas exist. It POSTs a JSON body once and is finished.

type webhookSink struct {
	out *transcript

	mu     sync.Mutex
	posts  map[string]int // run id -> bodies posted
	kinds  map[envelope.Kind]int
	bodies []string
}

func newWebhookSink(out *transcript) *webhookSink {
	return &webhookSink{out: out, posts: make(map[string]int), kinds: make(map[envelope.Kind]int)}
}

func (w *webhookSink) Name() string { return "webhook" }

func (w *webhookSink) Deliver(_ context.Context, target address.Address, e envelope.Event) error {
	life, err := payload.Decode[payload.Lifecycle](e)
	if err != nil {
		return err
	}
	body, err := json.Marshal(map[string]any{
		"run":   e.RunID,
		"agent": e.Agent,
		"rev":   e.Rev,
		"kind":  string(e.Kind),
		"text":  life.Text,
	})
	if err != nil {
		return err
	}

	w.mu.Lock()
	w.posts[e.RunID]++
	w.kinds[e.Kind]++
	w.bodies = append(w.bodies, string(body))
	w.mu.Unlock()

	w.out.record(e.Seq, "webhook", "    webhook POST   https://hooks.example.test/%s/%s", target.Workspace, target.Resource())
	w.out.record(e.Seq, "webhook", "              %s", string(body))
	return nil
}

func (w *webhookSink) delivered(runID string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.posts[runID]
}

// seenKinds returns every kind this sink was ever handed, which is the evidence
// for "the webhook never saw a delta".
func (w *webhookSink) seenKinds() map[envelope.Kind]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[envelope.Kind]int, len(w.kinds))
	for k, v := range w.kinds {
		out[k] = v
	}
	return out
}

// ── the executor ────────────────────────────────────────────────────
//
// runtime.Executor is the seam between the durable turn and the agent loop. A
// deployment passes runtime.NewSandbox, which runs the turn as a dispatch task
// under the NGAC policy compiled from the pinned revision's Access grant.
//
// This example passes an in-process executor instead, for one reason: a sandbox
// is about CONTAINMENT, and containment is not what this example is trying to
// show. Everything above the seam — pinning, banding, publication, delivery,
// replay — is identical either way, because the seam is one method.
//
// What it does is real. It builds the agent with pkg/agentrt from the pinned
// revision and runs a saige agent loop against agenttest.ScriptedProvider, so
// the deltas that become events are saige's own typed deltas, translated by the
// same code a production turn uses.

type offlineExecutor struct {
	// calls counts how often the model was actually reached. It is the whole
	// proof in section 7: duraturo replays the turn, but a recorded activity
	// is served from the ledger, so this must not move on a replay.
	calls atomic.Int64

	retriever agentrt.Retriever
	scripts   map[string][][]saigetypes.Delta // agent name -> one entry per model call
}

func newOfflineExecutor(retriever agentrt.Retriever) *offlineExecutor {
	return &offlineExecutor{
		retriever: retriever,
		scripts:   make(map[string][][]saigetypes.Delta),
	}
}

// script sets what the scripted provider replays for an agent.
func (x *offlineExecutor) script(agent string, deltas ...[]saigetypes.Delta) {
	x.scripts[agent] = deltas
}

// Execute implements runtime.Executor.
//
// req.Revision is a resolved, PINNED revision, not a name and a number. An
// executor must never re-resolve it: it may be running in another process, and
// "which definition to run" is not a question it is allowed to answer.
func (x *offlineExecutor) Execute(ctx context.Context, req runtime.Request, emit func(context.Context, agentrt.Chunk) error) (runtime.Outcome, error) {
	x.calls.Add(1)

	runner, err := agentrt.New(req.Revision,
		// The spec says provider "offline", so agentrt resolves it to
		// saige's agenttest.ScriptedProvider with this script. Change the
		// spec's provider to "anthropic" and the same code calls a model.
		agentrt.WithScript(x.scripts[req.Agent()]...),
		agentrt.WithRetriever(x.retriever),
		agentrt.WithLogger(quiet),
	)
	if err != nil {
		return runtime.Outcome{}, err
	}

	res, err := runner.Run(ctx, agentrt.Turn{Text: req.Text, Meta: req.Meta}, emit)
	if err != nil {
		return runtime.Outcome{}, err
	}
	return runtime.Outcome{Text: res.Text, Chunks: res.Chunks, Citations: res.Citations}, nil
}

func (x *offlineExecutor) modelCalls() int64 { return x.calls.Load() }

// ── the corpus ──────────────────────────────────────────────────────
//
// agentrt.Retriever is the narrow half of saige's RAG pipeline: a turn
// retrieves, it does not ingest. A deployment with a real pipeline adapts it in
// four lines (Search, then take .Context).
//
// Citations are derived from THIS, not from the model. That is deliberate and
// it is what makes them checkable: they report the passages the turn was given,
// with resolvable provenance, rather than the ones a model claims to have used.

type corpus struct{ blocks []ragtypes.ContextBlock }

func (c corpus) Retrieve(context.Context, string) (*ragtypes.AssembledContext, error) {
	var prompt strings.Builder
	prompt.WriteString("Sources you may cite:")
	for _, b := range c.blocks {
		fmt.Fprintf(&prompt, "\n%s %s", b.Citation, b.Text)
	}
	return &ragtypes.AssembledContext{
		Prompt:     prompt.String(),
		Blocks:     c.blocks,
		TokenCount: 128,
	}, nil
}

// ── a bus that can be told to fail ──────────────────────────────────
//
// pkg/bus.Memory is a complete broker, not a stub, so there is no failure
// injection in it and there should not be. Section 7 needs one specific
// publish to fail once — that is how it forces duraturo to replay a turn whose
// agent activity is already recorded — so the failure is wrapped around the
// bus instead of built into it.
//
// Only Publish is overridden. Subscribe passes through, so the observer and the
// router read the real broker.

type flakyBus struct {
	bus.Bus

	mu   sync.Mutex
	fail map[string]int // runID + "/" + kind -> remaining failures
}

func newFlakyBus(b bus.Bus) *flakyBus {
	return &flakyBus{Bus: b, fail: make(map[string]int)}
}

// failOnce arms one failure for the next publish of kind on runID.
func (f *flakyBus) failOnce(runID string, kind envelope.Kind) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[runID+"/"+string(kind)]++
}

func (f *flakyBus) Publish(ctx context.Context, e envelope.Event) error {
	key := e.RunID + "/" + string(e.Kind)
	f.mu.Lock()
	armed := f.fail[key] > 0
	if armed {
		f.fail[key]--
	}
	f.mu.Unlock()

	if armed {
		return fmt.Errorf("simulated broker outage publishing %s seq %d", e.Kind, e.Seq)
	}
	return f.Bus.Publish(ctx, e)
}

// ── the third party ─────────────────────────────────────────────────
//
// An observer holds ONE subscription to agent:<name> and receives every run of
// that agent, from every trigger, forever. It never invoked anything, nothing
// routes to it, and no run knows it exists.

type observer struct {
	mu   sync.Mutex
	seen []envelope.Event
}

// watch subscribes and collects until ctx is cancelled. It returns a channel
// closed when the subscription has ended, so main can shut down cleanly.
func (o *observer) watch(ctx context.Context, b bus.Bus, sub envelope.Subscription) (<-chan struct{}, error) {
	stream, err := b.Subscribe(ctx, sub)
	if err != nil {
		return nil, err
	}
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		defer func() { _ = stream.Close() }()
		for {
			e, err := stream.Recv(ctx)
			if err != nil {
				return
			}
			o.mu.Lock()
			o.seen = append(o.seen, e)
			o.mu.Unlock()
		}
	}()
	return stopped, nil
}

// events returns everything the observer saw for one run, in arrival order.
func (o *observer) events(runID string) []envelope.Event {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []envelope.Event
	for _, e := range o.seen {
		if e.RunID == runID {
			out = append(out, e)
		}
	}
	return out
}

// total is how many events reached the bus in all, across every run. It is
// ground truth in a way runtime.Output.Events is not: Output reports what ONE
// successful pass published (accepted, opened, the chunks, completed), so for a
// run that replayed it undercounts by the interrupted attempt's output, which
// really was published and really was seen.
func (o *observer) total() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.seen)
}

// runs returns the distinct run ids the observer has seen, in first-sight
// order. Two entries means one subscription served two unrelated runs.
func (o *observer) runs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, e := range o.seen {
		if !seen[e.RunID] {
			seen[e.RunID] = true
			out = append(out, e.RunID)
		}
	}
	return out
}

// sawKind reports whether the observer received a kind for a run.
func (o *observer) sawKind(runID string, kind envelope.Kind) bool {
	for _, e := range o.events(runID) {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

// ── small helpers ───────────────────────────────────────────────────

// streamed builds one scripted model response out of several text deltas, so
// the stream arrives in pieces the way a real provider's does.
func streamed(parts ...string) []saigetypes.Delta {
	out := make([]saigetypes.Delta, 0, len(parts)+2)
	out = append(out, saigetypes.TextStartDelta{})
	for _, p := range parts {
		out = append(out, saigetypes.TextContentDelta{Content: p})
	}
	return append(out, saigetypes.TextEndDelta{})
}

// quote renders a message body on one line, elided in the middle so the growth
// between edits stays visible at a glance.
func quote(s string) string {
	const width = 58
	s = strings.Join(strings.Fields(s), " ")
	if len(s) <= width {
		return `"` + s + `"`
	}
	return `"` + s[:width-20] + " … " + s[len(s)-17:] + `"`
}

// decodeCitation reads a delta.citation event's body.
func decodeCitation(e envelope.Event) (payload.Citation, error) {
	return payload.Decode[payload.Citation](e)
}

// summarize renders an event's payload as one short line, for the observer's
// log. It goes through pkg/agentrt/payload, which is a leaf package: decoding
// an event needs neither saige nor duraturo, which is the point of it existing.
func summarize(e envelope.Event) string {
	body, known, err := payload.Of(e)
	if err != nil || !known {
		return ""
	}
	switch v := body.(type) {
	case payload.Text:
		return quote(v.Text)
	case payload.Citation:
		return fmt.Sprintf("%s %s", v.Label, v.Source.SourceURI)
	case payload.Lifecycle:
		var parts []string
		if v.Rev > 0 {
			parts = append(parts, fmt.Sprintf("rev=%d", v.Rev))
		}
		if v.Attempt > 0 {
			parts = append(parts, fmt.Sprintf("attempt=%d", v.Attempt))
		}
		if v.Text != "" {
			parts = append(parts, quote(v.Text))
		}
		if v.Error != "" {
			parts = append(parts, "error="+v.Error)
		}
		return strings.Join(parts, " ")
	default:
		return ""
	}
}
