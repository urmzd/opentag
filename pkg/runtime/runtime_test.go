package runtime_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/duraturo"
	"github.com/urmzd/duraturo/pkg/ledger"
	dqueue "github.com/urmzd/duraturo/pkg/queue"
	"github.com/urmzd/duraturo/pkg/replay"
	drun "github.com/urmzd/duraturo/pkg/run"
	"github.com/urmzd/duraturo/pkg/worker"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/agentrt"
	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/runtime"
	"github.com/urmzd/opentag/pkg/topic"
)

// ── fixtures ────────────────────────────────────────────────────────

// specs is a control plane's revision history, complete enough to be revised
// and to answer a pinned lookup after it has been.
type specs struct {
	mu   sync.Mutex
	revs map[string][]agentrt.Revision // agent -> revisions, index 0 is rev 1
}

func newSpecs() *specs { return &specs{revs: make(map[string][]agentrt.Revision)} }

// revise appends the next revision of an agent and returns it.
func (s *specs) revise(agent, prompt string) agentrt.Revision {
	s.mu.Lock()
	defer s.mu.Unlock()
	rev := agentrt.Revision{
		Rev: len(s.revs[agent]) + 1,
		Spec: agentrt.Spec{
			Name:         agent,
			Provider:     agentrt.ProviderOffline,
			Model:        "scripted",
			SystemPrompt: prompt,
		},
	}
	rev.Hash = fmt.Sprintf("sha256:%s@%d", agent, rev.Rev)
	s.revs[agent] = append(s.revs[agent], rev)
	return rev
}

// at overwrites a stored revision, which only a broken control plane would do.
// It exists so a test can prove that opentag notices.
func (s *specs) corrupt(agent string, rev int, hash string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.revs[agent][rev-1].Hash = hash
}

func (s *specs) Latest(_ context.Context, _, agent string) (agentrt.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	history := s.revs[agent]
	if len(history) == 0 {
		return agentrt.Revision{}, fmt.Errorf("no such agent %q", agent)
	}
	return history[len(history)-1], nil
}

func (s *specs) At(_ context.Context, _, agent string, rev int) (agentrt.Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	history := s.revs[agent]
	if rev < 1 || rev > len(history) {
		return agentrt.Revision{}, fmt.Errorf("agent %q has no revision %d", agent, rev)
	}
	return history[rev-1], nil
}

// recorder is a bus that keeps every Publish call, deduplication included, so a
// test can assert on what the runtime chose to publish rather than on what a
// broker decided to keep.
type recorder struct {
	mu     sync.Mutex
	events []envelope.Event
	fail   func(envelope.Event) error
}

func (r *recorder) Publish(_ context.Context, e envelope.Event) error {
	r.mu.Lock()
	fail := r.fail
	r.mu.Unlock()
	if fail != nil {
		if err := fail(e); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

func (r *recorder) all() []envelope.Event {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]envelope.Event(nil), r.events...)
}

func (r *recorder) kinds() []envelope.Kind {
	out := []envelope.Kind{}
	for _, e := range r.all() {
		out = append(out, e.Kind)
	}
	return out
}

func (r *recorder) ofKind(k envelope.Kind) []envelope.Event {
	var out []envelope.Event
	for _, e := range r.all() {
		if e.Kind == k {
			out = append(out, e)
		}
	}
	return out
}

func (r *recorder) failWhen(fn func(envelope.Event) error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fail = fn
}

// executor is an Executor that emits a scripted stream and counts how often it
// actually ran, which is how a test sees whether replay paid for the model twice.
type executor struct {
	mu       sync.Mutex
	runs     int
	requests []runtime.Request
	chunks   []agentrt.Chunk
	err      error
	answer   string
}

func newExecutor(answer string, chunks ...agentrt.Chunk) *executor {
	return &executor{answer: answer, chunks: chunks}
}

func (e *executor) Execute(ctx context.Context, req runtime.Request, emit func(context.Context, agentrt.Chunk) error) (runtime.Outcome, error) {
	e.mu.Lock()
	e.runs++
	e.requests = append(e.requests, req)
	chunks, failure, answer := e.chunks, e.err, e.answer
	e.mu.Unlock()

	for _, c := range chunks {
		if err := emit(ctx, c); err != nil {
			return runtime.Outcome{}, err
		}
	}
	if failure != nil {
		return runtime.Outcome{}, failure
	}
	return runtime.Outcome{Text: answer, Chunks: len(chunks)}, nil
}

func (e *executor) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.runs
}

func (e *executor) seen() []runtime.Request {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]runtime.Request(nil), e.requests...)
}

func textChunk(t *testing.T, s string) agentrt.Chunk {
	t.Helper()
	b, err := payload.Encode(payload.Text{Text: s})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	return agentrt.Chunk{Kind: envelope.KindText, Payload: b}
}

// harness is a runtime wired to in-memory duraturo storage.
type harness struct {
	rt     *runtime.Runtime
	bus    *recorder
	specs  *specs
	exec   *executor
	ledger *ledger.Memory
	queue  *dqueue.Memory
}

func newHarness(t *testing.T, exec *executor, opts ...runtime.Option) *harness {
	t.Helper()
	lgr := ledger.NewMemory()
	q := dqueue.NewMemory()
	bus := &recorder{}
	sp := newSpecs()
	rt, err := runtime.New(duraturo.New(lgr, q), sp, bus, exec, opts...)
	if err != nil {
		t.Fatalf("runtime.New: %v", err)
	}
	return &harness{rt: rt, bus: bus, specs: sp, exec: exec, ledger: lgr, queue: q}
}

// work starts a worker for the duration of the test.
func (h *harness) work(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = worker.New(h.ledger, h.queue,
			worker.WithRegistry(h.rt.Registry()),
			worker.WithConcurrency(1),
			worker.WithJanitorEvery(0),
			worker.WithLeaseTTL(2*time.Second),
			worker.WithBackoff(func(int) time.Duration { return time.Millisecond }),
		).Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func tag(agent, id, text string) envelope.Tag {
	return envelope.Tag{
		ID:     id,
		Tenant: "acme",
		Agent:  agent,
		Origin: "github",
		Source: address.MustParse("github://urmzd/opentag/issues/42"),
		Text:   text,
		At:     time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
	}
}

// ── tests ───────────────────────────────────────────────────────────

// A run's identity is the tag's identity, so a redelivery joins the run it
// duplicated. A tag id that cannot be a topic segment is hashed rather than
// rewritten, because two distinct ids collapsing onto one run is the one failure
// idempotency cannot survive.
func TestRunIDPreservesTagIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		tagID string
		want  string
	}{
		{"a slack event id is already topic safe", "Ev01ABCDEF", "Ev01ABCDEF"},
		{"a github delivery guid is topic safe", "72d3162e-cc78-11e3-81ab-4c9367dc0958", "72d3162e-cc78-11e3-81ab-4c9367dc0958"},
	}
	for _, tc := range tests {
		if got := runtime.RunID(tc.tagID); got != tc.want {
			t.Errorf("%s: RunID(%q) is %q, want %q", tc.name, tc.tagID, got, tc.want)
		}
	}

	awkward := []string{"slack:1699123456.001", "a/b", "spaces here", "üñî"}
	seen := map[string]string{}
	for _, id := range awkward {
		got := runtime.RunID(id)
		if _, err := topic.Run("docs-bot", got); err != nil {
			t.Errorf("RunID(%q) is %q, which is not a usable topic segment: %v", id, got, err)
		}
		if runtime.RunID(id) != got {
			t.Errorf("RunID(%q) is not deterministic", id)
		}
		if other, clash := seen[got]; clash {
			t.Errorf("RunID(%q) and RunID(%q) both produced %q", id, other, got)
		}
		seen[got] = id
	}
}

// The lifecycle a subscriber sees on a healthy run, in order, with the answer on
// the completed event so a sink that wants only the outcome needs nothing else.
func TestLifecycleOrderOnSuccess(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor("the docs are updated", func() agentrt.Chunk { return agentrt.Chunk{} }()))
	h.exec.chunks = nil // replaced below, once t is available for encoding
	h.exec.chunks = []agentrt.Chunk{textChunk(t, "the docs "), textChunk(t, "are updated")}
	h.specs.revise("docs-bot", "you document things")
	h.work(t)

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev1", "update the docs"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	out, err := h.rt.Result(ctx, acc.RunID)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if out.Text != "the docs are updated" {
		t.Errorf("run output is %q, want the agent's answer", out.Text)
	}

	want := []envelope.Kind{
		envelope.KindAccepted,
		envelope.KindStarted,
		envelope.KindText,
		envelope.KindText,
		envelope.KindCompleted,
	}
	if got := h.bus.kinds(); !sameKinds(got, want) {
		t.Fatalf("lifecycle is %v, want %v", got, want)
	}

	events := h.bus.all()
	for i := 1; i < len(events); i++ {
		if events[i].Seq <= events[i-1].Seq {
			t.Fatalf("sequences are not increasing: %d then %d", events[i-1].Seq, events[i].Seq)
		}
	}
	if events[0].Seq != runtime.SeqAccepted {
		t.Errorf("accepted is seq %d, want %d", events[0].Seq, runtime.SeqAccepted)
	}
	last := events[len(events)-1]
	if last.Seq != out.LastSeq {
		t.Errorf("Output.LastSeq is %d, want the completed event's seq %d", out.LastSeq, last.Seq)
	}
	body, err := payload.Decode[payload.Lifecycle](last)
	if err != nil {
		t.Fatalf("decode completed: %v", err)
	}
	if body.Text != "the docs are updated" {
		t.Errorf("completed event carries %q, want the answer", body.Text)
	}
	if body.Attempt != 1 {
		t.Errorf("completed event reports attempt %d, want 1", body.Attempt)
	}
}

// Every event of a run carries the run's identity, because a subscriber filters
// on those fields instead of on a topic per revision or origin.
func TestEveryEventCarriesRunIdentity(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor("done"))
	h.exec.chunks = []agentrt.Chunk{textChunk(t, "done")}
	h.specs.revise("docs-bot", "v1")
	rev := h.specs.revise("docs-bot", "v2")
	h.work(t)

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev2", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if _, err := h.rt.Result(ctx, acc.RunID); err != nil {
		t.Fatalf("Result: %v", err)
	}

	wantTopic, err := topic.Run("docs-bot", acc.RunID)
	if err != nil {
		t.Fatalf("topic.Run: %v", err)
	}
	for _, e := range h.bus.all() {
		if e.Topic.String() != wantTopic.String() {
			t.Errorf("%s published on %s, want the run topic %s", e.Kind, e.Topic, wantTopic)
		}
		if e.RunID != acc.RunID {
			t.Errorf("%s carries run %q, want %q", e.Kind, e.RunID, acc.RunID)
		}
		if e.Tenant != "acme" {
			t.Errorf("%s carries tenant %q, want acme", e.Kind, e.Tenant)
		}
		if e.Agent != "docs-bot" {
			t.Errorf("%s carries agent %q, want docs-bot", e.Kind, e.Agent)
		}
		if e.Rev != rev.Rev {
			t.Errorf("%s carries rev %d, want the pinned %d", e.Kind, e.Rev, rev.Rev)
		}
		if e.Origin != "github" {
			t.Errorf("%s carries origin %q, want github", e.Kind, e.Origin)
		}
		if e.At.IsZero() {
			t.Errorf("%s has no timestamp", e.Kind)
		}
	}
}

// THE property this package exists to protect: a run pins its revision at accept
// time and executes that revision forever, even after the agent has been revised
// past it. The assertion is on the definition actually handed to the executor,
// not merely on a number.
func TestARunPinnedToARevisionStillExecutesItAfterARevise(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor("answered"))
	h.exec.chunks = []agentrt.Chunk{textChunk(t, "answered")}
	for i := 1; i <= 5; i++ {
		h.specs.revise("docs-bot", fmt.Sprintf("revision %d instructions", i))
	}
	pinned := h.specs.revise("docs-bot", "revision 6 instructions")
	if pinned.Rev != 6 {
		t.Fatalf("fixture pinned revision %d, want 6", pinned.Rev)
	}

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev6", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if acc.Rev != 6 {
		t.Fatalf("accepted at revision %d, want 6", acc.Rev)
	}

	// The agent is revised while the run sits in the queue, which is exactly
	// the race a real deployment has: a tag arrives, someone ships a prompt
	// change, and the worker picks the run up afterwards.
	seven := h.specs.revise("docs-bot", "revision 7 instructions")
	if seven.Rev != 7 {
		t.Fatalf("fixture revised to %d, want 7", seven.Rev)
	}

	h.work(t)
	if _, err := h.rt.Result(ctx, acc.RunID); err != nil {
		t.Fatalf("Result: %v", err)
	}

	seen := h.exec.seen()
	if len(seen) != 1 {
		t.Fatalf("the executor ran %d times, want 1", len(seen))
	}
	req := seen[0]
	if req.Rev() != 6 {
		t.Errorf("the turn executed revision %d, want the pinned 6", req.Rev())
	}
	if got := req.Revision.Spec.SystemPrompt; got != "revision 6 instructions" {
		t.Errorf("the turn executed the prompt %q, want revision 6's", got)
	}
	if got := req.Revision.Hash; got != pinned.Hash {
		t.Errorf("the turn executed hash %q, want the pinned %q", got, pinned.Hash)
	}
	for _, e := range h.bus.all() {
		if e.Rev != 6 {
			t.Errorf("%s reports revision %d, want the pinned 6", e.Kind, e.Rev)
		}
	}
}

// A redelivery joins the existing run rather than starting a second one, and
// cannot re-pin it: the revision a run executes is whatever was current the first
// time the tag arrived.
func TestARedeliveredTagJoinsTheRunItDuplicated(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor("answered"))
	h.exec.chunks = []agentrt.Chunk{textChunk(t, "answered")}
	h.specs.revise("docs-bot", "revision 1 instructions")

	ctx := context.Background()
	first, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-dup", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	h.specs.revise("docs-bot", "revision 2 instructions")

	second, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-dup", "hello"))
	if err != nil {
		t.Fatalf("Accept (redelivery): %v", err)
	}
	if second.RunID != first.RunID {
		t.Fatalf("the redelivery started run %q, want it to join %q", second.RunID, first.RunID)
	}

	h.work(t)
	if _, err := h.rt.Result(ctx, first.RunID); err != nil {
		t.Fatalf("Result: %v", err)
	}
	if n := h.exec.count(); n != 1 {
		t.Fatalf("the turn executed %d times, want once", n)
	}
	if rev := h.exec.seen()[0].Rev(); rev != 1 {
		t.Fatalf("the run executed revision %d, want the revision pinned by the first delivery", rev)
	}
	// Both deliveries announce, and the redelivery resolves the revision that
	// is current when IT arrives — it cannot know the run already exists,
	// because the announcement has to precede the submission that would tell
	// it. What collapses the two into one is the sequence: both land on
	// SeqAccepted, so the broker's first-write-wins rule keeps the first and
	// discards the redelivery's, revision and all. This recorder is a spy on
	// Publish rather than a broker, so it sees both.
	accepted := h.bus.ofKind(envelope.KindAccepted)
	if len(accepted) != 2 {
		t.Fatalf("got %d accepted events, want 2, one per delivery", len(accepted))
	}
	for _, e := range accepted {
		if e.Seq != runtime.SeqAccepted {
			t.Errorf("an accepted event landed at seq %d, want %d: reusing the sequence is the only thing that lets the broker collapse the redelivery", e.Seq, runtime.SeqAccepted)
		}
	}
	if accepted[0].Rev != 1 {
		t.Errorf("the accepted event the broker keeps reports revision %d; the run pinned 1", accepted[0].Rev)
	}
	// Everything the run itself produced — which is everything a subscriber
	// will actually see, since the redelivery's announcement never survives —
	// reports the pinned revision.
	for _, e := range h.bus.all() {
		if e.Kind == envelope.KindAccepted {
			continue
		}
		if e.Rev != 1 {
			t.Errorf("%s reports revision %d; the run pinned 1", e.Kind, e.Rev)
		}
	}
}

// A failure has to reach the bus with enough information to tell a sink whether
// to give up: the attempt, and whether anything will try again.
func TestLifecycleOrderOnFailure(t *testing.T) {
	t.Parallel()

	exec := newExecutor("")
	exec.err = duraturo.NonRetryable(errors.New("the model refused"))
	h := newHarness(t, exec)
	h.exec.chunks = []agentrt.Chunk{textChunk(t, "partial ")}
	h.specs.revise("docs-bot", "v1")
	h.work(t)

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-fail", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if _, err := h.rt.Result(ctx, acc.RunID); err == nil {
		t.Fatal("the run succeeded, want a terminal failure")
	}

	want := []envelope.Kind{
		envelope.KindAccepted,
		envelope.KindStarted,
		envelope.KindText,
		envelope.KindFailed,
	}
	if got := h.bus.kinds(); !sameKinds(got, want) {
		t.Fatalf("lifecycle is %v, want %v", got, want)
	}
	failed := h.bus.ofKind(envelope.KindFailed)
	body, err := payload.Decode[payload.Lifecycle](failed[0])
	if err != nil {
		t.Fatalf("decode failed event: %v", err)
	}
	if !body.Terminal {
		t.Error("a non-retryable failure was reported as non-terminal")
	}
	if !strings.Contains(body.Error, "the model refused") {
		t.Errorf("failure reports %q, want the underlying reason", body.Error)
	}
	if body.Attempt != 1 {
		t.Errorf("failure reports attempt %d, want 1", body.Attempt)
	}
	// The partial answer stays on the stream: a sink has already rendered it,
	// and retracting it would make the thread disagree with the run.
	if len(h.bus.ofKind(envelope.KindText)) != 1 {
		t.Errorf("the partial output was not published")
	}
}

// A retryable failure is NOT terminal, because the retry budget belongs to
// duraturo. Reporting it as terminal would tell a sink to give up on a run that
// is about to succeed.
func TestARetryableFailureIsReportedAsNonTerminalAndTheRunResumes(t *testing.T) {
	t.Parallel()

	exec := newExecutor("second time lucky")
	exec.err = errors.New("provider timed out")
	h := newHarness(t, exec)
	h.specs.revise("docs-bot", "v1")
	h.work(t)

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-retry", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	// Let the first attempt fail, then heal the executor.
	waitFor(t, func() bool { return len(h.bus.ofKind(envelope.KindFailed)) == 1 })
	exec.mu.Lock()
	exec.err = nil
	exec.mu.Unlock()

	out, err := h.rt.Result(ctx, acc.RunID)
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if out.Text != "second time lucky" {
		t.Errorf("run output is %q, want the second attempt's answer", out.Text)
	}

	failed := h.bus.ofKind(envelope.KindFailed)
	body, err := payload.Decode[payload.Lifecycle](failed[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Terminal {
		t.Error("a retryable failure was reported as terminal")
	}
	// The second attempt announces itself as a resumption, and everything it
	// publishes sits above everything the first attempt published.
	resumed := h.bos(envelope.KindResumed)
	if len(resumed) != 1 {
		t.Fatalf("got %d resumed events, want 1", len(resumed))
	}
	if resumed[0].Seq <= failed[0].Seq {
		t.Errorf("the resumed event is seq %d, at or below the failed attempt's %d", resumed[0].Seq, failed[0].Seq)
	}
	completed := h.bus.ofKind(envelope.KindCompleted)
	if len(completed) != 1 {
		t.Fatalf("got %d completed events, want 1", len(completed))
	}
	if completed[0].Seq <= resumed[0].Seq {
		t.Errorf("completed is seq %d, at or below resumed at %d", completed[0].Seq, resumed[0].Seq)
	}
	if n := h.exec.count(); n != 2 {
		t.Errorf("the executor ran %d times, want 2 (a retry re-executes)", n)
	}
}

// bos is ofKind under a shorter name, for the assertions that read better in one
// line.
func (h *harness) bos(k envelope.Kind) []envelope.Event { return h.bus.ofKind(k) }

// Replay must not pay for the model twice: the agent's activity is memoized, so a
// re-execution of the same attempt publishes the identical sequence — which the
// bus deduplicates — and never re-runs the turn.
func TestReplayingAnAttemptRepublishesTheIdenticalSequence(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor("answered"))
	h.exec.chunks = []agentrt.Chunk{textChunk(t, "ans"), textChunk(t, "wered")}
	h.specs.revise("docs-bot", "v1")

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-replay", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	body, _, err := h.ledger.Load(ctx, acc.RunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	fn, ok := h.rt.Registry().Lookup(runtime.ActivityTurn)
	if !ok {
		t.Fatalf("activity %q is not registered", runtime.ActivityTurn)
	}

	// Attempt 1, executing for real. This is what the worker does on a claim.
	execute := func(attempt int) {
		t.Helper()
		_, records, err := h.ledger.Load(ctx, acc.RunID)
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		frame := replay.NewFrame(acc.RunID, attempt, h.ledger, drun.JSONCodec{}, records, nil)
		if _, err := fn(replay.WithFrame(ctx, frame), body.Input); err != nil {
			t.Fatalf("attempt %d: %v", attempt, err)
		}
	}
	execute(1)
	first := fingerprint(h.bus.all())
	if n := h.exec.count(); n != 1 {
		t.Fatalf("the executor ran %d times on the first attempt, want 1", n)
	}

	// The same attempt, replayed from the ledger.
	execute(1)
	if n := h.exec.count(); n != 1 {
		t.Fatalf("the executor ran %d times after a replay, want 1: the memoized turn must not re-execute", n)
	}
	replayed := h.bus.all()[len(first):]

	// The turn's own lifecycle is published again, because the turn body
	// re-executes; the agent's deltas are not, because the agent is a nested
	// activity whose result is memoized and whose emits therefore never happen
	// a second time. That asymmetry is the point of the split.
	if got := kindsOf(replayed); !sameKinds(got, []envelope.Kind{envelope.KindStarted, envelope.KindCompleted}) {
		t.Errorf("the replay published %v, want the turn's lifecycle only", got)
	}
	// And the replay is invisible to a subscriber: every event it publishes
	// reuses a sequence the first execution already used, carrying the same
	// kind and the same body, so the broker's first-write-wins rule discards
	// all of it. That is what makes replay safe without a dedupe table.
	seen := make(map[string]bool, len(first))
	for _, f := range first {
		seen[f] = true
	}
	for _, f := range fingerprint(replayed) {
		if !seen[f] {
			t.Errorf("the replay published %s, which the first execution never did: the broker cannot collapse it", f)
		}
	}

	// A later attempt keeps the memo but moves into its own band, so the
	// events it re-publishes cannot be mistaken for the earlier ones — and the
	// agent still does not re-run.
	before := len(first) + len(replayed)
	execute(2)
	if n := h.exec.count(); n != 1 {
		t.Fatalf("the executor ran %d times on a later attempt, want 1", n)
	}
	third := h.bus.all()[before:]
	for _, e := range third {
		if e.Kind == envelope.KindText {
			t.Errorf("a memoized replay re-published the agent's output: seq %d", e.Seq)
		}
	}
	highest := uint64(0)
	for _, e := range h.bus.all()[:before] {
		if e.Seq > highest {
			highest = e.Seq
		}
	}
	for _, e := range third {
		if e.Seq <= highest {
			t.Errorf("attempt 2 published %s at seq %d, at or below attempt 1's high-water mark %d", e.Kind, e.Seq, highest)
		}
	}
}

// A run that parks is waiting, not failing: it must say so, and it must not be
// reported as terminal.
func TestAParkedRunPublishesParked(t *testing.T) {
	t.Parallel()

	exec := newExecutor("")
	exec.err = drun.ErrParked
	h := newHarness(t, exec)
	h.specs.revise("docs-bot", "v1")

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-park", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	h.work(t)
	waitFor(t, func() bool { return len(h.bus.ofKind(envelope.KindParked)) == 1 })

	if got := len(h.bus.ofKind(envelope.KindFailed)); got != 0 {
		t.Errorf("a parked run published %d failed events, want none", got)
	}
	parked := h.bus.ofKind(envelope.KindParked)[0]
	body, err := payload.Decode[payload.Lifecycle](parked)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Reason == "" {
		t.Error("the parked event carries no reason")
	}
	r, err := h.ledger.GetRun(ctx, acc.RunID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if r.Status.Terminal() {
		t.Errorf("the run is %s, want it still pending: parking is waiting", r.Status)
	}
}

// A revision is immutable by contract. One whose content hash moved under a
// pinned number is corruption, and executing it would silently run a definition
// the run never agreed to.
func TestAPinnedRevisionWhoseHashMovedFailsTerminally(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor("answered"))
	h.specs.revise("docs-bot", "v1")

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-hash", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	h.specs.corrupt("docs-bot", 1, "sha256:something-else")
	h.work(t)

	if _, err := h.rt.Result(ctx, acc.RunID); err == nil {
		t.Fatal("the run succeeded against a changed revision")
	}
	if n := h.exec.count(); n != 0 {
		t.Errorf("the executor ran %d times, want 0: the mismatch must be caught before the model", n)
	}
	failed := h.bus.ofKind(envelope.KindFailed)
	if len(failed) == 0 {
		t.Fatal("no failure was published")
	}
	body, err := payload.Decode[payload.Lifecycle](failed[0])
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !body.Terminal {
		t.Error("a changed revision was reported as retryable")
	}
	if !strings.Contains(body.Error, "immutable revision changed") {
		t.Errorf("failure reports %q, want it to name the mismatch", body.Error)
	}
}

// A tag naming an agent that does not exist is refused synchronously, rather than
// accepted and then failed: the caller is still on the phone.
func TestAnUnknownAgentIsRefusedAtAcceptTime(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor(""))
	if _, err := h.rt.Accept(context.Background(), tag("nobody", "Ev-x", "hello")); err == nil {
		t.Fatal("Accept succeeded for an unknown agent")
	}
	if got := len(h.bus.all()); got != 0 {
		t.Errorf("%d events were published for a refused tag, want none", got)
	}
	if got, err := h.ledger.GetRun(context.Background(), runtime.RunID("Ev-x")); err == nil {
		t.Errorf("a run %+v was accepted for an unknown agent", got)
	}
}

// A malformed tag never becomes a run.
func TestAMalformedTagIsRefused(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor(""))
	h.specs.revise("docs-bot", "v1")
	tests := []struct {
		name string
		tag  envelope.Tag
	}{
		{"no id", envelope.Tag{Agent: "docs-bot", Origin: "github"}},
		{"no agent", envelope.Tag{ID: "Ev1", Origin: "github"}},
		{"no origin", envelope.Tag{ID: "Ev1", Agent: "docs-bot"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := h.rt.Accept(context.Background(), tc.tag); !errors.Is(err, envelope.ErrInvalid) {
				t.Fatalf("Accept returned %v, want an envelope.ErrInvalid", err)
			}
		})
	}
}

// The journal is the run's own durable record of its stream, and it has to give
// events back, not a shape a reader has to guess at. envelope.Event does not
// survive JSON on its own, which is the reason Entry exists.
func TestJournalEntriesRoundTripBackIntoEvents(t *testing.T) {
	t.Parallel()

	tp, err := topic.Run("docs-bot", "run_1")
	if err != nil {
		t.Fatalf("topic.Run: %v", err)
	}
	want := envelope.Event{
		Seq: 1048577, Topic: tp, RunID: "run_1", Tenant: "acme", Agent: "docs-bot",
		Rev: 6, Origin: "github", Kind: envelope.KindText,
		Payload: []byte(`{"text":"hi"}`),
		At:      time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC),
	}

	raw, err := json.Marshal(runtime.EntryOf(want))
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	var entry runtime.Entry
	if err := json.Unmarshal(raw, &entry); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	got, err := entry.Event()
	if err != nil {
		t.Fatalf("Event: %v", err)
	}
	if got.Topic.String() != want.Topic.String() {
		t.Errorf("topic is %q, want %q", got.Topic, want.Topic)
	}
	if string(got.Payload) != string(want.Payload) {
		t.Errorf("payload is %s, want %s", got.Payload, want.Payload)
	}
	// Compare the rest field by field: an Event holds a topic and a payload,
	// neither of which is comparable with ==.
	if got.Seq != want.Seq || got.RunID != want.RunID || got.Tenant != want.Tenant ||
		got.Agent != want.Agent || got.Rev != want.Rev || got.Origin != want.Origin ||
		got.Kind != want.Kind || !got.At.Equal(want.At) {
		t.Errorf("event is %+v, want %+v", got, want)
	}
}

// A failure to publish is a failure of the turn: a run whose events nobody can
// see has not done its job, and the attempt must be retried rather than quietly
// completed.
func TestAPublishFailureFailsTheAttempt(t *testing.T) {
	t.Parallel()

	h := newHarness(t, newExecutor("answered"))
	h.specs.revise("docs-bot", "v1")
	var once sync.Once
	h.bus.failWhen(func(e envelope.Event) error {
		var err error
		if e.Kind == envelope.KindCompleted {
			once.Do(func() { err = errors.New("broker unreachable") })
		}
		return err
	})
	h.work(t)

	ctx := context.Background()
	acc, err := h.rt.Accept(ctx, tag("docs-bot", "Ev-pub", "hello"))
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if _, err := h.rt.Result(ctx, acc.RunID); err != nil {
		t.Fatalf("Result: %v", err)
	}
	// The attempt that could not announce completion was retried; the agent
	// itself was memoized and must not have run twice.
	if n := h.exec.count(); n != 1 {
		t.Errorf("the executor ran %d times, want 1: the model must not be paid for twice", n)
	}
	if got := len(h.bus.ofKind(envelope.KindCompleted)); got != 1 {
		t.Errorf("%d completed events landed, want 1", got)
	}
}

// ── helpers ─────────────────────────────────────────────────────────

func sameKinds(got, want []envelope.Kind) bool {
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

func fingerprint(events []envelope.Event) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, fmt.Sprintf("%d:%s:%s", e.Seq, e.Kind, e.Payload))
	}
	return out
}

func kindsOf(events []envelope.Event) []envelope.Kind {
	out := make([]envelope.Kind, 0, len(events))
	for _, e := range events {
		out = append(out, e.Kind)
	}
	return out
}

// waitFor polls cond until it holds or the test times out. The runtime hands work
// to a worker goroutine, so a test that asserts on an intermediate state has to
// wait for it rather than assume it.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for the run to reach the expected state")
		}
		time.Sleep(time.Millisecond)
	}
}
