// Package runtime is the durable turn: where a tag becomes a run.
//
// It is the composition root of the three libraries opentag is built on, and it
// exists because each of them owns exactly one thing and none of them owns this:
//
//	duraturo  the run survives a crash and replays from its ledger
//	dispatch  the turn executes inside a sandbox that enforces the agent's policy
//	saige     the turn is an agent loop (through pkg/agentrt)
//	pkg/bus   every event fans out live to observers and sinks
//
// # Idempotency
//
// A run's id is the tag's id (see RunID). Slack retries its event, GitHub
// redelivers its webhook, a cron fires twice on a leader flap — and duraturo
// treats starting a run that already exists as a no-op returning the existing
// run. A redelivery therefore JOINS the run it duplicated instead of starting a
// second one, and no component downstream has to deduplicate anything.
//
// # Pinning, and why it is the most important line in this package
//
// Accept resolves the agent's current revision ONCE and writes its number into
// the run's input. Every execution of that run, forever, resolves that number.
// Nothing inside the workflow body ever asks for "latest".
//
// This is not a nicety. duraturo replays a workflow from the top: on the second
// attempt the body re-executes and its recorded calls are matched by name and
// input hash. If the body resolved "latest" and someone revised the agent
// between attempts, the replayed call would carry a different input, the hashes
// would not match, and duraturo would fail the run with ErrNonDeterministic —
// after the model had already been paid for the first attempt. Worse, if it did
// somehow proceed, half a conversation would be answered by one definition and
// half by another, and the transcript would be a lie.
//
// The pinned revision's content hash travels with it, so a revision whose
// content changed under a pinned number fails loudly instead of executing
// something the run never agreed to.
//
// # Every event is published twice, to two different systems
//
//	duraturo.Emit    the run's own durable journal: replayable, per-run,
//	                 attempt-tagged, and readable by the engine that owns the
//	                 run. It survives the process. It is how an interrupted
//	                 stream can be reconstructed and how a resuming attempt can
//	                 see what a previous one already said.
//	bus.Publish      the live cross-run bus: ordered per run, fanned out to
//	                 every prefix subscriber, filtered per kind, and delivered
//	                 to routes. It is the product surface.
//
// Neither substitutes for the other. The journal is scoped to one run and known
// only to its engine, so a dashboard watching every agent cannot read it. The bus
// is a broker with retention, so it is not a durable record of what a run did.
// Emit is best-effort here (it is a no-op outside a run and when the queue has no
// delta log), while a failed Publish fails the turn: a run whose events nobody
// can see has not done its job.
//
// # Single goroutine
//
// duraturo v1 workflow bodies are single-goroutine. Nothing in this package
// forks the workflow, and every duraturo call — Call, Emit — happens on the
// goroutine the worker invoked. Parallelism lives inside the dispatch task,
// where it cannot confuse replay.
package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/urmzd/duraturo"
	"github.com/urmzd/duraturo/pkg/run"

	"github.com/urmzd/opentag/pkg/agentrt"
	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/topic"
)

// Activity names. They are the correlation contract with the ledger: records
// belong to a name, so a breaking change to an input or output type is a new
// name, never a redefinition of an existing one.
const (
	// ActivityTurn is the run's entry point: one durable turn.
	ActivityTurn = "opentag.turn"
	// ActivityAgent is the nested activity that executes the agent. It is
	// separate from the turn so that its result is memoized on its own: a
	// replay of the turn re-publishes lifecycle events but must never pay for
	// the model a second time.
	ActivityAgent = "opentag.turn.agent"
)

// ErrInvalid reports a tag or configuration this package cannot run.
var ErrInvalid = errors.New("runtime: invalid")

// Bus is the live fan-out this package publishes to. It is the publishing half
// of pkg/bus.Bus, narrowed to what a run needs: a run produces events and never
// subscribes to them.
type Bus interface {
	Publish(ctx context.Context, e envelope.Event) error
}

// Specs resolves agent definitions. The control plane implements it.
//
// The two methods exist for the two moments that matter. Latest is called once,
// at accept time, and is the only place "current" is ever asked for. At is called
// on every execution of a run with the number that was pinned, and must return
// the same immutable revision forever — including after the agent has been
// revised past it, and including after it has been deleted, or a run in flight
// would be unable to finish.
type Specs interface {
	Latest(ctx context.Context, tenant, agent string) (agentrt.Revision, error)
	At(ctx context.Context, tenant, agent string, rev int) (agentrt.Revision, error)
}

// Executor runs one turn in isolation and streams what it produces.
//
// It is an interface because isolation is a deployment decision: Sandbox runs
// the turn as a dispatch task under the pinned revision's NGAC policy, and that
// is the implementation a deployment should use. A caller that has already
// isolated its process differently can supply another.
//
// emit is called on the caller's goroutine, in order. Returning an error from it
// aborts the turn.
type Executor interface {
	Execute(ctx context.Context, req Request, emit func(context.Context, agentrt.Chunk) error) (Outcome, error)
}

// Input is a run's durable input: the tag, plus the revision pinned to it.
//
// It must be byte-identical on every attempt, because duraturo hashes it to
// match a replayed call against its record. Nothing derived from the moment of
// execution may appear here — not a timestamp, not an attempt number, not a
// sequence number. The attempt-varying values a turn needs are read from the
// replay frame instead (see duraturo.FromContext).
type Input struct {
	// Tag is the request as the trigger raised it.
	Tag envelope.Tag `json:"tag"`
	// Rev is the revision pinned at accept time. It never changes.
	Rev int `json:"rev"`
	// Hash is the pinned revision's content hash, or empty if the control
	// plane does not compute one. When present, it is verified before the
	// agent runs.
	Hash string `json:"hash,omitempty"`
}

// Output is a run's durable result.
type Output struct {
	// Rev is the revision that executed, which is always Input.Rev.
	Rev int `json:"rev"`
	// Text is the agent's final answer.
	Text string `json:"text"`
	// LastSeq is the sequence of the run's completed event: the cursor a
	// consumer passes back as Subscription.From to resume after this run.
	LastSeq uint64 `json:"last_seq"`
	// Attempt is the attempt that completed the run.
	Attempt int `json:"attempt"`
	// Events counts what the run published, the accepted event included.
	Events int `json:"events"`
	// Stored is the workspace key the answer artifact was written to, empty
	// when the pinned revision granted no workspace area for it.
	Stored string `json:"stored,omitempty"`
}

// Request is one turn handed to an Executor: the tag's content, plus the exact
// definition to run it with.
//
// Revision is a resolved, pinned value rather than a name and a number. An
// executor must not re-resolve it: it may be running in a sandbox, in another
// process, or on another machine, and "the definition to run" is not a question
// it is allowed to answer.
type Request struct {
	// RunID is the durable run. It is also the stream key: chunks a Sandbox
	// tool produces find their way back to this run by it.
	RunID string `json:"run_id"`
	// Tenant is the authorization scope carried from the tag.
	Tenant string `json:"tenant,omitempty"`
	// Attempt is the execution attempt, from 1.
	Attempt int `json:"attempt"`
	// Origin is the trigger that raised the tag.
	Origin string `json:"origin,omitempty"`
	// Source is the address URI of the surface the tag came from, and the
	// target connector actions are bound to. Empty for a trigger with no
	// surface, such as a schedule.
	Source string `json:"source,omitempty"`
	// Text is the request.
	Text string `json:"text"`
	// Meta is trigger context.
	Meta map[string]string `json:"meta,omitempty"`
	// Revision is the pinned definition to execute.
	Revision agentrt.Revision `json:"revision"`
	// Service is set by the Sandbox: the dispatch deployment this turn runs
	// in, so a delegated sub-task can be awaited in the right place.
	Service string `json:"service,omitempty"`
}

// Agent returns the agent name.
func (r Request) Agent() string { return r.Revision.Spec.Name }

// Rev returns the pinned revision number.
func (r Request) Rev() int { return r.Revision.Rev }

// Outcome is what an Executor produced.
type Outcome struct {
	// Text is the agent's final answer.
	Text string `json:"text"`
	// Chunks counts the event bodies produced.
	Chunks int `json:"chunks"`
	// Citations counts the retrieved passages the turn was grounded in.
	Citations int `json:"citations,omitempty"`
	// Stored is the workspace key the answer was written to.
	Stored string `json:"stored,omitempty"`
	// StoreError explains why the answer was not stored. A policy denial here
	// is not a turn failure: it means the pinned revision was not granted that
	// workspace area, which is the sandbox working as defined.
	StoreError string `json:"store_error,omitempty"`
}

// Acceptance is what a caller learns when a tag is accepted. It mirrors
// opentag.v1.InvokeResponse: the caller never builds a topic itself.
type Acceptance struct {
	RunID string
	// Rev is the revision this call resolved. For the delivery that created
	// the run it is the pinned revision. For a redelivery it is whatever is
	// current now, which may be higher than what the run actually pinned:
	// Accept must announce before it submits (see below), so at the moment it
	// resolves it cannot yet know the run already exists. The run's own events
	// always carry the pinned revision — read Rev off the stream, or off the
	// run record, if the distinction matters.
	Rev        int
	Topic      topic.Topic
	AcceptedAt time.Time
}

// Entry is one event in a run's durable journal, as duraturo.Emit records it.
//
// It exists because envelope.Event does not survive JSON: topic.Topic keeps its
// segments unexported so a malformed topic cannot be constructed, which is the
// right trade for the wire contract and the wrong shape for a journal. Entry
// carries the topic as its string form and converts both ways, so anything
// reading the journal — a resuming attempt, an operator, a bridge onto another
// transport — gets events back rather than a shape it has to guess at.
type Entry struct {
	Seq     uint64          `json:"seq"`
	Topic   string          `json:"topic"`
	RunID   string          `json:"run_id"`
	Tenant  string          `json:"tenant,omitempty"`
	Agent   string          `json:"agent"`
	Rev     int             `json:"rev"`
	Origin  string          `json:"origin,omitempty"`
	Kind    envelope.Kind   `json:"kind"`
	Payload json.RawMessage `json:"payload,omitempty"`
	At      time.Time       `json:"at"`
}

// EntryOf projects an event into a journal entry.
func EntryOf(e envelope.Event) Entry {
	return Entry{
		Seq: e.Seq, Topic: e.Topic.String(), RunID: e.RunID, Tenant: e.Tenant,
		Agent: e.Agent, Rev: e.Rev, Origin: e.Origin, Kind: e.Kind,
		Payload: json.RawMessage(e.Payload), At: e.At,
	}
}

// Event rebuilds an event from a journal entry.
func (n Entry) Event() (envelope.Event, error) {
	t, err := topic.Parse(n.Topic)
	if err != nil {
		return envelope.Event{}, fmt.Errorf("runtime: journal entry seq %d: %w", n.Seq, err)
	}
	return envelope.Event{
		Seq: n.Seq, Topic: t, RunID: n.RunID, Tenant: n.Tenant, Agent: n.Agent,
		Rev: n.Rev, Origin: n.Origin, Kind: n.Kind, Payload: n.Payload, At: n.At,
	}, nil
}

// Runtime accepts tags and runs them durably.
type Runtime struct {
	client *duraturo.Client
	specs  Specs
	bus    Bus
	exec   Executor
	stride uint64
	logger *slog.Logger
	now    func() time.Time

	registry *run.Registry
	turn     *duraturo.ActivityFn[Input, Output]
	agent    *duraturo.ActivityFn[Input, Outcome]
}

// Option configures a Runtime.
type Option func(*Runtime)

// WithStride sets the per-attempt sequence band size (default DefaultStride).
func WithStride(n uint64) Option {
	return func(r *Runtime) {
		if n > 2 {
			r.stride = n
		}
	}
}

// WithLogger sets the logger (default slog.Default()).
func WithLogger(l *slog.Logger) Option {
	return func(r *Runtime) {
		if l != nil {
			r.logger = l
		}
	}
}

// WithClock replaces the clock used to stamp events. Event timestamps are not
// part of replay determinism — the bus keeps the first write of a sequence, so a
// re-published event keeps its original stamp — but a test that asserts on order
// wants a clock it controls.
func WithClock(now func() time.Time) Option {
	return func(r *Runtime) {
		if now != nil {
			r.now = now
		}
	}
}

// New composes a runtime.
//
// The durable activities are registered in a registry OWNED BY THIS INSTANCE
// rather than duraturo's default one. Two runtimes in one process (a test suite,
// a fleet serving two tenants' agents with different executors) would otherwise
// panic on a duplicate activity name at construction. Pass Registry() to the
// worker that consumes these runs.
func New(client *duraturo.Client, specs Specs, b Bus, exec Executor, opts ...Option) (*Runtime, error) {
	switch {
	case client == nil:
		return nil, fmt.Errorf("%w: no duraturo client", ErrInvalid)
	case specs == nil:
		return nil, fmt.Errorf("%w: no spec source; a run cannot pin a revision it cannot resolve", ErrInvalid)
	case b == nil:
		return nil, fmt.Errorf("%w: no bus; a run whose events nobody can see has not done its job", ErrInvalid)
	case exec == nil:
		return nil, fmt.Errorf("%w: no executor", ErrInvalid)
	}
	r := &Runtime{
		client:   client,
		specs:    specs,
		bus:      b,
		exec:     exec,
		stride:   DefaultStride,
		logger:   slog.Default(),
		now:      time.Now,
		registry: run.NewRegistry(),
	}
	for _, o := range opts {
		o(r)
	}
	r.turn = duraturo.ActivityIn(r.registry, ActivityTurn, r.runTurn)
	r.agent = duraturo.ActivityIn(r.registry, ActivityAgent, r.runAgent)
	return r, nil
}

// Registry returns the activity registry this runtime's runs are resolved
// against. Hand it to the worker:
//
//	w := worker.New(ledger, queue, worker.WithRegistry(rt.Registry()))
func (r *Runtime) Registry() *run.Registry { return r.registry }

// Accept pins a revision to the tag and durably submits the run.
//
// Order matters, and it is not the obvious one. The revision is resolved first,
// so a tag naming an unknown agent is refused rather than accepted and then
// failed. The accepted event is published SECOND, before the run is submitted,
// because submitting makes the run claimable and a fast worker would otherwise
// publish started first — and the bus treats a sequence at or below a run's
// high-water mark as an already-seen event, so an accepted event that lost that
// race would be dropped rather than delivered late. Events must reach the bus in
// ascending sequence order, and sequence 1 belongs to accepted.
//
// The cost of that order is a tag that is announced and then fails to submit. It
// is the cheaper failure: the caller gets the error synchronously and retries the
// same tag, which lands on the same run id and the same sequence 1, so the
// announcement it already made is the announcement it makes again.
//
// Accept is idempotent on Tag.ID: a redelivery cannot change what the run pinned,
// because duraturo returns the existing run untouched and the bus keeps the first
// write of sequence 1. (A redelivery does re-resolve the current revision for its
// own accepted event, which the bus then discards. The run's own events all
// report the pinned revision, which is the one that matters.)
func (r *Runtime) Accept(ctx context.Context, tag envelope.Tag) (Acceptance, error) {
	if err := tag.Validate(); err != nil {
		return Acceptance{}, fmt.Errorf("runtime: accept: %w", err)
	}
	runID := RunID(tag.ID)
	tp, err := topic.Run(tag.Agent, runID)
	if err != nil {
		return Acceptance{}, fmt.Errorf("runtime: accept %s: %w", tag.ID, err)
	}
	rev, err := r.specs.Latest(ctx, tag.Tenant, tag.Agent)
	if err != nil {
		return Acceptance{}, fmt.Errorf("runtime: accept %s: resolve agent %q: %w", tag.ID, tag.Agent, err)
	}
	if err := rev.Validate(); err != nil {
		return Acceptance{}, fmt.Errorf("runtime: accept %s: %w", tag.ID, err)
	}

	in := Input{Tag: tag, Rev: rev.Rev, Hash: rev.Hash}
	acc := Acceptance{RunID: runID, Rev: rev.Rev, Topic: tp, AcceptedAt: r.now().UTC()}

	e := r.event(in, SeqAccepted, envelope.KindAccepted)
	body, err := payload.Encode(payload.Lifecycle{Rev: rev.Rev})
	if err != nil {
		return Acceptance{}, fmt.Errorf("runtime: accept %s: %w", tag.ID, err)
	}
	e.Payload = body
	if err := r.bus.Publish(ctx, e); err != nil {
		return Acceptance{}, fmt.Errorf("runtime: accept %s: publish accepted: %w", tag.ID, err)
	}

	if _, err := duraturo.Start(ctx, r.client, r.turn, in, duraturo.WithRunID(runID)); err != nil {
		// Start returns a handle alongside an enqueue failure: the run is
		// durable and the janitor will drive it anyway. Report the error —
		// the caller asked for an acknowledgement it did not fully get — but
		// return the acceptance too, so it can observe the run it now owns.
		return acc, fmt.Errorf("runtime: accept %s: %w", tag.ID, err)
	}
	return acc, nil
}

// Result blocks until a run is terminal and returns its output.
func (r *Runtime) Result(ctx context.Context, runID string) (Output, error) {
	return duraturo.HandleFor[Output](r.client, runID).Result(ctx)
}

// runTurn is the workflow body: one durable turn.
//
// It publishes the run's lifecycle and delegates the work. Everything in it is
// either derived from the pinned input or read from the replay frame, so a
// replay produces the same decisions; nothing here resolves, reads a clock for a
// decision, or forks.
func (r *Runtime) runTurn(ctx context.Context, in Input) (Output, error) {
	attempt := attemptOf(ctx)
	b, err := newBand(r.stride, attempt)
	if err != nil {
		return Output{}, duraturo.NonRetryable(err)
	}

	// Attempt 1 started the work; every later attempt resumed it, whether it
	// was parked on an event or interrupted by a crash. The two are one fact
	// for a subscriber — this run is executing again — and the attempt number
	// in the payload is what distinguishes them for anyone who cares.
	opened := envelope.KindStarted
	if attempt > 1 {
		opened = envelope.KindResumed
	}
	if err := r.publish(ctx, in, b.open, opened, payload.Lifecycle{Attempt: attempt, Rev: in.Rev}); err != nil {
		return Output{}, err
	}

	out, err := r.agent.Call(ctx, in)
	switch {
	case errors.Is(err, run.ErrParked):
		// The run stopped at an event that has no record yet. It is not
		// failed and it has not consumed retry budget; it waits.
		r.report(ctx, in, b.close, envelope.KindParked, payload.Lifecycle{
			Attempt: attempt, Rev: in.Rev, Reason: err.Error(),
		})
		return Output{}, err

	case err != nil:
		// Retry budget belongs to duraturo, not here, so terminality is
		// reported as what this package can actually know: a non-retryable
		// error will not be attempted again, anything else may be. A run that
		// later exhausts its budget has its last failure on the bus marked
		// non-terminal; the ledger stays the authority on run status.
		r.report(ctx, in, b.close, envelope.KindFailed, payload.Lifecycle{
			Attempt: attempt, Rev: in.Rev, Error: err.Error(),
			Terminal: run.IsNonRetryable(err) || errors.Is(err, run.ErrNonDeterministic),
		})
		return Output{}, err
	}

	if err := r.publish(ctx, in, b.close, envelope.KindCompleted, payload.Lifecycle{
		Attempt: attempt, Rev: in.Rev, Text: out.Text,
	}); err != nil {
		return Output{}, err
	}
	return Output{
		Rev:     in.Rev,
		Text:    out.Text,
		LastSeq: b.close,
		Attempt: attempt,
		// accepted, opened, the agent's chunks, and completed.
		Events: out.Chunks + 3,
		Stored: out.Stored,
	}, nil
}

// runAgent is the nested activity that executes the agent.
//
// It is where the pinned revision is resolved — by number, never by "latest" —
// and where the agent's events are published as they stream. Its result is
// memoized, which is the whole reason it is a separate activity: a replay of the
// turn must re-publish the run's lifecycle without paying for the model again.
func (r *Runtime) runAgent(ctx context.Context, in Input) (Outcome, error) {
	rev, err := r.pinned(ctx, in)
	if err != nil {
		return Outcome{}, err
	}
	attempt := attemptOf(ctx)
	b, err := newBand(r.stride, attempt)
	if err != nil {
		return Outcome{}, duraturo.NonRetryable(err)
	}

	req := Request{
		RunID:    RunID(in.Tag.ID),
		Tenant:   in.Tag.Tenant,
		Attempt:  attempt,
		Origin:   in.Tag.Origin,
		Source:   in.Tag.Source.String(),
		Text:     in.Tag.Text,
		Meta:     in.Tag.Meta,
		Revision: rev,
	}

	out, err := r.exec.Execute(ctx, req, func(ctx context.Context, c agentrt.Chunk) error {
		seq, err := b.delta()
		if err != nil {
			// The band is spent. Retrying would produce the same flood, so
			// this is terminal rather than another paid attempt.
			return duraturo.NonRetryable(err)
		}
		return r.publishRaw(ctx, in, seq, c.Kind, c.Payload)
	})
	if err != nil {
		return out, fmt.Errorf("runtime: run %s: agent %q revision %d: %w", req.RunID, rev.Spec.Name, rev.Rev, err)
	}
	return out, nil
}

// pinned resolves the run's pinned revision and refuses anything that is not it.
//
// The hash check is the second half of pinning. A revision is immutable by
// contract, so a revision whose content hash no longer matches the one the run
// pinned is either corruption or a control plane that mutated history. Executing
// it would silently run a definition the run never agreed to, so it fails
// terminally: replaying identical code against identical records cannot fix it.
func (r *Runtime) pinned(ctx context.Context, in Input) (agentrt.Revision, error) {
	rev, err := r.specs.At(ctx, in.Tag.Tenant, in.Tag.Agent, in.Rev)
	if err != nil {
		return agentrt.Revision{}, fmt.Errorf("runtime: resolve agent %q revision %d: %w", in.Tag.Agent, in.Rev, err)
	}
	if err := rev.Validate(); err != nil {
		return agentrt.Revision{}, duraturo.NonRetryable(err)
	}
	if rev.Rev != in.Rev {
		return agentrt.Revision{}, duraturo.NonRetryable(fmt.Errorf(
			"%w: run pinned agent %q revision %d but the control plane returned revision %d",
			ErrInvalid, in.Tag.Agent, in.Rev, rev.Rev))
	}
	if in.Hash != "" && rev.Hash != "" && in.Hash != rev.Hash {
		return agentrt.Revision{}, duraturo.NonRetryable(fmt.Errorf(
			"%w: agent %q revision %d hash is %s, but this run pinned %s; an immutable revision changed",
			ErrInvalid, in.Tag.Agent, in.Rev, rev.Hash, in.Hash))
	}
	return rev, nil
}

// publish sends one event to both the live bus and the run's durable journal.
func (r *Runtime) publish(ctx context.Context, in Input, seq uint64, kind envelope.Kind, body any) error {
	b, err := payload.Encode(body)
	if err != nil {
		return fmt.Errorf("runtime: publish %s: %w", kind, err)
	}
	return r.publishRaw(ctx, in, seq, kind, b)
}

// publishRaw is publish for a body that is already marshalled: the agent's
// chunks arrive that way and re-encoding them would be a round trip for nothing.
//
// The bus comes first and its failure fails the turn. The journal is written
// after and best-effort: duraturo.Emit is a no-op outside a run and when the
// queue has no delta log, and a journal that lost an entry costs observability,
// never correctness — the ledger holds the truth. The one Emit error that is not
// advisory is ErrSuperseded: it means another attempt owns this run now, and a
// zombie that keeps publishing would interleave two attempts' output.
func (r *Runtime) publishRaw(ctx context.Context, in Input, seq uint64, kind envelope.Kind, body []byte) error {
	e := r.event(in, seq, kind)
	e.Payload = body
	if err := r.bus.Publish(ctx, e); err != nil {
		return fmt.Errorf("runtime: publish %s seq %d on %s: %w", kind, seq, e.Topic, err)
	}
	if err := duraturo.Emit(ctx, EntryOf(e)); err != nil {
		if errors.Is(err, run.ErrSuperseded) {
			return fmt.Errorf("runtime: journal %s seq %d: %w", kind, seq, err)
		}
		r.logger.Warn("opentag/runtime: journal append failed; the bus has the event",
			"run_id", e.RunID, "seq", seq, "kind", string(kind), "error", err)
	}
	return nil
}

// report publishes an event whose failure must not mask the failure it reports.
// A parked or failed turn already has an error on its way up; losing the
// announcement of it is worth a log line, not a different error.
func (r *Runtime) report(ctx context.Context, in Input, seq uint64, kind envelope.Kind, body any) {
	if err := r.publish(ctx, in, seq, kind, body); err != nil {
		r.logger.Warn("opentag/runtime: could not publish lifecycle event",
			"run_id", RunID(in.Tag.ID), "kind", string(kind), "seq", seq, "error", err)
	}
}

// event builds the identity every event of a run shares. The topic is the most
// specific form, always: publishing to agent:<name>:<run> is what lets a
// subscriber on any prefix of it receive the event.
func (r *Runtime) event(in Input, seq uint64, kind envelope.Kind) envelope.Event {
	runID := RunID(in.Tag.ID)
	tp, err := topic.Run(in.Tag.Agent, runID)
	if err != nil {
		// Accept validated this topic before the run existed, so a failure
		// here is impossible; the zero topic makes the bus reject the event
		// rather than publishing it somewhere unintended.
		r.logger.Error("opentag/runtime: run topic is invalid", "agent", in.Tag.Agent, "run_id", runID, "error", err)
	}
	return envelope.Event{
		Seq:    seq,
		Topic:  tp,
		RunID:  runID,
		Tenant: in.Tag.Tenant,
		Agent:  in.Tag.Agent,
		Rev:    in.Rev,
		Origin: in.Tag.Origin,
		Kind:   kind,
		At:     r.now().UTC(),
	}
}

// attemptOf reports the execution attempt, from 1.
//
// It reads the replay frame rather than the input, because the attempt is the
// one attempt-varying value a deterministic workflow is allowed to see: it is
// not part of the input and cannot change what the input hashes to. Outside a
// run there is exactly one attempt.
func attemptOf(ctx context.Context) int {
	info, ok := duraturo.FromContext(ctx)
	if !ok || info.Attempt < 1 {
		return 1
	}
	return info.Attempt
}

// RunID is the run identity a tag maps to.
//
// A tag's id is the trigger's own event identity — a Slack event_id, a GitHub
// delivery GUID, a schedule occurrence — and using it as the run id is what
// makes a redelivery join the existing run instead of starting a second one.
//
// It is also a topic segment, and topics are restricted to letters, digits, "-",
// and "_" so that they stay safe to embed in stream keys and URLs unescaped. A
// tag id that does not fit is hashed rather than rewritten: sanitizing by
// dropping characters would map two distinct ids onto one run, which is the one
// failure mode idempotency cannot tolerate. The hash is deterministic, so a
// redelivery of an awkward id still lands on the same run.
func RunID(tagID string) string {
	if _, err := topic.Run("probe", tagID); err == nil {
		return tagID
	}
	sum := sha256.Sum256([]byte(tagID))
	return "r" + hex.EncodeToString(sum[:12])
}
