// Command local is opentag, end to end, in one process.
//
//	go run ./examples/local
//
// No API key. No network. No Postgres, no Redis, no Docker. Everything a
// deployment would run against real infrastructure runs here against the
// in-memory implementations of the same interfaces:
//
//	control plane   pkg/registry.Memory        (a real registry would be Postgres)
//	bus             pkg/bus.Memory             (a real bus would be Redis Streams)
//	ledger + queue  duraturo's ledger.Memory   (a real ledger would be Postgres)
//	                and queue.Memory
//	model           saige's agenttest.ScriptedProvider, reached through the
//	                agent's own "offline" provider — the agent loop, the typed
//	                deltas and the translation to events are all the real ones
//	connectors      two fakes in fakes.go, implementing connector.Sink exactly
//
// None of those are stubs standing in for the design. They are the design,
// with the storage swapped. The seven sections below are the seven claims
// opentag makes, each one demonstrated rather than described.
//
//  1. an agent is a declarative spec
//  2. a revision is immutable; revising creates the next one
//  3. origin and destination are independent   ← the mesh thesis
//  4. one run, two sinks, two granularities    ← N:M selectivity
//  5. a third party can listen to an agent     ← "the Kafka of agents"
//  6. citations are first-class events with resolvable provenance
//  7. a replayed run does not re-execute a recorded activity ← durability
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/urmzd/duraturo"
	"github.com/urmzd/duraturo/pkg/ledger"
	dqueue "github.com/urmzd/duraturo/pkg/queue"
	"github.com/urmzd/duraturo/pkg/worker"
	ragtypes "github.com/urmzd/saige/rag/types"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/agentspec"
	"github.com/urmzd/opentag/pkg/bus"
	"github.com/urmzd/opentag/pkg/connector"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/registry"
	"github.com/urmzd/opentag/pkg/router"
	"github.com/urmzd/opentag/pkg/runtime"
	"github.com/urmzd/opentag/pkg/topic"
)

// tenant is the authorization scope. It comes from the caller's credential in
// a real deployment and is never on the wire and never a topic segment: an
// agent is addressed as agent:docs-bot, not agent:acme:docs-bot.
const tenant = "acme"

// author is who the control plane attributes these definitions to.
const author = "ada@acme.test"

// demoStride shrinks the per-attempt sequence band from its default of 2^20 so
// that the numbers in the output stay readable. Every attempt of a run gets its
// own band — attempt 1 opens at 101, attempt 2 at 201 — which is what lets a
// retry append its real output after an interrupted attempt's partial output
// instead of vanishing behind the bus's high-water mark. Section 7 shows it.
const demoStride = 100

// quiet sends every library log line to /dev/null. The story here is told by
// fmt.Println, and interleaving it with operational logging would bury it.
// Section 7 deliberately induces a failure, so this also suppresses the very
// real error the router and worker log about it.
var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fixedClock stamps events so two runs of this example print the same thing.
var fixedClock = func() time.Time { return time.Date(2026, 7, 26, 9, 30, 0, 0, time.UTC) }

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "\nexample failed: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(quiet)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	banner()

	// ── composition ─────────────────────────────────────────────────
	//
	// This is the whole composition root. Each of these owns exactly one
	// concern, and the wiring below is the only place they meet.

	// The control plane: append-only agent definitions with revisions.
	store := registry.NewMemory(registry.WithClock(fixedClock))

	// The broker. Runs publish here; everything else subscribes. flakyBus
	// wraps it for section 7 and forwards Subscribe untouched, so the router
	// and the observer below read the real broker.
	broker := bus.NewMemory()
	published := newFlakyBus(broker)

	// The connectors. Roles are DERIVED from the interfaces a connector
	// implements, never declared: both of these implement Deliver and neither
	// implements Ingest, so the registry knows them as sinks only.
	sinks := connector.NewRegistry()
	surfaces := &transcript{}
	slack := newSlackSink(surfaces)
	hook := newWebhookSink(surfaces)
	for _, c := range []connector.Connector{slack, hook} {
		if err := sinks.Register(c); err != nil {
			return fmt.Errorf("register %s: %w", c.Name(), err)
		}
	}

	// The router: another bus subscriber, which is the reason origin and
	// destination stay independent. Nothing on the path from trigger to sink
	// ever holds both ends at once.
	deliver := router.New(broker, sinks, router.WithLogger(quiet))
	routerDone := make(chan error, 1)
	go func() { routerDone <- deliver.Run(ctx) }()

	// The agent side: a corpus to ground answers in, and an executor that
	// builds the agent from a pinned revision and runs it.
	docs := corpus{blocks: []ragtypes.ContextBlock{
		{
			Text:     "Merging to main runs the release workflow: build, sign, upload, then open a draft release.",
			Citation: "[1]",
			Provenance: ragtypes.Provenance{
				DocumentUUID:   "doc-release-pipeline",
				DocumentTitle:  "Release pipeline",
				SourceURI:      "https://docs.acme.test/release-pipeline#on-merge",
				SectionUUID:    "sec-on-merge",
				SectionHeading: "On merge to main",
				SectionIndex:   2,
			},
		},
		{
			Text:     "Rollback is a re-run of the previous tag's workflow; never revert the release commit by hand.",
			Citation: "[2]",
			Provenance: ragtypes.Provenance{
				DocumentUUID:   "doc-runbook",
				DocumentTitle:  "Deploy runbook",
				SourceURI:      "https://docs.acme.test/runbook#rollback",
				SectionUUID:    "sec-rollback",
				SectionHeading: "Rollback",
				SectionIndex:   7,
			},
		},
	}}

	exec := newOfflineExecutor(docs)
	exec.script("docs-bot", streamed(
		"The release workflow runs on merge to main [1]. ",
		"It builds, signs and uploads the archive, then opens a draft release ",
		"for a human to publish. ",
		"To roll back, re-run the previous tag's workflow rather than reverting by hand [2].",
	))

	// Durable execution: the ledger is truth, the queue is disposable flow.
	lgr := ledger.NewMemory()
	queue := dqueue.NewMemory()

	// The durable turn, where a tag becomes a run. registry.NewSpecs adapts
	// the control plane to what the runtime needs: Latest, called once at
	// accept time, and At, called with the pinned number on every execution
	// forever.
	rt, err := runtime.New(
		duraturo.New(lgr, queue),
		registry.NewSpecs(store),
		published,
		exec,
		runtime.WithStride(demoStride),
		runtime.WithClock(fixedClock),
		runtime.WithLogger(quiet),
	)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}

	// The worker is a pull loop on a goroutine, not a service to deploy. In
	// production it is a separate process reading the same ledger.
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		_ = worker.New(lgr, queue,
			worker.WithRegistry(rt.Registry()),
			worker.WithConcurrency(1),
			worker.WithJanitorEvery(0),
			worker.WithLeaseTTL(2*time.Second),
			worker.WithHeartbeatEvery(200*time.Millisecond),
			worker.WithBackoff(func(int) time.Duration { return 10 * time.Millisecond }),
			worker.WithLogger(quiet),
		).Run(ctx)
	}()

	// ── 1 ───────────────────────────────────────────────────────────
	section(1, "AN AGENT IS A DECLARATIVE SPEC")

	first := agentspec.AgentSpec{
		Name:        "docs-bot",
		Description: "answers questions about how we ship",
		// Change these two fields to "anthropic" and a real model id and
		// every other line in this file stays the same. That is the only
		// difference between this example and a production deployment's
		// agent definition.
		Provider:     agentspec.ProviderOffline,
		Model:        "scripted",
		SystemPrompt: "You answer questions about the deploy pipeline. Cite your sources.",
		Sources: []agentspec.Source{
			{Name: "handbook", URI: "https://docs.acme.test/"},
		},
	}

	rev1, err := store.Create(ctx, tenant, first, author)
	if err != nil {
		return fmt.Errorf("create agent: %w", err)
	}
	fmt.Printf("  created  %s  revision %d\n", rev1.Spec.Name, rev1.Rev)
	fmt.Printf("  hash     %s\n", rev1.Hash)
	fmt.Printf("  prompt   %q\n", rev1.Spec.SystemPrompt)
	fmt.Printf("  address  agent:%s          ← this is now a topic anyone can subscribe to\n", rev1.Spec.Name)

	// ── 2 ───────────────────────────────────────────────────────────
	section(2, "A REVISION IS IMMUTABLE; REVISING CREATES THE NEXT ONE")

	second := first
	second.SystemPrompt = "You answer questions about the deploy pipeline. " +
		"Cite your sources, and always mention the rollback procedure."

	// expectedRev is optimistic concurrency: say which revision you edited,
	// and a racing editor cannot silently overwrite you.
	rev2, err := store.Revise(ctx, tenant, second, rev1.Rev, author)
	if err != nil {
		return fmt.Errorf("revise agent: %w", err)
	}
	fmt.Printf("  revised  %s  revision %d\n\n", rev2.Spec.Name, rev2.Rev)

	// Both numbers still resolve, and revision 1 is byte-for-byte what it was
	// before the edit. This is not a nicety: a run PINS its revision at accept
	// time and replays under it forever, because duraturo replays a workflow
	// from the top and a definition that moved between attempts would make the
	// replay diverge from its own ledger.
	got1, err := store.Get(ctx, tenant, "docs-bot", 1)
	if err != nil {
		return fmt.Errorf("resolve revision 1: %w", err)
	}
	got2, err := store.Get(ctx, tenant, "docs-bot", 2)
	if err != nil {
		return fmt.Errorf("resolve revision 2: %w", err)
	}
	fmt.Printf("  rev 1    %s\n           %q\n", got1.Hash, got1.Spec.SystemPrompt)
	fmt.Printf("  rev 2    %s\n           %q\n\n", got2.Hash, got2.Spec.SystemPrompt)

	if got1.Hash != rev1.Hash {
		return fmt.Errorf("revision 1 changed under a revise: %s became %s", rev1.Hash, got1.Hash)
	}
	fmt.Println("  revision 1 still resolves, unchanged, after the edit that created 2.")
	fmt.Println("  a run in flight against revision 1 finishes as revision 1.")

	// ── 3 ───────────────────────────────────────────────────────────
	section(3, "ORIGIN AND DESTINATION ARE INDEPENDENT   ← the mesh thesis")

	// A tag is the canonical request every trigger translates into. This one
	// is what the GitHub connector would produce from an issue comment that
	// mentions @docs-bot; ID is GitHub's delivery GUID, which becomes the
	// run's idempotency key, so a webhook redelivery JOINS the run it
	// duplicated instead of starting a second one.
	tag := envelope.Tag{
		ID:     "gh-72d3162e-cc78",
		Tenant: tenant,
		Agent:  "docs-bot",
		Origin: "github",
		Source: address.MustParse("github://acme/handbook/issues/42"),
		Text:   "how does a release actually get out the door?",
		Actor:  envelope.Actor{ID: "U_ada", Display: "Ada"},
		At:     fixedClock(),

		// Deliver is where this run's events go. Neither of these is where
		// the tag came from, and neither knows about the other.
		Deliver: []envelope.Route{
			{
				// Every kind: this sink renders the live stream.
				Target: address.MustParse("slack://T01/C02?thread=1699.001"),
			},
			{
				// One kind: this sink only learns that the work landed.
				Target: address.MustParse("webhook://acme/deploys"),
				Kinds:  []string{"lifecycle.completed"},
			},
		},
		Meta: map[string]string{"issue": "42", "repo": "acme/handbook"},
	}

	fmt.Printf("  raised in   %-42s  (origin %q)\n", tag.Source, tag.Origin)
	fmt.Println("  answered in")
	for _, r := range tag.Routes() {
		kinds := "every kind"
		if len(r.Kinds) > 0 {
			kinds = strings.Join(r.Kinds, ", ")
		}
		fmt.Printf("              %-42s  %s\n", r.Target, kinds)
	}
	fmt.Println()
	fmt.Println("  the trigger is GitHub. neither destination is GitHub.")
	fmt.Println("  no component on this path holds both ends: the trigger writes a Tag,")
	fmt.Println("  the run publishes Events, and the router — just another subscriber —")
	fmt.Println("  renders them onto whatever the routes named. that indirection is the")
	fmt.Println("  difference between a mesh and a pile of integrations.")

	// The third party attaches BEFORE anything is accepted, on the agent
	// topic rather than the run topic. It will pick up this run and the next
	// one without ever being told either exists. Section 5 reads its log.
	watcher := &observer{}
	watching, err := watcher.watch(ctx, broker, envelope.Subscription{
		Topic: topic.MustParse("agent:docs-bot"),
	})
	if err != nil {
		return fmt.Errorf("observer subscribe: %w", err)
	}

	// ── 4 ───────────────────────────────────────────────────────────
	section(4, "ONE RUN, TWO SINKS, TWO GRANULARITIES   ← N:M selectivity")

	fmt.Println("  slack   takes every kind and edits one message as the answer arrives")
	fmt.Println("  webhook takes lifecycle.completed only and posts exactly once")
	fmt.Println()

	// The router learns a run's routes at accept time, before its first event
	// is published: an event that arrives for a run it has no routes for has
	// nowhere to go. RunID is a pure function of the tag id, so the routes can
	// be registered before the run exists.
	meshRun := runtime.RunID(tag.ID)
	if err := deliver.Register(meshRun, tag.Routes()); err != nil {
		return fmt.Errorf("register routes: %w", err)
	}

	// Accept pins the revision, publishes the accepted event, and submits the
	// run durably. From here the process could be killed and restarted and the
	// run would still finish.
	acc, err := rt.Accept(ctx, tag)
	if err != nil {
		return fmt.Errorf("accept tag: %w", err)
	}
	fmt.Printf("  accepted run %s on topic %s, pinned to revision %d\n\n",
		acc.RunID, acc.Topic, acc.Rev)

	out, err := rt.Result(ctx, acc.RunID)
	if err != nil {
		return fmt.Errorf("run %s: %w", acc.RunID, err)
	}
	// The run is terminal in the ledger; delivery is a separate, asynchronous
	// subscription, so wait for the surfaces to catch up before reporting.
	if err := await(ctx, "slack to render the run", func() bool {
		_, done := slack.delivered(meshRun)
		return done
	}); err != nil {
		return err
	}
	if err := await(ctx, "the webhook to be posted", func() bool {
		return hook.delivered(meshRun) > 0
	}); err != nil {
		return err
	}

	// Both surfaces are settled, so the transcript can be printed in the
	// order the events caused it. See the comment on transcript for why the
	// sinks record rather than print.
	fmt.Println("  what the two surfaces were shown, by the sequence that caused it:")
	fmt.Println()
	surfaces.flush()

	slackCount, _ := slack.delivered(meshRun)
	fmt.Println()
	fmt.Printf("  the run published %d events, sequences 1..%d.\n", out.Events, out.LastSeq)
	fmt.Printf("  slack   was delivered %d of them (every kind)\n", slackCount)
	fmt.Printf("  webhook was delivered %d of them (%s)\n", hook.delivered(meshRun), kindList(hook.seenKinds()))
	fmt.Println()
	fmt.Println("  same run, same bus, same events. selectivity is a field on the route,")
	fmt.Println("  not a separate topic and not a separate run — which is why adding the")
	fmt.Println("  webhook cost nothing on the agent side.")

	// ── 5 ───────────────────────────────────────────────────────────
	section(5, `A THIRD PARTY CAN LISTEN TO AN AGENT   ← "the Kafka of agents"`)

	if err := await(ctx, "the observer to see the run finish", func() bool {
		return watcher.sawKind(meshRun, envelope.KindCompleted)
	}); err != nil {
		return err
	}

	fmt.Println("  this subscriber holds one subscription to agent:docs-bot.")
	fmt.Println("  it did not raise the tag, no route names it, and the run cannot")
	fmt.Println("  tell it is there. it sees the whole stream anyway:")
	fmt.Println()
	fmt.Printf("    %-5s  %-22s  %s\n", "seq", "kind", "payload")
	fmt.Printf("    %-5s  %-22s  %s\n", "─────", "──────────────────────", "───────")
	for _, e := range watcher.events(meshRun) {
		fmt.Printf("    %-5d  %-22s  %s\n", e.Seq, e.Kind, summarize(e))
	}
	fmt.Println()
	fmt.Println("  seq 1 is accepted; 101 opens attempt 1's sequence band; 200 closes it.")
	fmt.Println("  Seq is also the resume cursor: hand the last one back as")
	fmt.Println("  Subscription.From and the stream continues with no gap and no repeat.")

	// ── 6 ───────────────────────────────────────────────────────────
	section(6, "CITATIONS ARE FIRST-CLASS EVENTS WITH RESOLVABLE PROVENANCE")

	cites := 0
	for _, e := range watcher.events(meshRun) {
		if e.Kind != envelope.KindCitation {
			continue
		}
		c, err := decodeCitation(e)
		if err != nil {
			return err
		}
		cites++
		fmt.Printf("  %s  seq %d\n", c.Label, e.Seq)
		fmt.Printf("      text     %q\n", c.Text)
		fmt.Printf("      document %s\n", c.Source.DocumentTitle)
		fmt.Printf("      section  %s (index %d)\n", c.Source.SectionHeading, c.Source.SectionIndex)
		fmt.Printf("      resolve  %s\n", c.Source.SourceURI)
	}
	if cites == 0 {
		return errors.New("no citation events were published; the retriever was not consulted")
	}
	fmt.Println()
	fmt.Println("  a citation is an event of kind delta.citation, on the same stream and")
	fmt.Println("  in the same order as the text — not an attachment on the final answer.")
	fmt.Println("  it is derived from what the turn RETRIEVED, not from what the model")
	fmt.Println("  claimed, so a reader can resolve the URI and check it.")

	// ── 7 ───────────────────────────────────────────────────────────
	section(7, "A REPLAYED RUN DOES NOT RE-EXECUTE A RECORDED ACTIVITY")

	// A second tag for the same agent, this time from a schedule. cron has no
	// surface to answer into and this tag declares no routes, so it runs and
	// publishes and delivers nowhere — which is legal, and is the reverse
	// asymmetry from the webhook, a sink that can never trigger.
	nightly := envelope.Tag{
		ID:     "nightly-2026-07-26",
		Tenant: tenant,
		Agent:  "docs-bot",
		Origin: "cron",
		Text:   "summarise how a release gets out the door",
		At:     fixedClock(),
	}
	cronRun := runtime.RunID(nightly.ID)

	// Arm one failure: the first attempt's lifecycle.completed will not reach
	// the bus. The runtime treats a failed publish as a failed turn — a run
	// whose events nobody can see has not done its job — so duraturo will
	// replay the turn. Everything the turn recorded before that point is
	// already in the ledger, including the ENTIRE agent execution.
	published.failOnce(cronRun, envelope.KindCompleted)

	before := exec.modelCalls()
	fmt.Printf("  model calls so far           %d\n", before)
	fmt.Println("  arming a broker outage on this run's lifecycle.completed event...")
	fmt.Println()

	if _, err := rt.Accept(ctx, nightly); err != nil {
		return fmt.Errorf("accept nightly: %w", err)
	}
	nightlyOut, err := rt.Result(ctx, cronRun)
	if err != nil {
		return fmt.Errorf("nightly run: %w", err)
	}
	if err := await(ctx, "the observer to see the nightly run finish", func() bool {
		return watcher.sawKind(cronRun, envelope.KindCompleted)
	}); err != nil {
		return err
	}

	fmt.Println("  what the third party saw, on the same subscription as before:")
	fmt.Println()
	fmt.Printf("    %-5s  %-22s  %s\n", "seq", "kind", "payload")
	fmt.Printf("    %-5s  %-22s  %s\n", "─────", "──────────────────────", "───────")
	for _, e := range watcher.events(cronRun) {
		fmt.Printf("    %-5d  %-22s  %s\n", e.Seq, e.Kind, summarize(e))
	}

	after := exec.modelCalls()
	fmt.Println()
	fmt.Printf("  attempts on this run         %d\n", nightlyOut.Attempt)
	fmt.Printf("  model calls after the replay %d   (was %d — the difference is %d)\n",
		after, before, after-before)
	fmt.Println()
	if after-before != 1 {
		return fmt.Errorf("the agent executed %d times across %d attempts; a recorded activity was re-executed",
			after-before, nightlyOut.Attempt)
	}
	fmt.Println("  read the stream above: attempt 1 published started (101) and six deltas")
	fmt.Println("  (102..107), then its completed event was lost and the turn failed.")
	fmt.Println("  attempt 2 re-executed the workflow body from the top and published")
	fmt.Println("  resumed (201) and completed (300) — and NO deltas, because the agent")
	fmt.Println("  activity was already recorded and was served from the ledger.")
	fmt.Println()
	fmt.Println("  that is the durability claim in one line: the workflow replays, the")
	fmt.Println("  model is not paid twice, and the subscriber sees a resumed event rather")
	fmt.Println("  than a run that went silent. each attempt gets its own sequence band,")
	fmt.Println("  so attempt 2's output appends after attempt 1's instead of being")
	fmt.Println("  discarded as already-seen.")

	// ── shutdown ────────────────────────────────────────────────────
	section(0, "WHAT JUST HAPPENED")

	fmt.Printf("  agents defined      1 (%d revisions, both still resolvable)\n", rev2.Rev)
	fmt.Printf("  runs executed       %d (one from GitHub, one from a schedule)\n", len(watcher.runs()))
	fmt.Printf("  events on the bus   %d (%d + %d, counted by a subscriber that invoked nothing)\n",
		watcher.total(), len(watcher.events(meshRun)), len(watcher.events(cronRun)))
	fmt.Printf("  surfaces delivered  slack %d, webhook %d\n", slackCount, hook.delivered(meshRun))
	fmt.Printf("  model calls         %d\n", after)
	fmt.Println()
	fmt.Println("  swap registry.Memory for Postgres, bus.Memory for Redis Streams,")
	fmt.Println("  ledger.Memory for duraturo's pgledger, the two fakes for")
	fmt.Println("  pkg/connectors/slack and pkg/connectors/webhook, and the executor for")
	fmt.Println("  runtime.NewSandbox — and this file does not otherwise change.")
	fmt.Println()

	cancel()
	<-workerDone
	<-watching
	if err := <-routerDone; err != nil {
		return fmt.Errorf("router: %w", err)
	}
	return nil
}

// ── output helpers ──────────────────────────────────────────────────

const rule = "──────────────────────────────────────────────────────────────────────────"

func banner() {
	fmt.Println()
	fmt.Println("  opentag — tag an agent from anywhere, run it durably, stream it to")
	fmt.Println("            everyone, deliver it anywhere.")
	fmt.Println()
	fmt.Println("  one process, no API key, no network, no infrastructure.")
}

// section prints a numbered header. Section 0 is the epilogue.
func section(n int, title string) {
	fmt.Printf("\n%s\n", rule)
	if n == 0 {
		fmt.Printf("  %s\n", title)
	} else {
		fmt.Printf("  %d. %s\n", n, title)
	}
	fmt.Printf("%s\n\n", rule)
}

// kindList renders the kinds a sink was handed, so "the webhook never saw a
// delta" is a printed fact rather than an assertion.
func kindList(kinds map[envelope.Kind]int) string {
	if len(kinds) == 0 {
		return "nothing"
	}
	parts := make([]string, 0, len(kinds))
	for k, n := range kinds {
		parts = append(parts, fmt.Sprintf("%s x%d", k, n))
	}
	slices.Sort(parts)
	return strings.Join(parts, ", ")
}

// await polls cond until it holds. Delivery and observation are asynchronous
// subscriptions, not return values, so a caller that wants to print what a sink
// received has to wait for it. Polling rather than a channel per surface keeps
// the wiring in this file about the architecture rather than about
// synchronisation.
func await(ctx context.Context, what string, cond func() bool) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		if cond() {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}
