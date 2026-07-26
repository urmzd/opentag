# Architecture

> **Beta, pre-1.0.** This document describes the architecture as built and
> tested. Interfaces may change between minor versions. Where something is
> designed but not yet finished, it is called out in
> [Limitations](#limitations) rather than described as though it exists.

opentag is an agent platform and a low-latency pub/sub bus. Create an agent from
a spec, tag it from anywhere, get its events streamed to anyone and delivered
anywhere.

The single idea the whole design turns on: **where a tag is raised and where its
answer lands are independent.** A tag raised in GitHub can answer into Jira. A
schedule nobody watched can answer into both. That independence is what makes
this a mesh rather than a collection of integrations, and almost every decision
below follows from it.

## What each layer owns

opentag composes three libraries and adds the parts that make them a product.

| Concern | Owner |
|---------|-------|
| The agent loop, typed deltas, RAG and citations | [saige](https://github.com/urmzd/saige) |
| Durability, replay, exactly-once effects, per-run journal | [duraturo](https://github.com/urmzd/duraturo) |
| Sandboxed execution nodes, NGAC access control | [dispatch](https://github.com/urmzd/dispatch) |
| Agent specs, revisions, registry | opentag |
| Topics, the bus, delivery routing, transports | opentag |
| Connectors: triggers, sinks, and native actions | opentag |

The division is strict. duraturo owns whether work survives a crash; dispatch
owns what a tool is allowed to touch; saige owns what the model does. opentag
owns addressing, distribution, and the control plane, and delegates the rest.

## Topics: one namespace, matched by prefix

The mental model is one line: **users listen to an agent.**

```
agent                     every agent (an audit or dashboard consumer)
agent:docs-bot            every run of one agent
agent:docs-bot:run_01J    one run
```

Events are always published to the most specific form. Subscribing to any prefix
receives them. There is no wildcard language, because prefix containment already
expresses every subscription the mesh needs and it cannot be got wrong.

Containment is segment-wise, not string-wise: `agent:docs` does not cover
`agent:docs-bot`. A naive `strings.HasPrefix` would leak one agent's events into
another agent's subscription, so the property is pinned by a test.

### What is deliberately not a topic

An earlier design put tenant, origin, agent, revision, and event kind all in the
address, with a wildcard matcher over them. It was collapsed on purpose. Each
token that came out went somewhere better:

| Was going to be a topic token | Is now | Why |
|---|---|---|
| Event kind | A field, filtered | All events stream the same way. Splitting kinds across topics forces a consumer who wants the whole conversation to fan in from several topics and re-order them. |
| Agent revision | A field, filtered | Canary observation is a filter on one topic, not a second topic that appears and disappears as agents are revised. |
| Tenant | An authorization scope | A subscriber never names its own tenant. The server sets it from the caller's credential, so a subscriber cannot ask for another tenant's traffic by spelling it. |
| Destination | A route on the run | The same agent answers into Slack for one tag and GitHub for the next. Neither destination should exist in the topic namespace. |

The result is that a consumer only ever needs to know an agent's name to listen
to it, and no topic name has to be discovered at runtime.

## Triggers, sinks, and actors

A connector is a peer, not a client. It may implement up to three faces, and the
faces are independent:

| Face | Role |
|------|------|
| `Trigger` | Raises tags into the mesh |
| `Sink` | Renders events onto a native surface |
| `Actor` | Contributes native verbs the agent can call as tools |

Which faces a connector implements is what gives the mesh its shape, and the
asymmetry is real rather than incidental:

| Connector | Trigger | Sink | Actor |
|-----------|:-------:|:----:|:-----:|
| slack | yes | yes | yes |
| github | yes | yes | yes |
| jira | yes | yes | yes |
| cron | yes | no | no |
| webhook | no | yes | no |

A schedule has no surface to deliver to, so cron only triggers. An outbound POST
raises nothing, so webhook only sinks. Roles are derived by type assertion in
`connector.RolesOf` rather than declared, so a connector cannot advertise a face
it does not have, and `Registry.Sink(cron://...)` returns `ErrUnsupported`
instead of silently dropping an event.

No connector ever calls another connector. Sources publish, sinks subscribe, and
the bus is the only thing in between. Adding a destination is adding a
subscriber.

The `Actor` face is what turns "the agent replied" into "the work got done".
Actions run under the agent's NGAC policy rather than freely, and each one that
fires emits an `action.taken` event, so the work is auditable on the same bus as
the conversation that caused it.

## N:M delivery

One tag fans out to many sinks; one sink serves many agents. Each route chooses
its own event kinds, so granularity is per destination.

The worked example, which is what `examples/local` demonstrates:

```
Tag raised in:  github://acme/handbook/issues/42        origin "github"

Route 1:  slack://T01/C02?thread=1699.001     kinds: (all)
Route 2:  webhook://acme/deploys              kinds: lifecycle.completed
```

Slack receives every delta and edits one message as the answer arrives. The
webhook receives exactly one event and learns only that the work landed. Same
run, same bus, same events. Selectivity is a field on the route, not a separate
topic and not a separate run, which is why adding the webhook costs nothing on
the agent side.

Delivery is at-least-once, so sinks must be idempotent. The shape that makes
that natural is one message per run, edited as deltas arrive, keyed by run id.
Posting a message per delta would be correct and unusable.

The router preserves ordering per `(run, target)` pair and parallelises across
targets and runs. Ordering within a pair matters concretely: a Slack message
edited out of order shows the wrong final text.

## Agent revisions are pinned per run

An `AgentSpec` is immutable. Revising appends revision N+1 and never modifies
what is already there. A run pins its revision at accept time and executes
against that revision forever.

This is a correctness requirement, not bookkeeping. duraturo replays a workflow
from the top on every attempt. If a run resolved "latest" inside its workflow
body and someone revised the agent between attempts, the replay would run
against a different prompt and diverge, which duraturo detects as
`ErrNonDeterministic`. Pinning the revision in the run's durable input is what
makes replay safe.

The runtime goes one step further and pins the revision's content hash too, so a
revision whose content moved under a fixed number fails terminally rather than
silently executing something else.

Two consequences worth knowing:

- Deleting an agent is a tombstone, not an erase. The name stops accepting new
  tags, but `Get(name, rev)` still resolves, because a run pinned to that
  revision must remain replayable and explicable after the name is retired.
- Canary and A/B become subscription concerns. Watching revision 7 is
  `--rev 7` on one subscription, not a deploy.

## Durability

duraturo owns durability; dispatch does not. dispatch's queue is at-most-once in
its current beta, so a node that dies mid-task drops it. That is acceptable here
only because the durable run wrapping the task is duraturo's, and duraturo
replays it.

Each event is published twice, to two systems with different jobs:

- `duraturo.Emit` writes the durable per-run journal, which is what replay and
  cursor resume are built on.
- `bus.Publish` writes the live cross-run bus, which is what observers and
  delivery read.

The journal is truth; the bus is flow.

`Tag.ID` is the run's idempotency key. Every surface that raises tags
redelivers on timeout, so a Slack retry or a GitHub redelivery joins the run
that already exists rather than starting a second one and answering twice.

## Package DAG

Dependencies run one way. Leaves are stdlib-only and know nothing about
transports, storage, or agents.

```
leaves (stdlib only)
  pkg/topic        agent:name[:run], prefix containment
  pkg/address      github://urmzd/opentag/issues/42, slack://T01/C02?thread=...
  pkg/envelope     Tag, Event, Kind, Route, Filter, Subscription
  pkg/signature    HMAC verification for inbound webhooks
  pkg/agentspec    immutable specs, content-addressed revisions

seams
  pkg/connector    Trigger / Sink / Actor faces, registry
  pkg/bus          Bus interface, Memory and Redis Streams, bustest suite
  pkg/registry     agent store, Memory implementation

composition
  pkg/agentrt      builds a saige agent from a pinned revision; citations
  pkg/runtime      the durable duraturo turn, dispatch sandbox
  pkg/router       delivery fan-out to sinks
  pkg/connectors   slack, github, jira, cron, webhook
  internal/server  Connect handlers, SSE, webhook ingress
  internal/cli     command tree and the composition root
```

`internal/cli/core.go` is the composition root and the fastest way to understand
how the system fits together: it assembles the bus, registry, router, runtime,
connectors, and transport in the order data flows through them.

## Transports

Connect (connectrpc.com) serves gRPC, gRPC-Web, and HTTP/JSON from one handler
set, so gRPC clients and browsers share one contract and one codegen path.

| Surface | Purpose |
|---------|---------|
| `InvokeService.Invoke` | Tag an agent, return the run id once durable |
| `InvokeService.InvokeStream` | Invoke and tail in one RPC, with no race between accept and subscribe |
| `BusService.Subscribe` | Topic plus filter plus cursor, long-lived |
| `BusService.Publish` | Append an event to a run's topic |
| `AgentService.*` | Agent spec control plane |
| `GET /v1/sse` | The same stream, browser-native, resumable via `Last-Event-ID` |
| `POST /v1/webhooks/{connector}` | HMAC-verified ingress |

`InvokeStream` exists because doing invoke and subscribe separately races: events
emitted between the two are missed. Here the subscription is established before
the run is enqueued.

## Security boundaries

Two, and changes near either require tests proving confinement still holds.

**`pkg/signature`** is the only thing between the public internet and starting a
run. It verifies HMAC over the raw request body before that body is parsed.
Rules that follow: verify before parsing, hash the exact bytes received, compare
with `hmac.Equal` in constant time, and fail closed on every ambiguity including
a missing, empty, or duplicated header.

Slack signs a timestamp inside its base string, so a replay window is
enforceable and is enforced. GitHub does not, so a captured GitHub request stays
valid as long as the secret does. That defense lives downstream in `Tag.ID`
idempotency instead, and the package documents this rather than implying a
protection it cannot provide.

**The NGAC policy path** is dispatch's. An agent's `Access` block compiles into a
policy graph enforced by the sandbox with default deny, covering both workspace
areas and the spawn allowlist that governs agent-tags-agent.

## Limitations

Verified against the code, and honest.

- **The bus needs Redis or memory, not Postgres.** duraturo's `pgqueue` does not
  implement the `DeltaLog` capability, stated explicitly at
  `adapters/postgres/pgqueue/pgqueue.go`. A Postgres-only deployment runs
  durably but cannot stream.
- **duraturo's Redis and Postgres adapter submodules are not depended on.** They
  currently fail checksum verification against `sum.golang.org`, so opentag
  depends only on duraturo's root module and ships its own Redis Streams bus
  backend. duraturo's in-memory ledger and queue are complete systems and are
  what the single-binary deployment uses.
- **dispatch's queue is at-most-once** in beta. Durability comes from duraturo.
- **Citations are derived, not native.** saige v0.14.0 has no agent-level
  citation delta, so citations are extracted from RAG `AssembledContext` blocks
  and emitted as opentag's own `delta.citation` events.
- **duraturo v1 workflow bodies are single-goroutine.** Parallelism belongs
  inside a dispatch task, not in a forked workflow.
- **The bus is at-least-once with no consumer groups.** Subjects, filters, and
  cursors cover real-time consumers. If durable competing consumers with
  independent offsets become a requirement, the honest answer is NATS JetStream
  behind the `Bus` interface rather than growing a broker here.
- **Agent specs are loaded from JSON**, not YAML, so the CLI carries no parser
  dependency.
