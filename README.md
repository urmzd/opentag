<p align="center">
  <h1 align="center">opentag</h1>
  <p align="center">
    Tag an agent from anywhere. Stream it to everyone. Deliver it anywhere.
    <br /><br />
    <a href="https://github.com/urmzd/opentag/releases">Download</a>
    &middot;
    <a href="https://github.com/urmzd/opentag/issues">Report Bug</a>
    &middot;
    <a href="https://pkg.go.dev/github.com/urmzd/opentag">Go Docs</a>
  </p>
</p>

<p align="center">
  <a href="#status"><img src="https://img.shields.io/badge/status-beta-orange" alt="Status: Beta"></a>
  &nbsp;
  <a href="https://github.com/urmzd/opentag/actions/workflows/ci.yml"><img src="https://github.com/urmzd/opentag/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  &nbsp;
  <a href="https://pkg.go.dev/github.com/urmzd/opentag"><img src="https://pkg.go.dev/badge/github.com/urmzd/opentag.svg" alt="Go Reference"></a>
  &nbsp;
  <a href="LICENSE"><img src="https://img.shields.io/github/license/urmzd/opentag" alt="License"></a>
</p>

> ## Status
>
> **opentag is beta software. It is pre-1.0 and not yet recommended for
> production.**
>
> Interfaces are stabilizing but may change between minor versions, and a minor
> release may require code changes on your side.
>
> What that means concretely today:
>
> - **Works and is tested.** The single-binary deployment (in-memory bus, ledger
>   and registry), the topic and delivery model, durable runs with replay, the
>   Connect and SSE transports, and the Slack, GitHub, Jira, cron and webhook
>   connectors.
> - **Written but not exercised in CI.** The Redis Streams bus backend passes the
>   shared conformance suite, but its integration tests skip without a Redis to
>   point at.
> - **Not built yet.** A persistent registry: agent specs live in memory, so they
>   do not survive a restart.
>
> See [Limitations](#limitations) for the full list.

opentag is an agent platform and a low-latency pub/sub bus. Define an agent with
a spec, tag it from Slack, GitHub, Jira or a schedule, and its events stream to
any subscriber while being delivered to any destination you name.

The idea it turns on: **where a tag is raised and where its answer lands are
independent.** A review request raised in GitHub can answer into Jira. A nightly
schedule nobody watched can answer into both. That is what makes it a mesh
rather than a pile of integrations.

## Features

- **Agents are documents.** Create one by submitting a spec. No redeploy, no code, no restart.
- **Listen to an agent.** One topic namespace, matched by prefix: `agent:docs-bot` receives every run of that agent, including runs that start after you subscribe.
- **Origin and destination are independent.** Any trigger can raise a tag; any sink can receive it. They never call each other, they only publish and subscribe.
- **N:M delivery with per-route selectivity.** One run streams every delta into a GitHub comment it keeps editing while a webhook on the same run receives only `lifecycle.completed`.
- **Connectors act, not just talk.** The `Actor` face contributes native verbs (transition a Jira ticket, request a review) as agent tools, sandboxed by NGAC, each emitting an auditable `action.taken` event.
- **Durable by construction.** Runs survive crashes and replay from a ledger. A redelivered webhook joins the run it duplicated instead of answering twice.
- **Revisions are pinned per run.** A run accepted under revision 6 executes as revision 6 forever, even after a revise, because replay demands it.
- **Citations are first-class events**, carrying the exact quoted source and resolvable provenance.
- **One contract, three transports.** Connect serves gRPC, gRPC-Web and HTTP/JSON, plus resumable SSE for browsers.

## Installation

### Script (macOS / Linux)

```sh
curl -fsSL https://raw.githubusercontent.com/urmzd/opentag/main/install.sh | sh
```

### Go

```sh
go install github.com/urmzd/opentag/cmd/opentag@latest
```

### Library

```sh
go get github.com/urmzd/opentag
```

## Quick Start

See the whole architecture run in one process, with no API key, no network and
no infrastructure:

```sh
go run ./examples/local
```

It creates an agent, revises it, tags it from a simulated GitHub trigger with
delivery routed to Slack and a webhook, shows a third party listening to the
agent's topic, and demonstrates that a replayed run does not pay for the model
twice.

Or run the real thing:

```sh
opentag serve &

cat > docs-bot.json <<'JSON'
{
  "name": "docs-bot",
  "description": "Answers questions about the deploy pipeline",
  "provider": "anthropic",
  "model": "claude-sonnet-5",
  "system_prompt": "You answer questions about the deploy pipeline. Cite your sources."
}
JSON

opentag agent create -f docs-bot.json
opentag tag docs-bot "how does a release get out the door?"
```

Deliver the answer somewhere other than your terminal, with each destination
choosing its own granularity:

```sh
opentag tag review-bot "review this change" \
  --deliver 'github://urmzd/opentag/issues/42|delta' \
  --deliver 'webhook://acme/deploys|lifecycle.completed'
```

Watch the bus. This subscriber invoked nothing and still sees every run:

```sh
opentag listen agent:docs-bot
opentag listen agent --kinds lifecycle.completed --format json | jq
```

## Commands

| Command | Purpose |
|---------|---------|
| `opentag serve` | Run the core: API, SSE, webhooks, bus, router, worker |
| `opentag work` | Run an execution node with no inbound API |
| `opentag agent` | Create, revise, get, list, history, delete |
| `opentag tag` | Tag an agent and stream its answer |
| `opentag listen` | Subscribe to a topic and print events |
| `opentag version` | Version, commit, build date |
| `opentag update` | Self-update from GitHub releases |

Every command takes `--format text|json`. Results go to stdout, diagnostics to
stderr, so `--format json` output pipes cleanly into `jq` even while the same
process logs a reconnect.

## Documentation

| Document | Covers |
|----------|--------|
| [docs/architecture/overview.md](docs/architecture/overview.md) | Topics, connector faces, N:M delivery, revision pinning, package DAG, limitations |
| [AGENTS.md](AGENTS.md) | AI-facing conventions, commands, rules, extension guide |
| [examples/README.md](examples/README.md) | Runnable example index |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development workflow and commit convention |
| [SECURITY.md](SECURITY.md) | Security boundaries and reporting |

API reference: [pkg.go.dev/github.com/urmzd/opentag](https://pkg.go.dev/github.com/urmzd/opentag)

## How it composes

opentag builds on three libraries and adds addressing, distribution and the
control plane:

| Concern | Owner |
|---------|-------|
| Agent loop, typed deltas, RAG, citations | [saige](https://github.com/urmzd/saige) |
| Durability, replay, per-run journal | [duraturo](https://github.com/urmzd/duraturo) |
| Sandboxed execution nodes, NGAC | [dispatch](https://github.com/urmzd/dispatch) |
| Topics, bus, routing, connectors, specs | opentag |

## Limitations

- The bus requires Redis or the in-memory backend. duraturo's `pgqueue` does not implement `DeltaLog`, so a Postgres-only deployment runs durably but cannot stream.
- The registry is in-memory only; a Postgres implementation is not written yet.
- dispatch's queue is at-most-once in beta, so durability comes from duraturo rather than from dispatch.
- Citations are derived from RAG context blocks, because saige v0.14.0 has no agent-level citation delta.
- The bus is at-least-once with no consumer groups. If durable competing consumers become a requirement, NATS JetStream behind the `Bus` interface is a better answer than growing a broker here.
- Agent specs load from JSON, not YAML.

## License

Apache 2.0. See [LICENSE](LICENSE).
