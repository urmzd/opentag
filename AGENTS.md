# opentag

Agent platform and low-latency pub/sub bus. Tag an agent from anywhere, stream
its events to anyone, deliver them anywhere. Go module
`github.com/urmzd/opentag`, binary `opentag`.

**Status: beta, pre-1.0, not recommended for production.** Interfaces are
stabilizing but may change between minor versions. The single-binary deployment
is complete and tested; the Redis bus backend is conformance-tested but not
exercised in CI; there is no persistent registry yet, so agent specs do not
survive a restart. Keep the surface small and document limitations honestly.

The design turns on one idea: where a tag is raised and where its answer lands
are independent. Read `docs/architecture/overview.md` before changing anything
structural.

## Architecture

Composes three libraries and adds addressing, distribution and the control
plane. saige owns the agent loop, duraturo owns durability, dispatch owns
sandboxing and access control.

| Package | Role |
|---------|------|
| `pkg/topic` | Topic addressing: `agent`, `agent:<name>`, `agent:<name>:<run>`. Prefix containment, no wildcards. Leaf. |
| `pkg/address` | Mesh endpoint URIs: `github://urmzd/opentag/issues/42`, `slack://T01/C02?thread=...`. Leaf. |
| `pkg/envelope` | Wire contract: `Tag`, `Event`, `Kind`, `Route`, `Filter`, `Subscription`. Leaf. |
| `pkg/signature` | HMAC verification for inbound webhooks (Slack v0, GitHub sha256). Leaf. Security boundary. |
| `pkg/agentspec` | Immutable agent specs, content-addressed revisions. Leaf. |
| `pkg/connector` | `Trigger` / `Sink` / `Actor` faces, derived by `RolesOf`, plus the registry. |
| `pkg/registry` | Agent store and revision history. `Memory` implementation, plus the adapter to `internal/server.Store`. |
| `pkg/bus` | `Bus` interface, `Memory` and Redis Streams backends, `bustest` conformance suite. |
| `pkg/router` | Delivery fan-out to sinks, ordered per `(run, target)`. |
| `pkg/agentrt` | Builds a saige agent from a pinned revision; delta translation; citation extraction. `payload/` holds typed event bodies. |
| `pkg/runtime` | The durable duraturo turn: revision pinning, dispatch sandbox, dual publish. |
| `pkg/connectors` | `slack`, `github`, `jira`, `cron`, `webhook`, plus shared helpers under `internal/`. |
| `internal/server` | Connect handlers, SSE, webhook ingress, tenant interceptor. |
| `internal/cli` | Command tree. `core.go` is the composition root. |
| `gen/`, `proto/` | The protobuf contract and its generated code. `gen/` is committed. |

Discover layout with `tree` or ripgrep; do not trust stale listings.

## Commands

| Action | Command |
|--------|---------|
| init | `make init` |
| build | `make build` |
| test | `go test ./...` |
| race test | `go test ./... -race` |
| lint | `golangci-lint run` |
| fmt | `gofmt -w .` |
| quality gate | `make check` |
| run server | `make run` (or `go run ./cmd/opentag serve`) |
| regenerate proto | `make proto` (needs `buf` on PATH) |
| end-to-end demo | `go run ./examples/local` |

## Code Style

- Idiomatic Go, stdlib-first; every dependency must earn its place.
- Package doc comments explain the concern, its orthogonality boundary, and the
  why. `pkg/topic/topic.go` and `pkg/signature/signature.go` set the bar.
- Errors: wrap with `fmt.Errorf("pkg: context: %w", err)`; sentinel errors for
  callers to `errors.Is` on.
- In-memory implementations are complete systems, never stubs.
- Tests are table-driven and named after the property they protect, not the
  function they call.
- Conventional commits (feat/fix/chore/...); `sr` cuts releases from them.
- No em dashes in documentation.

## Rules

- **`pkg/signature` is a security boundary.** It is the only thing between the
  public internet and starting a run. Any change needs tests proving
  confinement: constant-time comparison, verify-before-parse, replay window,
  malformed input, and duplicated headers. Default is always deny.
- **The NGAC policy path is a security boundary.** An agent's `Access` block
  compiles into dispatch's policy graph with default deny. Changes need tests
  proving spawn gating and workspace confinement still hold.
- **Preserve the DAG.** Leaves (`topic`, `address`, `envelope`, `signature`,
  `agentspec`) stay stdlib-only. Nothing under `pkg/` imports `internal/`.
- **Revision pinning is load-bearing.** A run pins its revision at accept time
  and must never re-resolve "latest" inside a workflow body: duraturo replays
  from the top, and an unpinned run diverges. Tests that assert this exist in
  `pkg/runtime`; do not weaken them.
- **Durability comes from duraturo, never from dispatch.** dispatch's queue is
  at-most-once in beta.
- **Sinks must be idempotent.** Delivery is at-least-once. Render one message
  per run and edit it; never post per delta.
- **Tenant is never read from a request body.** The server sets it from the
  caller's credential and overwrites whatever the client sent.
- This is a beta: keep the surface small and document limitations honestly
  rather than papering over them.

## Extension Guide

- **New connector**: implement `connector.Connector` plus whichever of
  `Trigger`, `Sink`, `Actor` apply, then register it. Roles are derived by type
  assertion, so implementing a face is how you advertise it. Reuse the shared
  helpers in `pkg/connectors/internal/` for mention parsing, idempotent sink
  rendering and HTTP plumbing rather than reimplementing them.
- **New bus backend**: implement `bus.Bus` and run it through
  `pkg/bus/bustest.Run`, which holds every backend to the identical contract.
- **New registry backend**: implement `registry.Store`; the adapter to the
  server's interface already exists.
- **New agent provider**: extend `pkg/agentrt`'s provider construction. Keep
  `agenttest.ScriptedProvider` working, since it is what makes the example and
  the tests run with no API key.
- **New event kind**: add it to `pkg/envelope`, give it a typed body in
  `pkg/agentrt/payload`, and teach the connector renderers about it. Kinds are
  fields, never topics.
