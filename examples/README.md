# Examples

Runnable programs that demonstrate opentag rather than describe it. Every
example in this directory runs with no API key, no network and no
infrastructure: the in-memory implementations of the same interfaces stand in
for Postgres, Redis and a model provider, so `go run` is the only setup.

| Example | Command | What it proves |
| --- | --- | --- |
| [`local`](local) | `go run ./examples/local` | The whole architecture end to end: a declarative agent, immutable revisions, a tag raised in one connector answered in two others, N:M selectivity, an uninvolved third party reading the stream, first-class citations, and a replayed run that does not re-execute a recorded activity. |

## local

```
go run ./examples/local
```

One process, one file of story (`main.go`) and one file of scaffolding
(`fakes.go`). It prints seven numbered sections, each one a claim opentag makes
and a demonstration of it:

1. **An agent is a declarative spec.** A name, a provider, a model, a system
   prompt and its sources go into the control plane and come back as revision 1
   with a content hash. The name is now a topic anyone can subscribe to.
2. **A revision is immutable.** Editing the prompt creates revision 2 and leaves
   revision 1 byte for byte what it was. Both numbers still resolve. This is why
   a run can pin its revision at accept time and replay under it forever.
3. **Origin and destination are independent.** A tag arrives from
   `github://acme/handbook/issues/42` and is answered into
   `slack://T01/C02?thread=1699.001` and `webhook://acme/deploys`. The trigger is
   GitHub; neither destination is. Nothing on the path from trigger to sink
   holds both ends at once, which is the difference between a mesh and a pile of
   integrations.
4. **One run, two sinks, two granularities.** The Slack route takes every kind
   and edits one message as the answer streams in; the webhook route takes
   `lifecycle.completed` only and posts exactly once. Same run, same bus, same
   events. Selectivity is a field on the route, not a second topic and not a
   second run.
5. **A third party can listen to an agent.** A subscriber holds one
   subscription to `agent:docs-bot`. It raised nothing, no route names it, and
   neither run can tell it is there. It sees both runs in full, with `Seq` as a
   resume cursor.
6. **Citations are first-class events.** `delta.citation` arrives on the same
   stream and in the same order as the text, carrying document, section and a
   resolvable URI. Citations are derived from what the turn retrieved, not from
   what the model claimed, so a reader can check them.
7. **A replayed run does not re-execute a recorded activity.** The example arms
   one broker outage on a run's `lifecycle.completed` publish. The turn fails,
   duraturo replays the workflow from the top, and the printed model-call
   counter moves by exactly one across two attempts: the agent activity was
   served from the ledger. The subscriber sees a `lifecycle.resumed` event in
   its own sequence band rather than a run that went silent.

### What is real and what is swapped

Nothing above the storage seam is simplified for the demo. The agent loop, the
typed deltas, the translation into events, the sequence banding, publication,
routing, per-lane ordering and the durable replay are all the production code
paths.

| Concern | The example runs | A deployment runs |
| --- | --- | --- |
| Control plane | `pkg/registry.Memory` | a `registry.Store` over Postgres |
| Bus | `pkg/bus.Memory` | `pkg/bus` Redis Streams |
| Ledger and queue | duraturo `ledger.Memory`, `queue.Memory` | duraturo `pgledger` and `redisqueue` |
| Model | saige `agenttest.ScriptedProvider`, via the spec's `offline` provider | `anthropic` or `ollama`, by changing two fields on the spec |
| Execution | an in-process `runtime.Executor` | `runtime.NewSandbox`, a dispatch node under NGAC |
| Connectors | two fakes implementing `connector.Sink` | `pkg/connectors/slack`, `pkg/connectors/webhook` |

Every row is a constructor change. `main.go` does not otherwise change.

### Reading the output

Two details are worth knowing before the first run.

**Sequence numbers jump.** Each attempt of a run gets its own sequence band, so
attempt 1 opens at 101 and attempt 2 at 201. That is what lets a retry append
its output after an interrupted attempt's partial output instead of vanishing
behind the bus's high-water mark. The example shrinks the band from its default
of 2^20 to 100 (`runtime.WithStride`) purely so the numbers stay readable.

**The surface output is printed settled, not live.** The router carries events
for one `(run, target)` pair on a single goroutine and deliberately imposes no
order across targets, so two sinks printing directly would interleave
differently on every run. The fake sinks therefore record what they wrote to
their surface keyed by the sequence that caused it, and `main` prints the
transcript in `(seq, surface)` order once the run is terminal. What each sink
received, and when, is unchanged.

The example is deterministic: a fixed clock stamps the events, so two runs print
the same bytes.
