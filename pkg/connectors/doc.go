// Package connectors holds the mesh: the concrete peers that raise tags and
// render events onto real surfaces.
//
// Each subpackage implements the faces of pkg/connector that its surface can
// actually support, and the asymmetry between them is the whole point:
//
//	slack    trigger + sink + actor
//	github   trigger + sink + actor
//	jira     trigger + sink + actor
//	cron     trigger only          — a schedule has no surface to deliver to
//	webhook  sink only             — an outbound POST raises nothing
//
// No connector imports another. A tag raised in github reaches jira because
// both talk to the bus, never to each other, and pkg/connector.RolesOf derives
// the faces from the types rather than trusting a declaration. Registering the
// five in one pkg/connector.Registry is all the wiring the mesh needs.
//
// # Two rules shared by every human-facing sink
//
// Delivery is at-least-once, so rendering must be idempotent. The shape that
// achieves this is one message per (run, target) that gets EDITED as the run
// proceeds, never a new message per delta: a Slack channel that got one message
// per token would be unusable, and a redelivered event would double every line.
// The engine in internal/sink owns that invariant, and every event is rendered
// by re-rendering the whole accumulated document, which makes redelivery a
// no-op by construction rather than by bookkeeping.
//
// Ordering is not guaranteed either, so accumulation is order-insensitive.
// internal/render files each event under its Event.Seq, so a late low-Seq text
// delta lands in its correct position instead of appending stale text after
// fresh text, and a Seq already applied is dropped.
//
// The exception proves the rule: the webhook sink posts every event, because
// its receiver is a program that dedupes on the delivery key rather than a
// human reading a thread.
//
// # No provider SDKs
//
// These connectors speak to their APIs with net/http and encoding/json. A
// handful of endpoints per surface does not justify pulling a vendor SDK (and
// its transitive graph) into a module whose reason to exist is being the bus.
// Every connector puts its calls behind a small interface with a real
// implementation and an in-memory fake, so all the logic above the wire —
// mention parsing, addressing, idempotent rendering, coalescing, retries — is
// tested without a network.
package connectors
