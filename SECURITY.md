# Security Policy

## Reporting a Vulnerability

Report security issues privately through
[GitHub Security Advisories](https://github.com/urmzd/mandatum/security/advisories/new)
rather than in a public issue. Please include a description, the affected
version or commit, and a reproduction if you have one.

## Security boundaries

Two parts of mandatum are security boundaries. A change near either one needs
tests proving confinement still holds, and a pull request that touches them
without such tests will not be merged.

### `pkg/signature`

For a webhook-style trigger this is the only boundary there is. There is no
session and no handshake: the sole evidence that a request came from Slack or
GitHub, rather than from anyone who learned the URL, is a MAC over the raw
request body. A run, the agent's tools, and everything those tools may do all
hang off this check returning nil.

Invariants:

- Comparison is constant-time, via `hmac.Equal`. Never `==`, never
  `bytes.Equal` on a digest: a comparison that short-circuits leaks through
  timing how long a prefix matched, and a public endpoint can be probed
  repeatedly.
- Verification happens before the body is parsed, and hashes the exact bytes
  received. Not a re-encoded struct, not a normalized copy.
- Every ambiguity fails closed: a missing header, an empty header, a duplicated
  header, a malformed digest, and an unconfigured secret all return an error.
  An empty secret is treated as broken rather than permissive, because HMAC with
  an empty key is computable by anyone.
- Slack's replay window is enforced, and a stale timestamp is rejected without
  the digest being computed at all.

GitHub signs the body alone, with no timestamp in the base string, so a captured
GitHub request stays valid as long as the secret does. No code in this package
can change that. That defense lives downstream in `Tag.ID` idempotency, where a
replayed delivery joins the run it duplicated rather than starting a second one.
This is stated plainly rather than implying a protection the package cannot
provide.

Required test coverage for changes: known-good vectors pinning the exact signing
base string, tampered body, tampered signature, wrong secret, missing and
duplicated headers, malformed digests, replay window on both sides of its
boundary, and a fuzz pass proving no input panics.

### The NGAC policy path

An agent's `Access` block compiles into dispatch's policy graph and is enforced
by the sandbox with default deny. It governs both the workspace areas a tool may
touch and the spawn allowlist that decides which agents an agent may delegate
to.

Required test coverage for changes: path traversal, sibling prefixes, list-leak,
spawn gating, and prohibition override. Default is always deny.

## Tenant isolation

Tenant is an authorization scope taken from the caller's credential. It is never
read from a request body, and the server overwrites whatever a client sent. A
client must not be able to reach another tenant's agents, runs, or events by
spelling a tenant name, and there is a test asserting exactly that.

## Supported Versions

mandatum is pre-1.0. Security fixes land on the latest minor release.
