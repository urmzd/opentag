package registry

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/urmzd/mandatum/pkg/agentspec"
)

// Memory is a complete Store held in process memory.
//
// It is not a placeholder for a database. It is the store a single-node
// deployment runs on, the one the tests and the offline demo use, and the
// reference for what any other implementation must do: every rule this package
// documents — tombstones that keep resolving, revision numbers that never
// repeat, identical content that does not append, tenants that cannot see each
// other — is enforced here, not left to whatever comes later.
//
// What it does not have is durability. A restart loses every definition, so a
// deployment whose runs outlive its process needs a persistent Store; the
// interface is the seam for exactly that. The mutable state is one map behind
// one RWMutex, because the workload is read-mostly (every accepted tag resolves
// a definition, definitions change rarely) and a lock per agent would buy
// nothing but the chance to deadlock two of them.
type Memory struct {
	mu      sync.RWMutex
	tenants map[string]map[string]*record

	// Immutable after New, so they need no lock.
	clock     func() time.Time
	providers []string
}

// record is one agent's whole life: its revisions in order, and whether the
// name is retired. revs is append-only and index i holds revision i+1.
type record struct {
	revs    []agentspec.Revision
	deleted bool
}

// Option configures a Memory.
type Option func(*Memory)

// WithClock replaces the source of CreatedAt. A test that needs to assert on
// timestamps supplies its own rather than sleeping.
func WithClock(now func() time.Time) Option {
	return func(m *Memory) {
		if now != nil {
			m.clock = now
		}
	}
}

// WithProviders replaces the provider table definitions are validated against.
//
// It exists because a deployment that registers a custom provider with
// pkg/agentrt must be able to store the agents that use it. Without this the
// control plane would reject definitions the runtime can execute, which is the
// worst of both: valid work refused, with no way to say so.
func WithProviders(providers ...string) Option {
	return func(m *Memory) { m.providers = slices.Clone(providers) }
}

// NewMemory returns an empty store.
func NewMemory(opts ...Option) *Memory {
	m := &Memory{
		tenants:   make(map[string]map[string]*record),
		clock:     time.Now,
		providers: agentspec.KnownProviders(),
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Memory is the reference implementation, so the compiler is asked to say so.
var _ Store = (*Memory)(nil)

// Create records spec as a new agent.
//
// Creating a name that was deleted revives it and continues its revision
// numbering. Restarting at revision 1 would be the obvious alternative and it
// is unsound: a run pinned to revision 1 of the old agent would resolve
// revision 1 of the new one, which is a different definition wearing the same
// address. pkg/runtime's hash check would catch it and fail the run, but a
// design whose correctness depends on another package noticing a collision is
// not a design.
func (m *Memory) Create(ctx context.Context, tenant string, spec agentspec.AgentSpec, by string) (agentspec.Revision, error) {
	canonical, err := m.prepare(ctx, tenant, spec)
	if err != nil {
		return agentspec.Revision{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rec := m.lookup(tenant, canonical.Name)
	if rec != nil && !rec.deleted {
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q is already at revision %d", ErrExists, canonical.Name, len(rec.revs))
	}
	if rec == nil {
		rec = &record{}
		agents := m.tenants[tenant]
		if agents == nil {
			agents = make(map[string]*record)
			m.tenants[tenant] = agents
		}
		agents[canonical.Name] = rec
	}
	rec.deleted = false
	return m.appendLocked(rec, canonical, by)
}

// Revise appends spec as the next revision of a live agent.
//
// The concurrency check runs before the no-op check on purpose. A caller that
// passed expectedRev asserted which definition it was editing, and answering
// "no change needed" to an assertion that is already false would confirm a
// belief the caller does not get to hold — it edited revision 4 while the world
// moved to revision 6, and the fact that its text happens to match revision 6
// is a coincidence, not agreement.
func (m *Memory) Revise(ctx context.Context, tenant string, spec agentspec.AgentSpec, expectedRev int, by string) (agentspec.Revision, error) {
	if expectedRev < 0 {
		return agentspec.Revision{}, fmt.Errorf("%w: expected revision %d is negative", ErrInvalid, expectedRev)
	}
	canonical, err := m.prepare(ctx, tenant, spec)
	if err != nil {
		return agentspec.Revision{}, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rec := m.lookup(tenant, canonical.Name)
	if rec == nil || rec.deleted {
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q", ErrNotFound, canonical.Name)
	}
	if current := len(rec.revs); expectedRev != 0 && expectedRev != current {
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q is at revision %d, not %d; re-read it and reapply the change", ErrConflict, canonical.Name, current, expectedRev)
	}
	return m.appendLocked(rec, canonical, by)
}

// Get reads one revision.
//
// rev zero means the current definition, which is the only reading a tombstone
// hides. An explicit revision resolves whether or not the agent is live: this
// is the replay property, and it is the reason Delete does not erase. A run
// that pinned revision 3 executes revision 3 on every attempt, and an operator
// retiring the agent halfway through must not turn that run into an
// unresolvable reference in a ledger that cannot be edited.
func (m *Memory) Get(ctx context.Context, tenant, name string, rev int) (agentspec.Revision, error) {
	if err := ctx.Err(); err != nil {
		return agentspec.Revision{}, err
	}
	if err := checkTenant(tenant); err != nil {
		return agentspec.Revision{}, err
	}
	key := agentspec.NormalizeName(name)
	if key == "" {
		return agentspec.Revision{}, fmt.Errorf("%w: no agent name", ErrInvalid)
	}
	if rev < 0 {
		return agentspec.Revision{}, fmt.Errorf("%w: revision %d is negative", ErrInvalid, rev)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	rec := m.lookup(tenant, key)
	if rec == nil {
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q", ErrNotFound, key)
	}
	if rev == 0 {
		if rec.deleted {
			return agentspec.Revision{}, fmt.Errorf("%w: agent %q is deleted; revisions 1 to %d still resolve by number", ErrNotFound, key, len(rec.revs))
		}
		return rec.revs[len(rec.revs)-1].Clone(), nil
	}
	if rev > len(rec.revs) {
		return agentspec.Revision{}, fmt.Errorf("%w: agent %q has no revision %d; the latest is %d", ErrNotFound, key, rev, len(rec.revs))
	}
	return rec.revs[rev-1].Clone(), nil
}

// List returns one page of live agents at their current revision.
func (m *Memory) List(ctx context.Context, tenant string, page Page) ([]agentspec.Revision, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	if err := checkTenant(tenant); err != nil {
		return nil, "", err
	}
	after, err := decodeToken(page.Token, tenant)
	if err != nil {
		return nil, "", err
	}
	size := page.Size
	switch {
	case size <= 0:
		size = DefaultPageSize
	case size > MaxPageSize:
		size = MaxPageSize
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.tenants[tenant]))
	for name, rec := range m.tenants[tenant] {
		if rec.deleted || name <= after {
			continue
		}
		names = append(names, name)
	}
	slices.Sort(names)

	next := ""
	if len(names) > size {
		// There is more to come, so the token names the last agent on
		// this page. Resuming by name rather than by offset is what
		// makes a stale token safe: a create or delete between pages
		// changes what comes next, never whether something was skipped.
		next = encodeToken(tenant, names[size-1])
		names = names[:size]
	}
	out := make([]agentspec.Revision, 0, len(names))
	for _, name := range names {
		rec := m.tenants[tenant][name]
		out = append(out, rec.revs[len(rec.revs)-1].Clone())
	}
	return out, next, nil
}

// History returns every revision of one agent, oldest first.
//
// A deleted agent still has a history. Retiring a name is not a claim that it
// never existed, and an audit trail that disappears when someone presses delete
// is not an audit trail.
func (m *Memory) History(ctx context.Context, tenant, name string) ([]agentspec.Revision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := checkTenant(tenant); err != nil {
		return nil, err
	}
	key := agentspec.NormalizeName(name)
	if key == "" {
		return nil, fmt.Errorf("%w: no agent name", ErrInvalid)
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	rec := m.lookup(tenant, key)
	if rec == nil {
		return nil, fmt.Errorf("%w: agent %q", ErrNotFound, key)
	}
	out := make([]agentspec.Revision, len(rec.revs))
	for i, rev := range rec.revs {
		out[i] = rev.Clone()
	}
	return out, nil
}

// Delete retires a name, leaving its history intact.
//
// Deleting an already-deleted agent reports ErrNotFound rather than succeeding
// quietly. The alternative reading — delete is idempotent, so retiring a
// retired name is fine — would make Delete the one operation that can see a
// tombstone, and the invariant that a tombstoned agent is absent to everything
// meaning "the agent now" is worth more than the convenience.
func (m *Memory) Delete(ctx context.Context, tenant, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := checkTenant(tenant); err != nil {
		return err
	}
	key := agentspec.NormalizeName(name)
	if key == "" {
		return fmt.Errorf("%w: no agent name", ErrInvalid)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	rec := m.lookup(tenant, key)
	if rec == nil || rec.deleted {
		return fmt.Errorf("%w: agent %q", ErrNotFound, key)
	}
	rec.deleted = true
	return nil
}

// prepare runs every check that does not need the lock: the caller is still
// waiting, the tenant is real, and the definition is one this deployment
// accepts. Validation happens outside the lock because it is the most expensive
// thing a write does and none of it depends on stored state.
func (m *Memory) prepare(ctx context.Context, tenant string, spec agentspec.AgentSpec) (agentspec.AgentSpec, error) {
	if err := ctx.Err(); err != nil {
		return agentspec.AgentSpec{}, err
	}
	if err := checkTenant(tenant); err != nil {
		return agentspec.AgentSpec{}, err
	}
	canonical := spec.Normalize()
	if err := canonical.ValidateWith(m.providers); err != nil {
		return agentspec.AgentSpec{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	return canonical, nil
}

// appendLocked appends the next revision, or returns the current one when there
// is nothing to append. The caller holds the write lock.
//
// The no-op is decided on the content hash, not on the submitted bytes, so a
// client that reordered its tool list or re-indented its prompt does not burn a
// revision number. A revision is a version of a definition; if the definition
// did not change, there is no version to record, and appending one would make
// "revision 12" mean "the twelfth save" instead of "the twelfth definition".
func (m *Memory) appendLocked(rec *record, canonical agentspec.AgentSpec, by string) (agentspec.Revision, error) {
	if n := len(rec.revs); n > 0 && rec.revs[n-1].Hash == canonical.Hash() {
		return rec.revs[n-1].Clone(), nil
	}
	rev, err := agentspec.NewRevisionWith(canonical, len(rec.revs)+1, by, m.clock(), m.providers)
	if err != nil {
		return agentspec.Revision{}, fmt.Errorf("%w: %w", ErrInvalid, err)
	}
	// The stored revision owns its own memory. Callers get clones on the way
	// out; this is the matching half, so a caller that kept a reference to
	// the slice it passed in cannot reach into history afterwards.
	rec.revs = append(rec.revs, rev.Clone())
	return rev, nil
}

// lookup resolves a normalized name within one tenant. The caller holds a lock.
func (m *Memory) lookup(tenant, name string) *record {
	agents := m.tenants[tenant]
	if agents == nil {
		return nil
	}
	return agents[name]
}

// checkTenant rejects the unscoped call. An empty tenant is not a wildcard and
// must never become one: the tenant comes from the caller's credential, so a
// missing one means the authorization step was skipped, and serving that
// request from a shared bucket is how one customer reads another's agents.
func checkTenant(tenant string) error {
	if tenant == "" {
		return fmt.Errorf("%w: no tenant; every registry call is scoped to one", ErrInvalid)
	}
	return nil
}
