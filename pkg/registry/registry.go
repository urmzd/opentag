// Package registry is the agent control plane: the append-only record of what
// every agent has ever been, scoped by tenant.
//
// It answers four questions and refuses to answer anything else: what is agent
// X now, what was agent X at revision N, what agents does this tenant have, and
// who changed one when. Everything about executing an agent belongs to
// pkg/runtime, everything about what a definition means belongs to
// pkg/agentspec, and everything about who is allowed to ask belongs to the
// caller's credential, which is why every method takes the tenant explicitly
// rather than fishing it out of a context (see Store).
//
// # Append-only, and why nothing is ever overwritten
//
// A revision is immutable and revising an agent appends revision N+1. That is
// not fastidiousness about history, it is what makes durable execution
// possible: a run pins the revision it accepted and duraturo replays that run
// from the top on every attempt, so a definition that changed under a run would
// make the replay diverge from the ledger that is supposed to be the truth
// about it. Three rules follow, and they are the whole design:
//
//   - Get(rev N) returns the same bytes forever, including after N+1 exists,
//     including after the agent is deleted.
//   - Revision numbers are never reused. Delete does not free a number and
//     re-creating a deleted name continues the sequence rather than restarting
//     it, because a run pinned to revision 3 must never be handed a different
//     revision 3.
//   - A revise whose content is the current content is a no-op. The revision
//     number is a version of the definition, not a count of the times someone
//     pressed save, and a client that resubmits an unchanged spec on every
//     deploy would otherwise walk the number away from anything meaningful.
//
// # Delete is a tombstone
//
// Delete retires a name: the agent stops appearing in List, stops resolving as
// "the current definition", and cannot be revised. Its history stays, and
// Get(rev N) keeps resolving, because a run in flight holds a pinned number and
// deleting an agent must not strand it. Retiring a definition and cancelling
// the work already running under it are separate acts; this package only ever
// does the first.
//
// The invariant is one sentence: a tombstoned agent is absent to every
// operation that means "the agent now" (Get with rev zero, Revise, Delete,
// List) and present to every operation that names a point in its past (Get with
// an explicit revision, History).
//
// # Tenancy
//
// The tenant is a hard boundary, not a filter applied late. Two tenants may
// each have an agent called docs-bot; they share a name and nothing else,
// including revision numbers, history and existence. There is no operation in
// this package that reads across tenants, and no error message that reveals
// that a name exists in another one.
package registry

import (
	"context"
	"errors"

	"github.com/urmzd/mandatum/pkg/agentspec"
)

// Sentinels every implementation reports. They are matched with errors.Is, and
// internal/server maps each to a status code (see ServerStore).
var (
	// ErrNotFound reports an agent, or a revision of one, that this tenant
	// does not have. A tombstoned agent is not found by name, and an agent
	// belonging to another tenant is not found either — reporting anything
	// else would make the registry a directory of other people's agents.
	ErrNotFound = errors.New("registry: not found")

	// ErrExists reports a create against a name that is already live.
	ErrExists = errors.New("registry: already exists")

	// ErrConflict reports a failed optimistic concurrency check: the caller
	// said which revision it was editing and that is no longer the current
	// one, so someone else's edit would have been silently overwritten.
	ErrConflict = errors.New("registry: conflict")

	// ErrInvalid reports a definition or an argument the registry will not
	// accept. It wraps pkg/agentspec's own ErrInvalid when the definition is
	// the problem, so a caller can match either.
	ErrInvalid = errors.New("registry: invalid")
)

// Page sizes.
const (
	// DefaultPageSize is used when a caller asks for none.
	DefaultPageSize = 50
	// MaxPageSize caps what a caller can ask for. A listing is served from
	// memory under a read lock, so an unbounded page is a way to hold that
	// lock for as long as the caller likes.
	MaxPageSize = 500
)

// Page is one step through a listing.
//
// The token is opaque on purpose. It encodes the tenant and the name the next
// page resumes after, which makes a listing resilient to writes between pages:
// resuming at a name rather than at an offset cannot skip an agent because an
// earlier one was deleted, and cannot repeat one because an earlier one was
// created. A caller that could see the encoding would eventually construct one,
// and that guarantee would become a coincidence.
type Page struct {
	// Size caps how many agents come back. Zero takes DefaultPageSize, and
	// anything above MaxPageSize is clamped rather than rejected: a page
	// size is a request for convenience, not an assertion about the world.
	Size int
	// Token continues a previous listing. Empty starts at the beginning.
	Token string
}

// Store is the agent control plane.
//
// It mirrors internal/server.Store method for method, in this package's
// vocabulary, so that adaptation at the edge is mechanical (see ServerStore)
// while the definition, its validation and its content hash stay here rather
// than leaking into a transport.
//
// Every method takes the tenant explicitly. A store cannot then be accidentally
// correct: a call with no tenant does not compile, and an implementation that
// ignores the argument is visibly wrong at its own boundary rather than subtly
// wrong at somebody else's.
type Store interface {
	// Create records spec as a new agent, attributing it to by. It fails
	// with ErrExists if the name is live. A name that was deleted may be
	// created again; its revision numbering continues rather than
	// restarting, because a run pinned to an old number must never resolve
	// to a new definition.
	Create(ctx context.Context, tenant string, spec agentspec.AgentSpec, by string) (agentspec.Revision, error)

	// Revise appends spec as the next revision. When expectedRev is non-zero
	// the append is rejected with ErrConflict unless it is the current
	// revision, so two editors racing cannot silently overwrite each other.
	// A spec whose canonical content is the current content is a no-op that
	// returns the current revision.
	Revise(ctx context.Context, tenant string, spec agentspec.AgentSpec, expectedRev int, by string) (agentspec.Revision, error)

	// Get reads one revision. rev zero means the current one, which a
	// tombstoned agent does not have; an explicit rev resolves for as long
	// as the record exists, deleted or not.
	Get(ctx context.Context, tenant, name string, rev int) (agentspec.Revision, error)

	// List returns one page of live agents at their current revision,
	// ordered by name, with the token that continues the listing. An empty
	// token means the listing is complete.
	List(ctx context.Context, tenant string, page Page) ([]agentspec.Revision, string, error)

	// History returns every revision of one agent, oldest first, including
	// after the agent has been deleted.
	History(ctx context.Context, tenant, name string) ([]agentspec.Revision, error)

	// Delete retires a name. It does not erase history and does not cancel
	// runs: a run holds its pinned revision independently of the store.
	Delete(ctx context.Context, tenant, name string) error
}
