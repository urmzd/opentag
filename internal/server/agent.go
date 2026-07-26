package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"

	opentagv1 "github.com/urmzd/opentag/gen/opentag/v1"
)

// Spec is an agent definition as the edge carries it: a document the server
// validates the identity of and otherwise does not interpret.
//
// It mirrors opentag.v1.AgentSpec field for field, and it exists as a Go type
// so that the Store seam does not speak protobuf. A store implementation is
// free to hold its own richer type — hashing, tool-catalog validation and
// canonicalisation all belong to it — and adapts at this boundary.
type Spec struct {
	Name         string
	Description  string
	Model        string
	Provider     string
	SystemPrompt string
	Tools        []string
	Sources      []Source
	Access       Access
}

// Source names one corpus an agent retrieves from.
type Source struct {
	Name    string
	URI     string
	Options map[string]string
}

// Access is the agent's authority boundary: an allowlist of agents it may
// delegate to and of workspace prefixes it may touch. Empty grants neither.
type Access struct {
	Spawn          []string
	WorkspaceAreas []string
}

// Revision is one immutable version of an agent.
//
// Hash and CreatedAt are the store's to assign, not the edge's: the hash is a
// property of the store's canonical encoding of the spec, and computing a
// second one here would create a second, quietly diverging answer to "is this
// the same definition".
type Revision struct {
	Spec      Spec
	Rev       int
	Hash      string
	CreatedAt time.Time
	CreatedBy string
}

// Page is one step through a listing.
type Page struct {
	// Size caps how many agents come back. Zero takes the store's default.
	Size int
	// Token continues a previous listing. Empty starts at the beginning.
	Token string
}

// Store is the agent control plane the edge needs, and nothing more.
//
// Every method takes the tenant explicitly rather than reading it from the
// context, so that a store cannot accidentally be correct: a call with no
// tenant does not compile, and an implementation that ignores the argument is
// visibly wrong at its own boundary rather than subtly wrong at ours.
//
// Errors are expected to match the sentinels this package declares —
// ErrNotFound, ErrExists, ErrConflict, ErrInvalid — which is one translation at
// the seam in exchange for identical status codes across every method.
//
// The immutability invariant lives here, not at the edge: Create accepts
// revision 1 and fails if the name exists, Revise appends N+1 and never
// modifies what is already there.
type Store interface {
	// Create accepts spec as revision 1 of a new agent, attributing it to by.
	Create(ctx context.Context, tenant string, spec Spec, by string) (Revision, error)

	// Revise appends spec as the next revision. When expectedRev is non-zero
	// the append is rejected unless it is the current revision, so two editors
	// racing cannot silently overwrite each other.
	Revise(ctx context.Context, tenant string, spec Spec, expectedRev int, by string) (Revision, error)

	// Get reads one revision. rev zero means the latest.
	Get(ctx context.Context, tenant, name string, rev int) (Revision, error)

	// List returns one page of agents at their latest revision, name-ordered,
	// with the token that continues the listing.
	List(ctx context.Context, tenant string, page Page) ([]Revision, string, error)

	// History returns every revision of one agent, oldest first.
	History(ctx context.Context, tenant, name string) ([]Revision, error)

	// Delete retires a name. It does not erase history and does not cancel
	// runs: a run holds its pinned revision independently of the store.
	Delete(ctx context.Context, tenant, name string) error
}

// agentService serves opentag.v1.AgentService. It authenticates, scopes every
// call to the caller's tenant, attributes authorship to the caller's subject,
// and forwards. Everything else is the Store's.
type agentService struct {
	store Store
	log   *slog.Logger
}

func (s *agentService) CreateAgent(ctx context.Context, req *connect.Request[opentagv1.CreateAgentRequest]) (*connect.Response[opentagv1.CreateAgentResponse], error) {
	id, store, err := s.ready(ctx)
	if err != nil {
		return nil, err
	}
	spec, err := specFromProto(req.Msg.GetSpec())
	if err != nil {
		return nil, fail(s.log, "create agent", err)
	}
	rev, err := store.Create(ctx, id.Tenant, spec, id.Subject)
	if err != nil {
		return nil, fail(s.log, "create agent", err)
	}
	return connect.NewResponse(&opentagv1.CreateAgentResponse{Revision: revisionToProto(rev)}), nil
}

func (s *agentService) ReviseAgent(ctx context.Context, req *connect.Request[opentagv1.ReviseAgentRequest]) (*connect.Response[opentagv1.ReviseAgentResponse], error) {
	id, store, err := s.ready(ctx)
	if err != nil {
		return nil, err
	}
	spec, err := specFromProto(req.Msg.GetSpec())
	if err != nil {
		return nil, fail(s.log, "revise agent", err)
	}
	rev, err := store.Revise(ctx, id.Tenant, spec, int(req.Msg.GetExpectedRev()), id.Subject)
	if err != nil {
		return nil, fail(s.log, "revise agent", err)
	}
	return connect.NewResponse(&opentagv1.ReviseAgentResponse{Revision: revisionToProto(rev)}), nil
}

func (s *agentService) GetAgent(ctx context.Context, req *connect.Request[opentagv1.GetAgentRequest]) (*connect.Response[opentagv1.GetAgentResponse], error) {
	id, store, err := s.ready(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.GetName() == "" {
		return nil, fail(s.log, "get agent", fmt.Errorf("%w: no agent name", ErrInvalid))
	}
	rev, err := store.Get(ctx, id.Tenant, req.Msg.GetName(), int(req.Msg.GetRev()))
	if err != nil {
		return nil, fail(s.log, "get agent", err)
	}
	return connect.NewResponse(&opentagv1.GetAgentResponse{Revision: revisionToProto(rev)}), nil
}

func (s *agentService) ListAgents(ctx context.Context, req *connect.Request[opentagv1.ListAgentsRequest]) (*connect.Response[opentagv1.ListAgentsResponse], error) {
	id, store, err := s.ready(ctx)
	if err != nil {
		return nil, err
	}
	revs, next, err := store.List(ctx, id.Tenant, Page{
		Size:  int(req.Msg.GetPageSize()),
		Token: req.Msg.GetPageToken(),
	})
	if err != nil {
		return nil, fail(s.log, "list agents", err)
	}
	out := &opentagv1.ListAgentsResponse{
		Agents:        make([]*opentagv1.Revision, 0, len(revs)),
		NextPageToken: next,
	}
	for _, rev := range revs {
		out.Agents = append(out.Agents, revisionToProto(rev))
	}
	return connect.NewResponse(out), nil
}

func (s *agentService) GetAgentHistory(ctx context.Context, req *connect.Request[opentagv1.GetAgentHistoryRequest]) (*connect.Response[opentagv1.GetAgentHistoryResponse], error) {
	id, store, err := s.ready(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.GetName() == "" {
		return nil, fail(s.log, "get agent history", fmt.Errorf("%w: no agent name", ErrInvalid))
	}
	revs, err := store.History(ctx, id.Tenant, req.Msg.GetName())
	if err != nil {
		return nil, fail(s.log, "get agent history", err)
	}
	out := &opentagv1.GetAgentHistoryResponse{Revisions: make([]*opentagv1.Revision, 0, len(revs))}
	for _, rev := range revs {
		out.Revisions = append(out.Revisions, revisionToProto(rev))
	}
	return connect.NewResponse(out), nil
}

func (s *agentService) DeleteAgent(ctx context.Context, req *connect.Request[opentagv1.DeleteAgentRequest]) (*connect.Response[opentagv1.DeleteAgentResponse], error) {
	id, store, err := s.ready(ctx)
	if err != nil {
		return nil, err
	}
	if req.Msg.GetName() == "" {
		return nil, fail(s.log, "delete agent", fmt.Errorf("%w: no agent name", ErrInvalid))
	}
	if err := store.Delete(ctx, id.Tenant, req.Msg.GetName()); err != nil {
		return nil, fail(s.log, "delete agent", err)
	}
	return connect.NewResponse(&opentagv1.DeleteAgentResponse{}), nil
}

// ready resolves the two preconditions every method shares: an authenticated
// caller and a configured store. Returning the store rather than reading the
// field again makes the nil check impossible to forget.
func (s *agentService) ready(ctx context.Context) (Identity, Store, error) {
	if s.store == nil {
		return Identity{}, nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("agent service: %w: no agent store is configured", ErrUnimplemented))
	}
	id, err := identityOf(ctx)
	if err != nil {
		return Identity{}, nil, connect.NewError(connect.CodeUnauthenticated, err)
	}
	return id, s.store, nil
}
