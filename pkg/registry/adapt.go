package registry

import (
	"context"
	"errors"
	"fmt"

	"github.com/urmzd/opentag/internal/server"
	"github.com/urmzd/opentag/pkg/agentrt"
	"github.com/urmzd/opentag/pkg/agentspec"
)

// The two seams a registry has to reach, and the direction of the dependency.
//
// internal/server declares the store shape it needs and pkg/runtime declares the
// resolver shape it needs; neither imports this package, because a transport
// that knew about a storage implementation would be a transport you could only
// deploy one way. The adaptation therefore lives here, and a composition root
// wires it:
//
//	store := registry.NewMemory()
//	srv, _ := server.New(server.WithAgentStore(registry.ServerStore(store)))
//	rt, _ := runtime.New(..., registry.NewSpecs(store))
//
// pkg/runtime is not imported even so. Its Specs is an interface of two
// methods, and Go satisfies interfaces structurally, so Specs below fits it
// without this package taking a dependency on durable execution to hand out a
// definition.

// ServerStore adapts a Store to internal/server.Store.
//
// Two things happen at this boundary and nothing else does. The edge's Spec —
// a plain document it does not interpret — becomes an agentspec.AgentSpec,
// which is the type that knows what a valid definition is; and this package's
// sentinels become the server's, so that every method answers with the same
// status code. The translation joins both errors rather than replacing one with
// the other, so a caller matching either registry.ErrNotFound or
// server.ErrNotFound gets the answer it expects and the message keeps the
// detail the registry put in it.
func ServerStore(s Store) server.Store { return serverStore{store: s} }

type serverStore struct{ store Store }

var _ server.Store = serverStore{}

func (a serverStore) Create(ctx context.Context, tenant string, spec server.Spec, by string) (server.Revision, error) {
	rev, err := a.store.Create(ctx, tenant, fromServerSpec(spec), by)
	if err != nil {
		return server.Revision{}, serverError(err)
	}
	return toServerRevision(rev), nil
}

func (a serverStore) Revise(ctx context.Context, tenant string, spec server.Spec, expectedRev int, by string) (server.Revision, error) {
	rev, err := a.store.Revise(ctx, tenant, fromServerSpec(spec), expectedRev, by)
	if err != nil {
		return server.Revision{}, serverError(err)
	}
	return toServerRevision(rev), nil
}

func (a serverStore) Get(ctx context.Context, tenant, name string, rev int) (server.Revision, error) {
	got, err := a.store.Get(ctx, tenant, name, rev)
	if err != nil {
		return server.Revision{}, serverError(err)
	}
	return toServerRevision(got), nil
}

func (a serverStore) List(ctx context.Context, tenant string, page server.Page) ([]server.Revision, string, error) {
	revs, next, err := a.store.List(ctx, tenant, Page{Size: page.Size, Token: page.Token})
	if err != nil {
		return nil, "", serverError(err)
	}
	out := make([]server.Revision, len(revs))
	for i, rev := range revs {
		out[i] = toServerRevision(rev)
	}
	return out, next, nil
}

func (a serverStore) History(ctx context.Context, tenant, name string) ([]server.Revision, error) {
	revs, err := a.store.History(ctx, tenant, name)
	if err != nil {
		return nil, serverError(err)
	}
	out := make([]server.Revision, len(revs))
	for i, rev := range revs {
		out[i] = toServerRevision(rev)
	}
	return out, nil
}

func (a serverStore) Delete(ctx context.Context, tenant, name string) error {
	return serverError(a.store.Delete(ctx, tenant, name))
}

// serverError maps a registry sentinel onto the server's. An error that matches
// none is passed through: the server classifies what it does not recognise as
// internal, which is the right answer for a storage failure nobody anticipated.
func serverError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotFound):
		return fmt.Errorf("%w: %w", server.ErrNotFound, err)
	case errors.Is(err, ErrExists):
		return fmt.Errorf("%w: %w", server.ErrExists, err)
	case errors.Is(err, ErrConflict):
		return fmt.Errorf("%w: %w", server.ErrConflict, err)
	case errors.Is(err, ErrInvalid), errors.Is(err, agentspec.ErrInvalid):
		return fmt.Errorf("%w: %w", server.ErrInvalid, err)
	default:
		return err
	}
}

func fromServerSpec(s server.Spec) agentspec.AgentSpec {
	out := agentspec.AgentSpec{
		Name:         s.Name,
		Description:  s.Description,
		Model:        s.Model,
		Provider:     s.Provider,
		SystemPrompt: s.SystemPrompt,
		Tools:        s.Tools,
		Access: agentspec.Access{
			Spawn:          s.Access.Spawn,
			WorkspaceAreas: s.Access.WorkspaceAreas,
		},
	}
	for _, src := range s.Sources {
		out.Sources = append(out.Sources, agentspec.Source{
			Name:    src.Name,
			URI:     src.URI,
			Options: src.Options,
		})
	}
	// The store owns its memory, and this spec's slices belong to whoever
	// decoded the request. Normalize copies every one of them, so the store
	// never holds a slice the edge can still write to.
	return out.Normalize()
}

func toServerSpec(s agentspec.AgentSpec) server.Spec {
	out := server.Spec{
		Name:         s.Name,
		Description:  s.Description,
		Model:        s.Model,
		Provider:     s.Provider,
		SystemPrompt: s.SystemPrompt,
		Tools:        s.Tools,
		Access: server.Access{
			Spawn:          s.Access.Spawn,
			WorkspaceAreas: s.Access.WorkspaceAreas,
		},
	}
	for _, src := range s.Sources {
		out.Sources = append(out.Sources, server.Source{
			Name:    src.Name,
			URI:     src.URI,
			Options: src.Options,
		})
	}
	return out
}

func toServerRevision(r agentspec.Revision) server.Revision {
	return server.Revision{
		Spec:      toServerSpec(r.Spec),
		Rev:       r.Rev,
		Hash:      r.Hash,
		CreatedAt: r.CreatedAt,
		CreatedBy: r.CreatedBy,
	}
}

// Specs resolves agent definitions for pkg/runtime. It satisfies runtime.Specs
// structurally.
//
// The two methods are the two moments a run has an opinion about time. Latest
// is asked once, at accept time, and is the only place "current" is ever
// resolved; At is asked on every execution with the number the run pinned, and
// must answer identically forever — after the agent has been revised past that
// number and after it has been deleted. The registry's tombstone is what makes
// the second promise keepable, and this type is where the promise is spent.
type Specs struct{ store Store }

// NewSpecs returns the revision resolver for store.
func NewSpecs(store Store) *Specs { return &Specs{store: store} }

// Latest resolves the current definition of an agent. It is a control-plane
// read: a deleted agent has no current definition and reports ErrNotFound.
func (s *Specs) Latest(ctx context.Context, tenant, agent string) (agentrt.Revision, error) {
	return s.at(ctx, tenant, agent, 0)
}

// At resolves one pinned revision, deleted or not.
func (s *Specs) At(ctx context.Context, tenant, agent string, rev int) (agentrt.Revision, error) {
	if rev < agentspec.FirstRev {
		return agentrt.Revision{}, fmt.Errorf("%w: a pinned revision counts from %d, got %d", ErrInvalid, agentspec.FirstRev, rev)
	}
	return s.at(ctx, tenant, agent, rev)
}

func (s *Specs) at(ctx context.Context, tenant, agent string, rev int) (agentrt.Revision, error) {
	got, err := s.store.Get(ctx, tenant, agent, rev)
	if err != nil {
		return agentrt.Revision{}, err
	}
	return toRuntimeRevision(got), nil
}

func toRuntimeRevision(r agentspec.Revision) agentrt.Revision {
	spec := agentrt.Spec{
		Name:         r.Spec.Name,
		Description:  r.Spec.Description,
		Model:        r.Spec.Model,
		Provider:     r.Spec.Provider,
		SystemPrompt: r.Spec.SystemPrompt,
		Tools:        r.Spec.Tools,
		Access: agentrt.Access{
			Spawn:          r.Spec.Access.Spawn,
			WorkspaceAreas: r.Spec.Access.WorkspaceAreas,
		},
	}
	for _, src := range r.Spec.Sources {
		spec.Sources = append(spec.Sources, agentrt.Source{
			Name:    src.Name,
			URI:     src.URI,
			Options: src.Options,
		})
	}
	return agentrt.Revision{
		Spec:      spec,
		Rev:       r.Rev,
		Hash:      r.Hash,
		CreatedAt: r.CreatedAt,
		CreatedBy: r.CreatedBy,
	}
}
