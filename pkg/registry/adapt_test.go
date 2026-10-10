package registry_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/urmzd/mandatum/internal/server"
	"github.com/urmzd/mandatum/pkg/agentrt"
	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/registry"
	"github.com/urmzd/mandatum/pkg/runtime"
)

// The two seams this package exists to fit. Both are compile-time claims, and
// both are the reason a composition root needs no glue of its own.
var (
	_ server.Store  = registry.ServerStore(registry.NewMemory())
	_ runtime.Specs = registry.NewSpecs(registry.NewMemory())
)

func serverSpec(name, prompt string) server.Spec {
	return server.Spec{
		Name:         name,
		Description:  "answers questions",
		Model:        "claude-haiku-5-5",
		Provider:     agentspec.ProviderOffline,
		SystemPrompt: prompt,
		Tools:        []string{"search", "fetch"},
		Sources:      []server.Source{{Name: "handbook", URI: "s3://h", Options: map[string]string{"k": "v"}}},
		Access:       server.Access{Spawn: []string{"linter"}, WorkspaceAreas: []string{"docs/"}},
	}
}

// Nothing may be lost at the edge: what the transport hands in is what comes
// back out, in canonical form.
func TestServerStoreRoundTripsEveryField(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := registry.ServerStore(registry.NewMemory())

	in := serverSpec("Docs-Bot", "v1")
	created, err := store.Create(ctx, tenantA, in, "ada")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.Rev != agentspec.FirstRev || created.Hash == "" || created.CreatedBy != "ada" || created.CreatedAt.IsZero() {
		t.Fatalf("Create returned %+v", created)
	}
	want := server.Spec{
		Name:         "docs-bot",
		Description:  "answers questions",
		Model:        "claude-haiku-5-5",
		Provider:     agentspec.ProviderOffline,
		SystemPrompt: "v1",
		Tools:        []string{"fetch", "search"},
		Sources:      []server.Source{{Name: "handbook", URI: "s3://h", Options: map[string]string{"k": "v"}}},
		Access:       server.Access{Spawn: []string{"linter"}, WorkspaceAreas: []string{"docs/"}},
	}
	if !reflect.DeepEqual(created.Spec, want) {
		t.Fatalf("stored spec =\n%+v\nwant\n%+v", created.Spec, want)
	}

	got, err := store.Get(ctx, tenantA, "docs-bot", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatalf("Get = %+v, want %+v", got, created)
	}

	revised, err := store.Revise(ctx, tenantA, serverSpec("docs-bot", "v2"), created.Rev, "grace")
	if err != nil {
		t.Fatalf("Revise: %v", err)
	}
	if revised.Rev != 2 {
		t.Fatalf("Revise returned revision %d, want 2", revised.Rev)
	}
	history, err := store.History(ctx, tenantA, "docs-bot")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 2 || history[0].Rev != 1 || history[1].Rev != 2 {
		t.Fatalf("History = %+v", history)
	}
	page, next, err := store.List(ctx, tenantA, server.Page{Size: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(page) != 1 || page[0].Rev != 2 || next != "" {
		t.Fatalf("List = %+v, token %q", page, next)
	}
	if err := store.Delete(ctx, tenantA, "docs-bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := store.Get(ctx, tenantA, "docs-bot", 1); err != nil {
		t.Fatalf("a pinned revision stopped resolving through the adapter: %v", err)
	}
}

// The edge classifies errors by sentinel, so every registry failure has to
// arrive wearing the server's. The registry's own error survives underneath,
// because the detail it carries is the part an operator reads.
func TestServerStoreTranslatesEverySentinel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := registry.ServerStore(registry.NewMemory())
	if _, err := store.Create(ctx, tenantA, serverSpec("docs-bot", "v1"), "ada"); err != nil {
		t.Fatalf("Create: %v", err)
	}

	cases := []struct {
		name   string
		do     func() error
		want   error
		origin error
	}{
		{
			name:   "missing agent",
			do:     func() error { _, err := store.Get(ctx, tenantA, "ghost", 0); return err },
			want:   server.ErrNotFound,
			origin: registry.ErrNotFound,
		},
		{
			name:   "name already taken",
			do:     func() error { _, err := store.Create(ctx, tenantA, serverSpec("docs-bot", "x"), "ada"); return err },
			want:   server.ErrExists,
			origin: registry.ErrExists,
		},
		{
			name:   "stale expected revision",
			do:     func() error { _, err := store.Revise(ctx, tenantA, serverSpec("docs-bot", "x"), 7, "ada"); return err },
			want:   server.ErrConflict,
			origin: registry.ErrConflict,
		},
		{
			name:   "invalid definition",
			do:     func() error { _, err := store.Create(ctx, tenantA, serverSpec("docs bot", "v1"), "ada"); return err },
			want:   server.ErrInvalid,
			origin: agentspec.ErrInvalid,
		},
		{
			name:   "unscoped call",
			do:     func() error { _, err := store.Get(ctx, "", "docs-bot", 0); return err },
			want:   server.ErrInvalid,
			origin: registry.ErrInvalid,
		},
		{
			name:   "unscoped listing",
			do:     func() error { _, _, err := store.List(ctx, "", server.Page{}); return err },
			want:   server.ErrInvalid,
			origin: registry.ErrInvalid,
		},
		{
			name:   "history of a missing agent",
			do:     func() error { _, err := store.History(ctx, tenantA, "ghost"); return err },
			want:   server.ErrNotFound,
			origin: registry.ErrNotFound,
		},
		{
			name:   "revising a missing agent",
			do:     func() error { _, err := store.Revise(ctx, tenantA, serverSpec("ghost", "x"), 0, "ada"); return err },
			want:   server.ErrNotFound,
			origin: registry.ErrNotFound,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.do()
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not match the server sentinel %v", err, tc.want)
			}
			if !errors.Is(err, tc.origin) {
				t.Fatalf("error %v lost the registry's own sentinel %v", err, tc.origin)
			}
		})
	}

	// A failure nobody anticipated must not be laundered into a client
	// error; the edge answers those as internal on purpose.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := store.Delete(ctx, tenantA, "docs-bot"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Delete = %v, want the underlying context error to pass through", err)
	}
}

// What a run needs: resolve "current" once, then resolve the number it pinned
// forever, including after the agent is revised past it and deleted.
func TestSpecsResolvesPinnedRevisionsForever(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := registry.NewMemory()
	specs := registry.NewSpecs(store)

	if _, err := store.Create(ctx, tenantA, spec("docs-bot", "v1"), "ada"); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pinned, err := specs.Latest(ctx, tenantA, "docs-bot")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if pinned.Rev != agentspec.FirstRev {
		t.Fatalf("Latest = revision %d, want %d", pinned.Rev, agentspec.FirstRev)
	}
	if err := pinned.Validate(); err != nil {
		t.Fatalf("the runtime cannot build an agent from what it was handed: %v", err)
	}
	want := agentrt.Spec{
		Name:         "docs-bot",
		Description:  "answers questions",
		Model:        "claude-haiku-5-5",
		Provider:     agentspec.ProviderOffline,
		SystemPrompt: "v1",
		Tools:        []string{"search"},
		Sources:      []agentrt.Source{{Name: "handbook", URI: "s3://h", Options: map[string]string{"k": "v"}}},
		Access:       agentrt.Access{Spawn: []string{"linter"}, WorkspaceAreas: []string{"docs/"}},
	}
	if !reflect.DeepEqual(pinned.Spec, want) {
		t.Fatalf("resolved spec =\n%+v\nwant\n%+v", pinned.Spec, want)
	}

	if _, err := store.Revise(ctx, tenantA, spec("docs-bot", "v2"), 1, "grace"); err != nil {
		t.Fatalf("Revise: %v", err)
	}
	if err := store.Delete(ctx, tenantA, "docs-bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	replayed, err := specs.At(ctx, tenantA, "docs-bot", pinned.Rev)
	if err != nil {
		t.Fatalf("a run pinned to revision %d cannot replay: %v", pinned.Rev, err)
	}
	if !reflect.DeepEqual(replayed, pinned) {
		t.Fatalf("replay resolved %+v, want the pinned %+v", replayed, pinned)
	}
	if replayed.Hash != pinned.Hash {
		t.Fatalf("the pinned hash moved: %s != %s", replayed.Hash, pinned.Hash)
	}

	// Latest is a control-plane read and a retired agent has no current
	// definition, so a new tag against it does not start.
	if _, err := specs.Latest(ctx, tenantA, "docs-bot"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("Latest on a retired agent = %v, want ErrNotFound", err)
	}
	// At never means "latest", whatever it is passed.
	if _, err := specs.At(ctx, tenantA, "docs-bot", 0); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("At(0) = %v, want ErrInvalid; a pinned revision counts from one", err)
	}
	if _, err := specs.At(ctx, tenantB, "docs-bot", 1); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("At across a tenant boundary = %v, want ErrNotFound", err)
	}
}
