package registry_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/agentspec"
	"github.com/urmzd/mandatum/pkg/registry"
)

const tenantA, tenantB = "acme", "globex"

// spec returns a valid definition whose content is a function of prompt, so a
// test can produce "the same definition" or "a different one" on demand.
func spec(name, prompt string) agentspec.AgentSpec {
	return agentspec.AgentSpec{
		Name:         name,
		Description:  "answers questions",
		Model:        "claude-haiku-5-5",
		Provider:     agentspec.ProviderOffline,
		SystemPrompt: prompt,
		Tools:        []string{"search"},
		Sources:      []agentspec.Source{{Name: "handbook", URI: "s3://h", Options: map[string]string{"k": "v"}}},
		Access:       agentspec.Access{Spawn: []string{"linter"}, WorkspaceAreas: []string{"docs/"}},
	}
}

// newStore returns a store whose clock advances a second per read, so that
// CreatedAt is deterministic and still strictly increasing.
func newStore(t *testing.T) *registry.Memory {
	t.Helper()
	var mu sync.Mutex
	at := time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)
	return registry.NewMemory(registry.WithClock(func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		at = at.Add(time.Second)
		return at
	}))
}

func mustCreate(t *testing.T, s *registry.Memory, tenant, name, prompt string) agentspec.Revision {
	t.Helper()
	rev, err := s.Create(context.Background(), tenant, spec(name, prompt), "ada")
	if err != nil {
		t.Fatalf("Create(%s/%s): %v", tenant, name, err)
	}
	return rev
}

func mustRevise(t *testing.T, s *registry.Memory, tenant, name, prompt string, expected int) agentspec.Revision {
	t.Helper()
	rev, err := s.Revise(context.Background(), tenant, spec(name, prompt), expected, "grace")
	if err != nil {
		t.Fatalf("Revise(%s/%s): %v", tenant, name, err)
	}
	return rev
}

// The append-only property: revising produces N+1 and leaves N exactly as it
// was. A run pinned to N replays against those bytes forever.
func TestReviseAppendsAndLeavesTheEarlierRevisionIdentical(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	first := mustCreate(t, s, tenantA, "docs-bot", "v1")
	if first.Rev != agentspec.FirstRev {
		t.Fatalf("Create returned revision %d, want %d", first.Rev, agentspec.FirstRev)
	}
	second := mustRevise(t, s, tenantA, "docs-bot", "v2", 0)
	if second.Rev != 2 {
		t.Fatalf("Revise returned revision %d, want 2", second.Rev)
	}
	if second.Hash == first.Hash {
		t.Fatal("a changed definition kept its content hash")
	}

	got, err := s.Get(ctx, tenantA, "docs-bot", 1)
	if err != nil {
		t.Fatalf("Get(rev 1): %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("revision 1 changed under a revise:\n got %+v\nwant %+v", got, first)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("stored revision does not validate: %v", err)
	}
	if !got.CreatedAt.Before(second.CreatedAt) {
		t.Errorf("revision 1 was created at %v, revision 2 at %v", got.CreatedAt, second.CreatedAt)
	}
	if got.CreatedBy != "ada" || second.CreatedBy != "grace" {
		t.Errorf("authorship = %q, %q", got.CreatedBy, second.CreatedBy)
	}
}

// Revision zero means "now"; an explicit revision means that revision and never
// anything else.
func TestGetZeroIsLatestAndGetNIsExactlyN(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	want := []agentspec.Revision{mustCreate(t, s, tenantA, "docs-bot", "v1")}
	for i, prompt := range []string{"v2", "v3", "v4"} {
		want = append(want, mustRevise(t, s, tenantA, "docs-bot", prompt, i+1))
	}
	for i, rev := range want {
		got, err := s.Get(ctx, tenantA, "docs-bot", i+1)
		if err != nil {
			t.Fatalf("Get(rev %d): %v", i+1, err)
		}
		if !reflect.DeepEqual(got, rev) {
			t.Fatalf("Get(rev %d) = %+v, want %+v", i+1, got, rev)
		}
	}
	latest, err := s.Get(ctx, tenantA, "docs-bot", 0)
	if err != nil {
		t.Fatalf("Get(rev 0): %v", err)
	}
	if !reflect.DeepEqual(latest, want[len(want)-1]) {
		t.Fatalf("Get(rev 0) = revision %d, want %d", latest.Rev, len(want))
	}
	if _, err := s.Get(ctx, tenantA, "docs-bot", len(want)+1); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("Get(rev %d) = %v, want ErrNotFound", len(want)+1, err)
	}
	if _, err := s.Get(ctx, tenantA, "docs-bot", -1); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("Get(rev -1) = %v, want ErrInvalid", err)
	}
	// The name is an address, and addresses are case-insensitive.
	if _, err := s.Get(ctx, tenantA, "Docs-Bot", 0); err != nil {
		t.Fatalf("Get with a differently-cased name: %v", err)
	}
}

// A revision is a version of a definition, not a count of saves.
func TestReviseWithIdenticalContentIsANoOp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	first := mustCreate(t, s, tenantA, "docs-bot", "v1")
	again := mustRevise(t, s, tenantA, "docs-bot", "v1", 0)
	if !reflect.DeepEqual(again, first) {
		t.Fatalf("resubmitting the same definition returned %+v, want the existing %+v", again, first)
	}

	// Presentation is not content: a reordered tool list and a re-indented
	// prompt are the same definition, and must not burn a revision.
	reordered := spec("docs-bot", "  v1  ")
	reordered.Tools = []string{"search"}
	reordered.Access.Spawn = []string{"Linter"}
	third, err := s.Revise(ctx, tenantA, reordered, 0, "grace")
	if err != nil {
		t.Fatalf("Revise: %v", err)
	}
	if third.Rev != first.Rev {
		t.Fatalf("a presentation-only change appended revision %d", third.Rev)
	}

	history, err := s.History(ctx, tenantA, "docs-bot")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 1 {
		t.Fatalf("history has %d revisions, want 1", len(history))
	}
}

// Optimistic concurrency: an editor that names the revision it edited must not
// be able to overwrite an edit it never saw.
func TestExpectedRevGuardsAgainstLostUpdates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	mustCreate(t, s, tenantA, "docs-bot", "v1")
	mustRevise(t, s, tenantA, "docs-bot", "v2", 1)

	// Two editors both read revision 1; the second one loses.
	_, err := s.Revise(ctx, tenantA, spec("docs-bot", "v2-conflicting"), 1, "ada")
	if !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("stale expectedRev = %v, want ErrConflict", err)
	}
	// Naming a revision that does not exist yet is equally a conflict.
	if _, err := s.Revise(ctx, tenantA, spec("docs-bot", "v3"), 9, "ada"); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("future expectedRev = %v, want ErrConflict", err)
	}
	if _, err := s.Revise(ctx, tenantA, spec("docs-bot", "v3"), -1, "ada"); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("negative expectedRev = %v, want ErrInvalid", err)
	}
	// A caller with a false belief about the current revision is told so
	// even when the content it submitted happens to match.
	if _, err := s.Revise(ctx, tenantA, spec("docs-bot", "v2"), 1, "ada"); !errors.Is(err, registry.ErrConflict) {
		t.Fatalf("stale expectedRev with identical content = %v, want ErrConflict", err)
	}
	// Zero is the unconditional append.
	if rev := mustRevise(t, s, tenantA, "docs-bot", "v3", 0); rev.Rev != 3 {
		t.Fatalf("expectedRev 0 produced revision %d, want 3", rev.Rev)
	}
	if rev := mustRevise(t, s, tenantA, "docs-bot", "v4", 3); rev.Rev != 4 {
		t.Fatalf("matching expectedRev produced revision %d, want 4", rev.Rev)
	}
}

// Two tenants may each have a docs-bot. They share a name and nothing else.
func TestTenantsAreIsolated(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	a := mustCreate(t, s, tenantA, "docs-bot", "acme prompt")
	mustRevise(t, s, tenantA, "docs-bot", "acme prompt 2", 1)
	b := mustCreate(t, s, tenantB, "docs-bot", "globex prompt")

	if a.Hash == b.Hash {
		t.Fatal("two tenants' definitions share a content hash; they are not the same agent")
	}
	if b.Rev != agentspec.FirstRev {
		t.Fatalf("the second tenant's agent started at revision %d, want %d", b.Rev, agentspec.FirstRev)
	}
	got, err := s.Get(ctx, tenantB, "docs-bot", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Spec.SystemPrompt != "globex prompt" {
		t.Fatalf("tenant %s reads %q, which belongs to %s", tenantB, got.Spec.SystemPrompt, tenantA)
	}
	if _, err := s.Get(ctx, tenantB, "docs-bot", 2); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("tenant %s can read %s's revision 2: %v", tenantB, tenantA, err)
	}

	// A name one tenant has taken is not taken for the other.
	if _, err := s.Create(ctx, tenantA, spec("docs-bot", "x"), "ada"); !errors.Is(err, registry.ErrExists) {
		t.Fatalf("recreating a live name = %v, want ErrExists", err)
	}

	// Deleting in one tenant does not touch the other.
	if err := s.Delete(ctx, tenantB, "docs-bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := s.Get(ctx, tenantA, "docs-bot", 0); err != nil {
		t.Fatalf("deleting %s's agent affected %s: %v", tenantB, tenantA, err)
	}

	// And no listing crosses the boundary.
	for tenant, want := range map[string]int{tenantA: 1, tenantB: 0} {
		revs, _, err := s.List(ctx, tenant, registry.Page{})
		if err != nil {
			t.Fatalf("List(%s): %v", tenant, err)
		}
		if len(revs) != want {
			t.Fatalf("List(%s) returned %d agents, want %d", tenant, len(revs), want)
		}
	}

	// An unscoped call is not a wildcard.
	if _, _, err := s.List(ctx, "", registry.Page{}); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("List with no tenant = %v, want ErrInvalid", err)
	}
	if _, err := s.Create(ctx, "", spec("docs-bot", "x"), "ada"); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("Create with no tenant = %v, want ErrInvalid", err)
	}
}

// THE REPLAY PROPERTY. Deleting an agent retires the name; it does not strand
// the runs that pinned a revision of it, because duraturo replays from the top
// and a run whose definition became unresolvable could never finish.
func TestDeleteTombstonesButPinnedRevisionsStillResolve(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	pinned := mustCreate(t, s, tenantA, "docs-bot", "v1")
	mustRevise(t, s, tenantA, "docs-bot", "v2", 1)
	if err := s.Delete(ctx, tenantA, "docs-bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Absent to everything that means "the agent now".
	if _, err := s.Get(ctx, tenantA, "docs-bot", 0); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("Get(rev 0) after delete = %v, want ErrNotFound", err)
	}
	if _, err := s.Revise(ctx, tenantA, spec("docs-bot", "v3"), 0, "ada"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("Revise after delete = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, tenantA, "docs-bot"); !errors.Is(err, registry.ErrNotFound) {
		t.Fatalf("second Delete = %v, want ErrNotFound", err)
	}
	revs, _, err := s.List(ctx, tenantA, registry.Page{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(revs) != 0 {
		t.Fatalf("List returned a deleted agent: %+v", revs)
	}

	// Present to everything that names a point in its past.
	got, err := s.Get(ctx, tenantA, "docs-bot", pinned.Rev)
	if err != nil {
		t.Fatalf("a run pinned to revision %d can no longer resolve it: %v", pinned.Rev, err)
	}
	if !reflect.DeepEqual(got, pinned) {
		t.Fatalf("revision %d changed across a delete", pinned.Rev)
	}
	if history, err := s.History(ctx, tenantA, "docs-bot"); err != nil || len(history) != 2 {
		t.Fatalf("History after delete = %d revisions, %v; want 2 and no error", len(history), err)
	}
}

// Re-creating a retired name must not hand an old run a new definition, so the
// numbering continues instead of restarting.
func TestRecreatingADeletedNameContinuesItsNumbering(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	pinned := mustCreate(t, s, tenantA, "docs-bot", "v1")
	mustRevise(t, s, tenantA, "docs-bot", "v2", 1)
	if err := s.Delete(ctx, tenantA, "docs-bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	revived, err := s.Create(ctx, tenantA, spec("docs-bot", "reborn"), "ada")
	if err != nil {
		t.Fatalf("Create on a retired name: %v", err)
	}
	if revived.Rev != 3 {
		t.Fatalf("the revived agent is revision %d, want 3; a reused number would resolve an old pin to a new definition", revived.Rev)
	}
	got, err := s.Get(ctx, tenantA, "docs-bot", pinned.Rev)
	if err != nil {
		t.Fatalf("Get(rev %d) after revival: %v", pinned.Rev, err)
	}
	if !reflect.DeepEqual(got, pinned) {
		t.Fatalf("revision %d changed across a delete and recreate", pinned.Rev)
	}
	if _, err := s.Get(ctx, tenantA, "docs-bot", 0); err != nil {
		t.Fatalf("the revived agent has no current revision: %v", err)
	}
	// Reviving with the definition that was already there is still a no-op:
	// the tombstone is lifted, the numbering is not spent.
	if err := s.Delete(ctx, tenantA, "docs-bot"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	again, err := s.Create(ctx, tenantA, spec("docs-bot", "reborn"), "ada")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if again.Rev != revived.Rev {
		t.Fatalf("reviving with unchanged content produced revision %d, want %d", again.Rev, revived.Rev)
	}
}

// Revision numbers are handed out under the lock, so a crowd of editors
// produces a contiguous sequence with no gaps and no duplicates.
func TestConcurrentReviseNumbersEveryRevisionExactlyOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)
	mustCreate(t, s, tenantA, "docs-bot", "v1")

	const writers = 64
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		revs = make([]int, 0, writers)
	)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rev, err := s.Revise(ctx, tenantA, spec("docs-bot", fmt.Sprintf("v-%d", i)), 0, "ada")
			if err != nil {
				t.Errorf("Revise: %v", err)
				return
			}
			mu.Lock()
			revs = append(revs, rev.Rev)
			mu.Unlock()
		}()
	}
	wg.Wait()

	seen := make(map[int]bool, len(revs))
	for _, rev := range revs {
		if seen[rev] {
			t.Fatalf("revision %d was handed out twice", rev)
		}
		seen[rev] = true
	}
	for want := 2; want <= writers+1; want++ {
		if !seen[want] {
			t.Fatalf("revision %d was never handed out; the sequence has a gap", want)
		}
	}
	history, err := s.History(ctx, tenantA, "docs-bot")
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != writers+1 {
		t.Fatalf("history has %d revisions, want %d", len(history), writers+1)
	}
	for i, rev := range history {
		if rev.Rev != i+1 {
			t.Fatalf("history[%d] is revision %d; history must be ordered and contiguous", i, rev.Rev)
		}
		if err := rev.Validate(); err != nil {
			t.Fatalf("history[%d] does not validate: %v", i, err)
		}
	}
}

// Concurrent creates of one name: exactly one wins, the rest are told the name
// is taken.
func TestConcurrentCreateElectsOneWinner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	const writers = 32
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		created int
	)
	for i := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Create(ctx, tenantA, spec("docs-bot", fmt.Sprintf("v-%d", i)), "ada")
			switch {
			case err == nil:
				mu.Lock()
				created++
				mu.Unlock()
			case errors.Is(err, registry.ErrExists):
			default:
				t.Errorf("Create: %v", err)
			}
		}()
	}
	wg.Wait()
	if created != 1 {
		t.Fatalf("%d creates succeeded, want exactly 1", created)
	}
}

// A listing walks every agent exactly once, in name order.
func TestPaginationCoversEveryAgentOnceInOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	const agents = 10
	for i := range agents {
		mustCreate(t, s, tenantA, fmt.Sprintf("agent-%02d", i), "v1")
	}

	var (
		seen  []string
		token string
		pages int
	)
	for {
		revs, next, err := s.List(ctx, tenantA, registry.Page{Size: 3, Token: token})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		pages++
		if pages > agents+1 {
			t.Fatal("the listing does not terminate")
		}
		for _, rev := range revs {
			seen = append(seen, rev.Spec.Name)
		}
		if next == "" {
			break
		}
		if len(revs) != 3 {
			t.Fatalf("a page with a continuation token returned %d agents, want the full 3", len(revs))
		}
		token = next
	}
	if len(seen) != agents {
		t.Fatalf("the listing returned %d agents, want %d: %v", len(seen), agents, seen)
	}
	for i, name := range seen {
		if want := fmt.Sprintf("agent-%02d", i); name != want {
			t.Fatalf("listing[%d] = %q, want %q; pages must be name-ordered and lossless", i, name, want)
		}
	}
}

// A token is a position, not an offset, so writes between pages cannot make a
// listing skip or repeat an agent that was there all along.
func TestAStaleTokenNeitherSkipsNorRepeats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	for i := range 6 {
		mustCreate(t, s, tenantA, fmt.Sprintf("agent-%02d", i), "v1")
	}
	first, token, err := s.List(ctx, tenantA, registry.Page{Size: 3})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(first) != 3 || token == "" {
		t.Fatalf("first page returned %d agents, token %q", len(first), token)
	}

	// The world moves under the cursor: one already-returned agent is
	// deleted, one is added before the cursor and one after it.
	if err := s.Delete(ctx, tenantA, "agent-00"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	mustCreate(t, s, tenantA, "agent-00b", "v1")
	mustCreate(t, s, tenantA, "agent-99", "v1")

	var rest []string
	for token != "" {
		revs, next, err := s.List(ctx, tenantA, registry.Page{Size: 3, Token: token})
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		for _, rev := range revs {
			rest = append(rest, rev.Spec.Name)
		}
		token = next
	}
	want := []string{"agent-03", "agent-04", "agent-05", "agent-99"}
	if len(rest) != len(want) {
		t.Fatalf("the rest of the listing = %v, want %v", rest, want)
	}
	for i := range want {
		if rest[i] != want[i] {
			t.Fatalf("the rest of the listing = %v, want %v", rest, want)
		}
	}
}

// A token belongs to the listing that issued it.
func TestAPageTokenIsOpaqueAndTenantBound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)
	for i := range 4 {
		mustCreate(t, s, tenantA, fmt.Sprintf("agent-%02d", i), "v1")
		mustCreate(t, s, tenantB, fmt.Sprintf("agent-%02d", i), "v1")
	}
	_, token, err := s.List(ctx, tenantA, registry.Page{Size: 2})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if token == "" {
		t.Fatal("no continuation token for a truncated listing")
	}
	if _, _, err := s.List(ctx, tenantB, registry.Page{Size: 2, Token: token}); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("a token replayed across a tenant boundary = %v, want ErrInvalid", err)
	}
	for _, bad := range []string{"not-base64!!", "AAAA", "eyJhIjoxfQ"} {
		if _, _, err := s.List(ctx, tenantA, registry.Page{Size: 2, Token: bad}); !errors.Is(err, registry.ErrInvalid) {
			t.Errorf("List with token %q = %v, want ErrInvalid", bad, err)
		}
	}
}

// The page size is a request for convenience, not an assertion about the world.
func TestPageSizeDefaultsAndClamps(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)
	for i := range registry.DefaultPageSize + 5 {
		mustCreate(t, s, tenantA, fmt.Sprintf("agent-%03d", i), "v1")
	}
	revs, next, err := s.List(ctx, tenantA, registry.Page{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(revs) != registry.DefaultPageSize || next == "" {
		t.Fatalf("List with no size returned %d agents (token %q), want %d and a token", len(revs), next, registry.DefaultPageSize)
	}
	revs, next, err = s.List(ctx, tenantA, registry.Page{Size: registry.MaxPageSize * 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(revs) != registry.DefaultPageSize+5 || next != "" {
		t.Fatalf("an oversized page returned %d agents (token %q), want them all", len(revs), next)
	}
}

// A store that hands out its own memory can be edited by anyone who reads it.
func TestReturnedRevisionsDoNotAliasStoredState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)

	submitted := spec("docs-bot", "v1")
	created, err := s.Create(ctx, tenantA, submitted, "ada")
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	// The caller still holds the slices it submitted.
	submitted.Tools[0] = "rm-rf"
	submitted.Sources[0].Options["k"] = "mutated"

	got, err := s.Get(ctx, tenantA, "docs-bot", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reflect.DeepEqual(got, created) {
		t.Fatal("editing the submitted spec changed the stored revision")
	}
	// And a reader can edit what it was given.
	got.Spec.Tools[0] = "rm-rf"
	got.Spec.Access.Spawn[0] = "anything"
	got.Spec.Sources[0].Options["k"] = "mutated"

	again, err := s.Get(ctx, tenantA, "docs-bot", 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !reflect.DeepEqual(again, created) {
		t.Fatalf("editing a returned revision changed the store: %+v", again.Spec)
	}
	if err := again.Validate(); err != nil {
		t.Fatalf("stored revision no longer validates: %v", err)
	}
}

// The store validates definitions rather than storing whatever it is handed,
// and reports it as ErrInvalid so the edge answers with the right status.
func TestInvalidDefinitionsAreRejected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)
	cases := map[string]agentspec.AgentSpec{
		"no name":          spec("", "v1"),
		"unaddressable":    spec("docs bot", "v1"),
		"no prompt":        spec("docs-bot", ""),
		"unknown provider": func() agentspec.AgentSpec { s := spec("docs-bot", "v1"); s.Provider = "gpt-9"; return s }(),
	}
	for name, bad := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := s.Create(ctx, tenantA, bad, "ada")
			if !errors.Is(err, registry.ErrInvalid) {
				t.Fatalf("Create = %v, want ErrInvalid", err)
			}
			if !errors.Is(err, agentspec.ErrInvalid) {
				t.Fatalf("Create = %v, want the definition's own error to survive wrapping", err)
			}
		})
	}
	if _, err := s.Get(ctx, tenantA, "  ", 0); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("Get with no name = %v, want ErrInvalid", err)
	}
	if _, err := s.History(ctx, tenantA, ""); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("History with no name = %v, want ErrInvalid", err)
	}
	if err := s.Delete(ctx, tenantA, ""); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("Delete with no name = %v, want ErrInvalid", err)
	}
}

// A deployment that registers its own provider must be able to store the agents
// that use it.
func TestWithProvidersWidensWhatIsStorable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := registry.NewMemory(registry.WithProviders("in-house"))
	custom := spec("docs-bot", "v1")
	custom.Provider = "in-house"
	if _, err := s.Create(ctx, tenantA, custom, "ada"); err != nil {
		t.Fatalf("Create with a registered provider: %v", err)
	}
	if _, err := s.Create(ctx, tenantA, spec("other-bot", "v1"), "ada"); !errors.Is(err, registry.ErrInvalid) {
		t.Fatalf("Create with a provider outside the table = %v, want ErrInvalid", err)
	}
}

// Missing agents are missing, whatever is asked of them.
func TestUnknownAgentsAreNotFound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newStore(t)
	if _, err := s.Get(ctx, tenantA, "ghost", 0); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("Get = %v, want ErrNotFound", err)
	}
	if _, err := s.History(ctx, tenantA, "ghost"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("History = %v, want ErrNotFound", err)
	}
	if err := s.Delete(ctx, tenantA, "ghost"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("Delete = %v, want ErrNotFound", err)
	}
	if _, err := s.Revise(ctx, tenantA, spec("ghost", "v1"), 0, "ada"); !errors.Is(err, registry.ErrNotFound) {
		t.Errorf("Revise = %v, want ErrNotFound", err)
	}
}

// A cancelled caller is not owed work.
func TestACancelledContextIsRefused(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	mustCreate(t, s, tenantA, "docs-bot", "v1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Get(ctx, tenantA, "docs-bot", 0); !errors.Is(err, context.Canceled) {
		t.Errorf("Get = %v, want context.Canceled", err)
	}
	if _, err := s.Create(ctx, tenantA, spec("other", "v1"), "ada"); !errors.Is(err, context.Canceled) {
		t.Errorf("Create = %v, want context.Canceled", err)
	}
	if _, _, err := s.List(ctx, tenantA, registry.Page{}); !errors.Is(err, context.Canceled) {
		t.Errorf("List = %v, want context.Canceled", err)
	}
}
