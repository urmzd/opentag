package agentspec_test

import (
	"testing"
	"time"

	"github.com/urmzd/opentag/pkg/agentspec"
)

// The hash is compared across processes, restarts and replays, so it must be a
// function of the content and of nothing that varies between computations.
func TestHashIsStableAcrossRepeatedComputation(t *testing.T) {
	t.Parallel()
	spec := valid()
	spec.Sources[0].Options = map[string]string{
		"alpha": "1", "beta": "2", "gamma": "3", "delta": "4",
		"epsilon": "5", "zeta": "6", "eta": "7", "theta": "8",
	}
	want := spec.Hash()
	// Go randomizes map iteration order per range, so a hash that digested
	// options in range order would diverge within a handful of attempts.
	for i := range 200 {
		if got := spec.Hash(); got != want {
			t.Fatalf("attempt %d hashed to %s, want %s; the encoding depends on map iteration order", i, got, want)
		}
	}
}

// Set-valued fields are sets. Reordering one is not an edit, and a store that
// thought it was would append a revision every time a client serialized its
// tools differently.
func TestHashIsIndependentOfSetOrder(t *testing.T) {
	t.Parallel()
	a := valid()
	a.Tools = []string{"fetch", "search", "grep"}
	a.Access.Spawn = []string{"linter", "auditor"}
	a.Access.WorkspaceAreas = []string{"docs/", "spec/"}
	a.Sources = []agentspec.Source{
		{Name: "handbook", URI: "s3://h", Options: map[string]string{"a": "1", "b": "2"}},
		{Name: "adr", URI: "s3://a"},
	}

	b := valid()
	b.Tools = []string{"search", "grep", "fetch"}
	b.Access.Spawn = []string{"auditor", "linter"}
	b.Access.WorkspaceAreas = []string{"spec/", "docs/"}
	b.Sources = []agentspec.Source{
		{Name: "adr", URI: "s3://a"},
		{Name: "handbook", URI: "s3://h", Options: map[string]string{"b": "2", "a": "1"}},
	}

	if a.Hash() != b.Hash() {
		t.Fatalf("reordering sets changed the hash:\n%s\n%s", a.Hash(), b.Hash())
	}
	if !a.SameContent(b) {
		t.Fatal("SameContent disagrees with Hash")
	}
}

// Every semantic difference must be visible in the content address, or a revise
// that changed something real would be swallowed as a no-op.
func TestHashDiffersForEveryMeaningfulChange(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*agentspec.AgentSpec)
	}{
		{"name", func(s *agentspec.AgentSpec) { s.Name = "other-bot" }},
		{"description", func(s *agentspec.AgentSpec) { s.Description = "different" }},
		{"model", func(s *agentspec.AgentSpec) { s.Model = "claude-opus-4" }},
		{"provider", func(s *agentspec.AgentSpec) { s.Provider = agentspec.ProviderOllama }},
		{"system prompt", func(s *agentspec.AgentSpec) { s.SystemPrompt = "Answer differently." }},
		{"a tool added", func(s *agentspec.AgentSpec) { s.Tools = append(s.Tools, "grep") }},
		{"a tool removed", func(s *agentspec.AgentSpec) { s.Tools = s.Tools[:1] }},
		{"a tool renamed", func(s *agentspec.AgentSpec) { s.Tools[0] = "grep" }},
		{"a source added", func(s *agentspec.AgentSpec) {
			s.Sources = append(s.Sources, agentspec.Source{Name: "adr", URI: "s3://a"})
		}},
		{"a source uri", func(s *agentspec.AgentSpec) { s.Sources[0].URI = "s3://elsewhere" }},
		{"a source option value", func(s *agentspec.AgentSpec) { s.Sources[0].Options["k"] = "different" }},
		{"a source option key", func(s *agentspec.AgentSpec) {
			delete(s.Sources[0].Options, "k")
			s.Sources[0].Options["k2"] = "v"
		}},
		{"a source option removed", func(s *agentspec.AgentSpec) { s.Sources[0].Options = nil }},
		{"a spawn grant", func(s *agentspec.AgentSpec) { s.Access.Spawn = append(s.Access.Spawn, "deployer") }},
		{"a workspace area", func(s *agentspec.AgentSpec) { s.Access.WorkspaceAreas = []string{"secrets/"} }},
	}
	base := valid().Hash()
	seen := map[string]string{base: "unchanged"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			spec := valid().Clone()
			tc.edit(&spec)
			got := spec.Hash()
			if got == base {
				t.Fatalf("changing %s did not change the hash", tc.name)
			}
			if prior, dup := seen[got]; dup {
				t.Fatalf("changing %s collides with %s", tc.name, prior)
			}
			seen[got] = tc.name
		})
	}
}

// Authorship and time are not content. A resubmission of the same definition by
// a different person at a different moment is the same definition.
func TestHashExcludesAuthorshipAndTime(t *testing.T) {
	t.Parallel()
	spec := valid()
	first, err := agentspec.NewRevision(spec, 1, "ada", time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewRevision: %v", err)
	}
	second, err := agentspec.NewRevision(spec, 2, "grace", time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("NewRevision: %v", err)
	}
	if first.Hash != second.Hash {
		t.Fatalf("who and when leaked into the content address: %s != %s", first.Hash, second.Hash)
	}
}

// The reason every field is length-prefixed. Under plain concatenation each of
// these pairs would produce one byte stream and two different agents would
// share a content address, so an edit would be indistinguishable from a no-op.
func TestLengthPrefixingPreventsFieldBoundaryCollisions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a, b func(*agentspec.AgentSpec)
	}{
		{
			name: "a character moved from the name into the model",
			a:    func(s *agentspec.AgentSpec) { s.Name = "ab"; s.Model = "c" },
			b:    func(s *agentspec.AgentSpec) { s.Name = "a"; s.Model = "bc" },
		},
		{
			name: "a character moved from the model into the prompt",
			a:    func(s *agentspec.AgentSpec) { s.Model = "mx"; s.SystemPrompt = "y" },
			b:    func(s *agentspec.AgentSpec) { s.Model = "m"; s.SystemPrompt = "xy" },
		},
		{
			name: "one tool split into two",
			a:    func(s *agentspec.AgentSpec) { s.Tools = []string{"ab"} },
			b:    func(s *agentspec.AgentSpec) { s.Tools = []string{"a", "b"} },
		},
		{
			name: "a source name that swallows its uri",
			a: func(s *agentspec.AgentSpec) {
				s.Sources = []agentspec.Source{{Name: "ab", URI: "c"}}
			},
			b: func(s *agentspec.AgentSpec) {
				s.Sources = []agentspec.Source{{Name: "a", URI: "bc"}}
			},
		},
		{
			name: "an option key that swallows its value",
			a: func(s *agentspec.AgentSpec) {
				s.Sources[0].Options = map[string]string{"ab": "c"}
			},
			b: func(s *agentspec.AgentSpec) {
				s.Sources[0].Options = map[string]string{"a": "bc"}
			},
		},
		{
			name: "a spawn grant that borrows from a workspace area",
			a: func(s *agentspec.AgentSpec) {
				s.Access = agentspec.Access{Spawn: []string{"ab"}, WorkspaceAreas: []string{"c"}}
			},
			b: func(s *agentspec.AgentSpec) {
				s.Access = agentspec.Access{Spawn: []string{"a"}, WorkspaceAreas: []string{"bc"}}
			},
		},
		{
			name: "the last tool merged into the first spawn grant",
			a: func(s *agentspec.AgentSpec) {
				s.Tools = []string{"ab"}
				s.Access = agentspec.Access{Spawn: []string{"c"}}
			},
			b: func(s *agentspec.AgentSpec) {
				s.Tools = []string{"a"}
				s.Access = agentspec.Access{Spawn: []string{"bc"}}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, b := valid().Clone(), valid().Clone()
			tc.a(&a)
			tc.b(&b)
			if a.Hash() == b.Hash() {
				t.Fatalf("two definitions collide on %s: %s", tc.name, a.Hash())
			}
		})
	}
}

// The prefix is part of the value so that a future algorithm change is visible
// in stored data rather than reinterpreting old hashes as new ones.
func TestHashIsSelfDescribing(t *testing.T) {
	t.Parallel()
	h := valid().Hash()
	if !agentspec.IsHash(h) {
		t.Fatalf("IsHash(%q) = false", h)
	}
	for _, bad := range []string{"", "sha256:", "sha256:zz", "md5:" + h[len("sha256:"):], h[len("sha256:"):]} {
		if agentspec.IsHash(bad) {
			t.Errorf("IsHash(%q) = true", bad)
		}
	}
}
