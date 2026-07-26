package agentspec_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/opentag/pkg/agentspec"
	"github.com/urmzd/opentag/pkg/topic"
)

// valid returns a definition every test can start from and mutate.
func valid() agentspec.AgentSpec {
	return agentspec.AgentSpec{
		Name:         "docs-bot",
		Description:  "answers questions about the docs",
		Model:        "claude-sonnet-4",
		Provider:     agentspec.ProviderAnthropic,
		SystemPrompt: "You answer from the docs.",
		Tools:        []string{"search", "fetch"},
		Sources: []agentspec.Source{
			{Name: "handbook", URI: "s3://corpus/handbook", Options: map[string]string{"k": "v", "a": "b"}},
		},
		Access: agentspec.Access{
			Spawn:          []string{"linter"},
			WorkspaceAreas: []string{"docs/"},
		},
	}
}

// A name becomes a topic segment, so the two must agree on what a name is. This
// is the property that keeps an accepted agent addressable on the bus.
func TestNameMustBeAddressableAsATopicSegment(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   string
		ok   bool
	}{
		{"letters digits dash underscore", "docs-bot_2", true},
		{"upper case is normalized, not rejected", "Docs-Bot", true},
		{"surrounding space is trimmed", "  docs-bot  ", true},
		{"empty", "", false},
		{"only whitespace", "   ", false},
		{"colon would deepen the topic", "docs:bot", false},
		{"space", "docs bot", false},
		{"slash", "docs/bot", false},
		{"dot", "docs.bot", false},
		{"star", "docs*", false},
		{"over the length limit", strings.Repeat("a", agentspec.MaxNameLen+1), false},
		{"at the length limit", strings.Repeat("a", agentspec.MaxNameLen), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := valid()
			spec.Name = tc.in
			err := spec.Validate()
			if tc.ok && err != nil {
				t.Fatalf("Validate(%q) = %v, want accepted", tc.in, err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("Validate(%q) accepted a name that is not a topic segment", tc.in)
			}
			if err != nil && !errors.Is(err, agentspec.ErrInvalid) {
				t.Fatalf("error %v does not match ErrInvalid", err)
			}
			if !tc.ok {
				return
			}
			// An accepted name must round-trip through the addressing
			// scheme it will be used in.
			if _, err := topic.Agent(spec.Normalize().Name); err != nil {
				t.Fatalf("accepted name %q is not addressable: %v", spec.Normalize().Name, err)
			}
		})
	}
}

// Case, whitespace and collection order are presentation. Two definitions that
// differ only in those ways are one definition.
func TestNormalizeCanonicalizesPresentation(t *testing.T) {
	t.Parallel()
	spec := agentspec.AgentSpec{
		Name:         "  Docs-Bot ",
		Description:  " answers ",
		Model:        " claude-sonnet-4 ",
		Provider:     "Anthropic",
		SystemPrompt: "You answer from the docs.\n",
		Tools:        []string{"search", "fetch"},
		Sources: []agentspec.Source{
			{Name: "Zeta", URI: "s3://z"},
			{Name: "alpha", URI: "s3://a"},
		},
		Access: agentspec.Access{
			Spawn:          []string{"Linter", "auditor"},
			WorkspaceAreas: []string{"z/", "a/"},
		},
	}
	got := spec.Normalize()
	if got.Name != "docs-bot" {
		t.Errorf("Name = %q, want %q", got.Name, "docs-bot")
	}
	if got.Description != "answers" {
		t.Errorf("Description = %q, want it trimmed", got.Description)
	}
	if got.Model != "claude-sonnet-4" {
		t.Errorf("Model = %q, want it trimmed", got.Model)
	}
	if got.Provider != agentspec.ProviderAnthropic {
		t.Errorf("Provider = %q, want it folded", got.Provider)
	}
	if got.SystemPrompt != "You answer from the docs." {
		t.Errorf("SystemPrompt = %q, want it trimmed", got.SystemPrompt)
	}
	if want := []string{"fetch", "search"}; !equal(got.Tools, want) {
		t.Errorf("Tools = %v, want %v", got.Tools, want)
	}
	if want := []string{"auditor", "linter"}; !equal(got.Access.Spawn, want) {
		t.Errorf("Spawn = %v, want %v", got.Access.Spawn, want)
	}
	if want := []string{"a/", "z/"}; !equal(got.Access.WorkspaceAreas, want) {
		t.Errorf("WorkspaceAreas = %v, want %v", got.Access.WorkspaceAreas, want)
	}
	if got.Sources[0].Name != "alpha" || got.Sources[1].Name != "zeta" {
		t.Errorf("Sources = %v, want them sorted and folded", got.Sources)
	}
	if !got.SameContent(got.Normalize()) {
		t.Error("Normalize is not idempotent")
	}
}

// An empty collection and an absent one are the same definition, so a spec
// decoded from JSON carrying "tools": [] must not look like an edit.
func TestNormalizeCollapsesEmptyCollectionsToNil(t *testing.T) {
	t.Parallel()
	spec := valid()
	spec.Tools = []string{}
	spec.Sources = []agentspec.Source{}
	spec.Access = agentspec.Access{Spawn: []string{}, WorkspaceAreas: []string{}}
	got := spec.Normalize()
	if got.Tools != nil || got.Sources != nil || got.Access.Spawn != nil || got.Access.WorkspaceAreas != nil {
		t.Fatalf("empty collections survived normalization: %+v", got)
	}
	bare := spec
	bare.Tools, bare.Sources, bare.Access = nil, nil, agentspec.Access{}
	if !bare.SameContent(spec) {
		t.Error("an empty collection hashes differently from an absent one")
	}
}

// Everything the control plane refuses, refused for a stated reason.
func TestValidateRejects(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		edit func(*agentspec.AgentSpec)
	}{
		{"no model", func(s *agentspec.AgentSpec) { s.Model = "" }},
		{"no provider", func(s *agentspec.AgentSpec) { s.Provider = "" }},
		{"unknown provider", func(s *agentspec.AgentSpec) { s.Provider = "gpt-9" }},
		{"empty system prompt", func(s *agentspec.AgentSpec) { s.SystemPrompt = "  " }},
		{"duplicate tool", func(s *agentspec.AgentSpec) { s.Tools = []string{"search", "search"} }},
		{"duplicate tool after trimming", func(s *agentspec.AgentSpec) { s.Tools = []string{"search", " search"} }},
		{"empty tool name", func(s *agentspec.AgentSpec) { s.Tools = []string{"search", ""} }},
		{"duplicate source name", func(s *agentspec.AgentSpec) {
			s.Sources = []agentspec.Source{{Name: "a", URI: "s3://1"}, {Name: "A", URI: "s3://2"}}
		}},
		{"source with no uri", func(s *agentspec.AgentSpec) {
			s.Sources = []agentspec.Source{{Name: "a"}}
		}},
		{"source with no name", func(s *agentspec.AgentSpec) {
			s.Sources = []agentspec.Source{{URI: "s3://1"}}
		}},
		{"duplicate spawn grant", func(s *agentspec.AgentSpec) {
			s.Access.Spawn = []string{"linter", "Linter"}
		}},
		{"spawn grant that is not an agent name", func(s *agentspec.AgentSpec) {
			s.Access.Spawn = []string{"other:agent"}
		}},
		{"duplicate workspace area", func(s *agentspec.AgentSpec) {
			s.Access.WorkspaceAreas = []string{"docs/", "docs/"}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := valid()
			tc.edit(&spec)
			err := spec.Validate()
			if err == nil {
				t.Fatalf("Validate accepted %s", tc.name)
			}
			if !errors.Is(err, agentspec.ErrInvalid) {
				t.Fatalf("error %v does not match ErrInvalid", err)
			}
		})
	}
}

// A deployment with its own provider must be able to store agents that use it,
// and an empty table must fail closed rather than accept everything.
func TestValidateWithUsesTheDeploymentsProviderTable(t *testing.T) {
	t.Parallel()
	spec := valid()
	spec.Provider = "in-house"
	if err := spec.Validate(); err == nil {
		t.Fatal("the built-in table accepted an unregistered provider")
	}
	if err := spec.ValidateWith([]string{"in-house"}); err != nil {
		t.Fatalf("ValidateWith([in-house]) = %v, want accepted", err)
	}
	if err := valid().ValidateWith(nil); err == nil {
		t.Fatal("an empty provider table accepted a definition; it must fail closed")
	}
}

// A caller may hand Validate whatever it received; canonicalization is not the
// caller's to remember.
func TestValidateAcceptsAnUncanonicalizedSpec(t *testing.T) {
	t.Parallel()
	spec := valid()
	spec.Name = " DOCS-BOT "
	spec.Tools = []string{"search", "fetch"}
	if err := spec.Validate(); err != nil {
		t.Fatalf("Validate = %v, want accepted", err)
	}
}

// NewRevision is the only way to build a Revision because the three steps are
// one step: canonicalize, validate, then hash what was stored.
func TestNewRevisionStoresTheCanonicalFormAndItsOwnHash(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 7, 25, 12, 0, 0, 0, time.FixedZone("x", 3600))
	spec := valid()
	spec.Name = "Docs-Bot"
	rev, err := agentspec.NewRevision(spec, agentspec.FirstRev, "urmzd", at)
	if err != nil {
		t.Fatalf("NewRevision: %v", err)
	}
	if rev.Spec.Name != "docs-bot" {
		t.Errorf("stored name %q, want the canonical form", rev.Spec.Name)
	}
	if rev.Hash != rev.Spec.Hash() {
		t.Errorf("Hash %q is not the hash of the stored content %q", rev.Hash, rev.Spec.Hash())
	}
	if !agentspec.IsHash(rev.Hash) {
		t.Errorf("Hash %q is not shaped like a content address", rev.Hash)
	}
	if rev.CreatedBy != "urmzd" {
		t.Errorf("CreatedBy = %q", rev.CreatedBy)
	}
	if rev.CreatedAt.Location() != time.UTC {
		t.Errorf("CreatedAt = %v, want it normalized to UTC", rev.CreatedAt)
	}
	if !rev.CreatedAt.Equal(at) {
		t.Errorf("CreatedAt = %v, want the same instant as %v", rev.CreatedAt, at)
	}
	if err := rev.Validate(); err != nil {
		t.Fatalf("a revision built here does not validate: %v", err)
	}
}

func TestNewRevisionRejectsANumberBelowTheFirst(t *testing.T) {
	t.Parallel()
	for _, rev := range []int{-1, 0} {
		if _, err := agentspec.NewRevision(valid(), rev, "urmzd", time.Now()); !errors.Is(err, agentspec.ErrInvalid) {
			t.Errorf("NewRevision(rev=%d) = %v, want ErrInvalid", rev, err)
		}
	}
}

// A revision whose recorded hash does not describe its own content is
// corruption, and must be loud.
func TestRevisionValidateCatchesContentThatMovedUnderItsHash(t *testing.T) {
	t.Parallel()
	rev, err := agentspec.NewRevision(valid(), 3, "urmzd", time.Now())
	if err != nil {
		t.Fatalf("NewRevision: %v", err)
	}
	rev.Spec.SystemPrompt = "You do something else entirely."
	if err := rev.Validate(); !errors.Is(err, agentspec.ErrInvalid) {
		t.Fatalf("Validate = %v, want ErrInvalid for a revision whose content moved", err)
	}
}

// Handing out a Revision must not hand out a handle on the store's memory.
func TestCloneIsDeep(t *testing.T) {
	t.Parallel()
	rev, err := agentspec.NewRevision(valid(), 1, "urmzd", time.Now())
	if err != nil {
		t.Fatalf("NewRevision: %v", err)
	}
	clone := rev.Clone()
	clone.Spec.Tools[0] = "mutated"
	clone.Spec.Sources[0].Options["k"] = "mutated"
	clone.Spec.Sources[0].Name = "mutated"
	clone.Spec.Access.Spawn[0] = "mutated"
	clone.Spec.Access.WorkspaceAreas[0] = "mutated"
	if rev.Hash != rev.Spec.Hash() {
		t.Fatalf("mutating a clone changed the original: %+v", rev.Spec)
	}
}

func equal(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
