// Package agentspec is the canonical definition of an agent, and the rules
// that decide when two definitions are the same one.
//
// An agent is a document: a name, a model behind a provider, a system prompt,
// the tools it may call, the corpora it may retrieve from, and the authority it
// holds beyond talking. This package owns exactly three statements about that
// document — what a valid one is, what its canonical form is, and what its
// content hash is — and nothing else. It does not store definitions, does not
// version them over time, does not authorize anybody to write one, and cannot
// run one. Those are pkg/registry's, the control plane's, and pkg/agentrt's
// respectively.
//
// # Immutability, and why the hash is load-bearing
//
// A revision is immutable. Editing an agent never mutates a Spec; it appends
// revision N+1, and a run pins the revision it accepted at accept time and
// replays under that revision forever. duraturo replays a workflow from the
// top, so a definition that moved between attempts would make the second
// attempt diverge from the ledger that is supposed to be the truth about the
// first.
//
// Pinning a number is only half of that guarantee. The number says which
// revision, the hash says which content, and a durable run carries both: on
// every attempt pkg/runtime compares the hash it pinned against the hash the
// control plane now reports for that number, and fails terminally when they
// differ. That check is the only thing standing between "an immutable revision
// changed" and a run that quietly executes a definition nobody agreed to, so
// the hash has to be a property of the content and of nothing else — not of a
// timestamp, not of an author, not of the order a map happened to iterate in.
//
// # Canonicalization
//
// Two definitions that differ only in presentation are the same definition.
// Normalize decides what presentation means:
//
//   - The name is lower-cased. Agents are addressed case-insensitively, and
//     "Docs-Bot" and "docs-bot" must not be two agents.
//   - Surrounding whitespace is trimmed from the free-text fields. A system
//     prompt that gained a trailing newline in an editor is not a new
//     definition, and treating it as one would burn a revision number.
//   - Tools, spawn grants, workspace areas and sources are sets, not
//     sequences: nothing downstream consumes them in order, so they are sorted.
//     A reordered tool list is not an edit.
//
// Hash then digests the canonical form with a length-prefixed encoding
// described in hash.go. Together they give the property the registry depends
// on: a revise whose content canonicalizes to the current revision is a no-op,
// not revision N+1.
//
// # Orthogonality
//
// The type is declared here rather than imported from pkg/agentrt, and
// pkg/agentrt declares its own rather than importing this one, on purpose.
// Building a running agent should depend on the definition of one and not on
// how definitions are validated, hashed, stored or authorized; validating and
// hashing a definition should not drag an LLM SDK into a linter or a control
// plane. The two shapes mirror opentag.v1.AgentSpec field for field, so the
// conversion at the seam is assignment (see pkg/registry).
//
// This package is a leaf: the standard library, plus pkg/topic for the one
// thing it must not re-decide — whether a name can be addressed.
package agentspec

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/urmzd/opentag/pkg/topic"
)

// ErrInvalid reports a definition that will not be accepted. Callers match with
// errors.Is.
var ErrInvalid = errors.New("agentspec: invalid")

// FirstRev is the revision number a new agent's first definition takes.
// Revisions count from one so that a zero Rev is unmistakably "unset" rather
// than an off-by-one that reads as the first revision.
const FirstRev = 1

// MaxNameLen bounds an agent name. A name becomes a topic segment, which
// becomes part of a stream key and of a URL path, so it is bounded here rather
// than discovered as a limit by whichever transport truncates first.
const MaxNameLen = 64

// Providers the built-in runtime can resolve.
//
// The names mirror pkg/agentrt's, and are declared rather than imported so that
// validating a definition costs no LLM client. The set is open in the same way:
// a deployment that registers its own provider validates against it with
// ValidateWith, because a definition that cannot be stored is a worse failure
// than one that fails at build time with a message listing what exists.
const (
	// ProviderAnthropic is the hosted Anthropic API.
	ProviderAnthropic = "anthropic"
	// ProviderOllama is a local Ollama daemon.
	ProviderOllama = "ollama"
	// ProviderOffline replays a canned script instead of calling a model.
	// It is what makes a deployment demonstrable with no API key.
	ProviderOffline = "offline"
)

// KnownProviders returns the built-in provider names, sorted. It returns a
// fresh slice so a caller extending it cannot edit this package's answer.
func KnownProviders() []string {
	return []string{ProviderAnthropic, ProviderOffline, ProviderOllama}
}

// AgentSpec is what an agent IS.
//
// It mirrors opentag.v1.AgentSpec, pkg/agentrt.Spec and internal/server.Spec
// field for field, so adaptation between them is assignment.
type AgentSpec struct {
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	Model        string   `json:"model"`
	Provider     string   `json:"provider"`
	SystemPrompt string   `json:"system_prompt"`
	Tools        []string `json:"tools,omitempty"`
	Sources      []Source `json:"sources,omitempty"`
	Access       Access   `json:"access,omitzero"`
}

// Source is one retrieval corpus the agent may ground answers in. Options are
// connector-specific and this package does not interpret them; it only insists
// that they hash deterministically.
type Source struct {
	Name    string            `json:"name"`
	URI     string            `json:"uri"`
	Options map[string]string `json:"options,omitempty"`
}

// Access is what the agent may do beyond talking, as two closed allowlists:
// everything not listed is denied. Spawn and WorkspaceAreas are separate
// because delegation and storage are separate hazards, and an agent that may
// write to a workspace area has not thereby earned the right to start other
// agents.
type Access struct {
	// Spawn names the agents this agent may delegate to.
	Spawn []string `json:"spawn,omitempty"`
	// WorkspaceAreas are the shared-workspace key prefixes it may use.
	WorkspaceAreas []string `json:"workspace_areas,omitempty"`
}

// Revision is one immutable version of an agent: the definition, which version
// it is, and the evidence that it has not changed since.
type Revision struct {
	Spec      AgentSpec `json:"spec"`
	Rev       int       `json:"rev"`
	Hash      string    `json:"hash"`
	CreatedAt time.Time `json:"created_at"`
	CreatedBy string    `json:"created_by"`
}

// NewRevision canonicalizes spec, validates it, and stamps it as revision rev.
//
// It is the only supported way to build a Revision, because the three steps are
// one step: a Revision whose Spec was not canonicalized would carry a hash of
// content it does not hold, and a store that skipped validation would accept an
// agent name that cannot be addressed on the bus.
func NewRevision(spec AgentSpec, rev int, by string, at time.Time) (Revision, error) {
	return NewRevisionWith(spec, rev, by, at, KnownProviders())
}

// NewRevisionWith is NewRevision for a deployment whose provider table is not
// the built-in one.
func NewRevisionWith(spec AgentSpec, rev int, by string, at time.Time, providers []string) (Revision, error) {
	if rev < FirstRev {
		return Revision{}, fmt.Errorf("%w: revision number %d is below %d; revisions count from %d", ErrInvalid, rev, FirstRev, FirstRev)
	}
	canonical := spec.Normalize()
	if err := canonical.ValidateWith(providers); err != nil {
		return Revision{}, err
	}
	return Revision{
		Spec:      canonical,
		Rev:       rev,
		Hash:      canonical.Hash(),
		CreatedAt: at.UTC(),
		CreatedBy: by,
	}, nil
}

// Validate reports whether r is a well-formed revision of a valid definition,
// including that its recorded hash is the hash of its own content.
//
// The hash check is not redundant with the one pkg/runtime makes. That one
// compares two revisions that claim to be the same revision; this one catches a
// single revision that is internally inconsistent — decoded from a corrupted
// record, or built by a store that mutated the spec after hashing it.
func (r Revision) Validate() error {
	if r.Rev < FirstRev {
		return fmt.Errorf("%w: revision number %d is below %d", ErrInvalid, r.Rev, FirstRev)
	}
	if err := r.Spec.Validate(); err != nil {
		return fmt.Errorf("%w (revision %d)", err, r.Rev)
	}
	if want := r.Spec.Hash(); r.Hash != "" && r.Hash != want {
		return fmt.Errorf("%w: agent %q revision %d records hash %s but its content hashes to %s; an immutable revision changed",
			ErrInvalid, r.Spec.Name, r.Rev, r.Hash, want)
	}
	return nil
}

// Clone returns a deep copy. A Revision carries slices and a map, so handing
// one to a caller without copying would let that caller edit a store's
// immutable history by accident.
func (r Revision) Clone() Revision {
	r.Spec = r.Spec.Clone()
	return r
}

// Clone returns a deep copy of the definition.
func (s AgentSpec) Clone() AgentSpec {
	s.Tools = slices.Clone(s.Tools)
	s.Access.Spawn = slices.Clone(s.Access.Spawn)
	s.Access.WorkspaceAreas = slices.Clone(s.Access.WorkspaceAreas)
	if s.Sources != nil {
		sources := make([]Source, len(s.Sources))
		for i, src := range s.Sources {
			src.Options = maps.Clone(src.Options)
			sources[i] = src
		}
		s.Sources = sources
	}
	return s
}

// NormalizeName is the canonical form of an agent name, and the key an agent is
// looked up by. It is exported because a store must resolve Get("Docs-Bot") to
// the agent it created as "docs-bot", and it must not reimplement the rule to
// do so.
func NormalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// Normalize returns the canonical form of the definition: names lower-cased,
// free text trimmed, set-valued fields sorted, and empty collections collapsed
// to nil so that a spec decoded from JSON with "tools": [] is the same
// definition as one without the field.
//
// It never reports an error and never drops content, including content
// Validate will reject. Dropping an empty tool name here would make the
// definition valid by deletion, which is exactly the kind of silent repair that
// leaves an operator wondering where their tool went.
func (s AgentSpec) Normalize() AgentSpec {
	out := AgentSpec{
		Name:         NormalizeName(s.Name),
		Description:  strings.TrimSpace(s.Description),
		Model:        strings.TrimSpace(s.Model),
		Provider:     NormalizeName(s.Provider),
		SystemPrompt: strings.TrimSpace(s.SystemPrompt),
		Tools:        normalizeSet(s.Tools, false),
		Access: Access{
			Spawn:          normalizeSet(s.Access.Spawn, true),
			WorkspaceAreas: normalizeSet(s.Access.WorkspaceAreas, false),
		},
	}
	if len(s.Sources) > 0 {
		sources := make([]Source, 0, len(s.Sources))
		for _, src := range s.Sources {
			sources = append(sources, Source{
				Name:    NormalizeName(src.Name),
				URI:     strings.TrimSpace(src.URI),
				Options: normalizeOptions(src.Options),
			})
		}
		slices.SortStableFunc(sources, func(a, b Source) int {
			if c := strings.Compare(a.Name, b.Name); c != 0 {
				return c
			}
			return strings.Compare(a.URI, b.URI)
		})
		out.Sources = sources
	}
	return out
}

// Validate reports whether the definition is one this deployment will accept,
// using the built-in provider table.
func (s AgentSpec) Validate() error { return s.ValidateWith(KnownProviders()) }

// ValidateWith is Validate against an explicit provider table.
//
// An empty table rejects every definition, which is the fail-closed reading: a
// deployment that can resolve no providers can execute no agents, and accepting
// definitions it will never be able to run only moves the failure to the first
// tag.
//
// Validation runs on the canonical form, so a caller may validate what it was
// given rather than having to remember to Normalize first. It deliberately does
// not resolve tools or sources: whether a tool is loaded is a property of a
// process, not of a definition, and a control plane that rejected a spec
// because one node had not been restarted yet would be enforcing an accident.
func (s AgentSpec) ValidateWith(providers []string) error {
	c := s.Normalize()
	switch {
	case c.Name == "":
		return fmt.Errorf("%w: the definition has no agent name", ErrInvalid)
	case len(c.Name) > MaxNameLen:
		return fmt.Errorf("%w: agent name %q is %d characters, the limit is %d", ErrInvalid, c.Name, len(c.Name), MaxNameLen)
	case c.Model == "":
		return fmt.Errorf("%w: agent %q names no model", ErrInvalid, c.Name)
	case c.Provider == "":
		return fmt.Errorf("%w: agent %q names no provider", ErrInvalid, c.Name)
	case c.SystemPrompt == "":
		return fmt.Errorf("%w: agent %q has an empty system prompt; an agent with no instructions is a definition of nothing", ErrInvalid, c.Name)
	}
	// The charset is pkg/topic's to decide, not this package's. An agent
	// name becomes a topic segment the moment the agent emits anything, so a
	// name that validated here and not there would produce an agent nobody
	// can subscribe to — discovered at the first run rather than at the
	// definition that caused it.
	if _, err := topic.Agent(c.Name); err != nil {
		return fmt.Errorf("%w: agent name %q is not addressable: %w", ErrInvalid, c.Name, err)
	}
	if !slices.ContainsFunc(providers, func(p string) bool { return strings.EqualFold(p, c.Provider) }) {
		return fmt.Errorf("%w: agent %q names provider %q; this deployment knows %s", ErrInvalid, c.Name, c.Provider, list(providers))
	}
	if err := uniqueNonEmpty("tool", c.Name, c.Tools); err != nil {
		return err
	}
	if err := uniqueNonEmpty("spawn grant", c.Name, c.Access.Spawn); err != nil {
		return err
	}
	if err := uniqueNonEmpty("workspace area", c.Name, c.Access.WorkspaceAreas); err != nil {
		return err
	}
	// A spawn grant names an agent, so it is bound by the same addressing
	// rule as the agent doing the spawning.
	for _, target := range c.Access.Spawn {
		if _, err := topic.Agent(target); err != nil {
			return fmt.Errorf("%w: agent %q may spawn %q, which is not an addressable agent name: %w", ErrInvalid, c.Name, target, err)
		}
	}
	seen := make(map[string]struct{}, len(c.Sources))
	for _, src := range c.Sources {
		switch {
		case src.Name == "":
			return fmt.Errorf("%w: agent %q has a source with no name", ErrInvalid, c.Name)
		case src.URI == "":
			return fmt.Errorf("%w: agent %q source %q has no uri", ErrInvalid, c.Name, src.Name)
		}
		if _, dup := seen[src.Name]; dup {
			return fmt.Errorf("%w: agent %q declares source %q twice; a source name is how the agent refers to a corpus and two corpora cannot answer to one name", ErrInvalid, c.Name, src.Name)
		}
		seen[src.Name] = struct{}{}
	}
	return nil
}

// uniqueNonEmpty enforces the two rules every set-valued field shares. The
// values arrive sorted from Normalize, so duplicates are adjacent.
func uniqueNonEmpty(what, agent string, values []string) error {
	for i, v := range values {
		if v == "" {
			return fmt.Errorf("%w: agent %q declares an empty %s name", ErrInvalid, agent, what)
		}
		if i > 0 && v == values[i-1] {
			return fmt.Errorf("%w: agent %q declares %s %q twice", ErrInvalid, agent, what, v)
		}
	}
	return nil
}

// normalizeSet trims, optionally lower-cases, and sorts a set-valued field,
// collapsing an empty one to nil.
func normalizeSet(values []string, lower bool) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	for _, v := range values {
		if lower {
			out = append(out, NormalizeName(v))
			continue
		}
		out = append(out, strings.TrimSpace(v))
	}
	slices.Sort(out)
	return out
}

// normalizeOptions copies a source's options, collapsing an empty map to nil.
// Keys and values are left exactly as given: they are a connector's vocabulary,
// and trimming or folding them here would change their meaning on someone
// else's behalf.
func normalizeOptions(options map[string]string) map[string]string {
	if len(options) == 0 {
		return nil
	}
	return maps.Clone(options)
}

// list renders a provider table for an error message.
func list(providers []string) string {
	if len(providers) == 0 {
		return "no providers"
	}
	sorted := slices.Clone(providers)
	slices.Sort(sorted)
	return strings.Join(sorted, ", ")
}
