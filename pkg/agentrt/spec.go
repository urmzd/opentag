package agentrt

import (
	"errors"
	"fmt"
	"time"
)

// ErrInvalid reports a revision this package cannot build an agent from.
// Callers match with errors.Is.
var ErrInvalid = errors.New("agentrt: invalid")

// Spec is what an agent IS: a name, a model behind a provider, a system
// prompt, the tools it may call, the sources it may retrieve from, and what it
// is allowed to do beyond talking.
//
// A Spec is immutable. Editing an agent does not mutate a Spec, it creates the
// next Revision, because a run pins the revision it accepted and replays under
// that revision forever.
//
// The type mirrors opentag.v1.AgentSpec field for field so the control plane's
// wire form converts by assignment. It is declared here, rather than imported
// from a control-plane package, so that building an agent depends only on the
// definition of one and not on how definitions are stored, versioned, or
// authorized.
type Spec struct {
	Name         string   `json:"name"`
	Description  string   `json:"description,omitempty"`
	Model        string   `json:"model"`
	Provider     string   `json:"provider"`
	SystemPrompt string   `json:"system_prompt,omitempty"`
	Tools        []string `json:"tools,omitempty"`
	Sources      []Source `json:"sources,omitempty"`
	Access       Access   `json:"access,omitempty"`
}

// Source is one retrieval corpus the agent may ground answers in.
type Source struct {
	Name    string            `json:"name"`
	URI     string            `json:"uri"`
	Options map[string]string `json:"options,omitempty"`
}

// Access is what the agent may do beyond talking, as two closed allowlists:
// everything not listed is denied. Spawn and WorkspaceAreas are separate
// because delegation and storage are separate hazards.
type Access struct {
	// Spawn names the agents this agent may delegate to.
	Spawn []string `json:"spawn,omitempty"`
	// WorkspaceAreas are the shared-workspace key prefixes it may use.
	WorkspaceAreas []string `json:"workspace_areas,omitempty"`
}

// Revision is one immutable version of an agent: the spec, plus which version
// it is and who put it there.
//
// Rev is the value a run pins and an event reports. Hash is the content hash of
// the spec, which lets a replaying run assert that the definition it is about
// to execute is the one it originally pinned — an immutable revision whose
// content changed is corruption, and it must fail loudly rather than quietly
// run something else.
type Revision struct {
	Spec      Spec      `json:"spec"`
	Rev       int       `json:"rev"`
	Hash      string    `json:"hash,omitempty"`
	CreatedAt time.Time `json:"created_at,omitempty"`
	CreatedBy string    `json:"created_by,omitempty"`
}

// Validate checks the fields this package needs to construct an agent. It does
// not judge the fields only the control plane cares about (description,
// authorship), and it does not resolve tools or sources: those depend on what a
// deployment has registered, and a spec naming a tool this process has not
// loaded is a deployment error, not an invalid definition.
func (r Revision) Validate() error {
	switch {
	case r.Rev <= 0:
		return fmt.Errorf("%w: revision number %d is not positive; a pinned revision counts from 1", ErrInvalid, r.Rev)
	case r.Spec.Name == "":
		return fmt.Errorf("%w: revision %d has no agent name", ErrInvalid, r.Rev)
	case r.Spec.Provider == "":
		return fmt.Errorf("%w: agent %q revision %d names no provider", ErrInvalid, r.Spec.Name, r.Rev)
	}
	return nil
}
