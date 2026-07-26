package jira

import (
	"context"
	"fmt"
	"strconv"
	"sync"
)

// Fake is a complete in-memory Jira, not a stub: comments have ids and can be
// edited, an issue has a status that transitions actually move, the transitions
// offered depend on the status the issue is in, and an assignee can be set and
// cleared. It ships in the package so the mesh can be run end to end — a comment
// in, an edited comment and a moved ticket out — with no token and no network.
type Fake struct {
	mu       sync.Mutex
	comments []FakeComment
	byID     map[string]int
	status   map[string]string
	assignee map[string]string
	nextID   int64

	// Workflow is the status graph. The zero value uses DefaultWorkflow.
	Workflow map[string][]string

	// Err, when set, fails every call, which is how a test drives the retry
	// path.
	Err error
}

// FakeComment is one comment in the fake instance.
type FakeComment struct {
	ID   string
	Ref  Ref
	Body string
	// Edits counts body replacements, which is what "one comment, edited" is
	// asserted on.
	Edits int
}

// DefaultStatus is where an issue the fake has never seen starts.
const DefaultStatus = "To Do"

// DefaultWorkflow is a plausible three-column board: the statuses reachable from
// each status. It is a graph rather than a flat list because a transition list
// that does not depend on the current status would let a test pass that Jira
// would reject.
var DefaultWorkflow = map[string][]string{
	"To Do":       {"In Progress", "Done"},
	"In Progress": {"To Do", "Done"},
	"Done":        {"To Do"},
}

// NewFake returns an empty fake Jira.
func NewFake() *Fake {
	return &Fake{
		byID:     make(map[string]int),
		status:   make(map[string]string),
		assignee: make(map[string]string),
		nextID:   10000,
	}
}

// CreateComment implements API.
func (f *Fake) CreateComment(_ context.Context, ref Ref, body string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	f.nextID++
	id := strconv.FormatInt(f.nextID, 10)
	f.comments = append(f.comments, FakeComment{ID: id, Ref: ref, Body: body})
	f.byID[id] = len(f.comments) - 1
	return id, nil
}

// UpdateComment implements API.
func (f *Fake) UpdateComment(_ context.Context, _ Ref, id, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	i, ok := f.byID[id]
	if !ok {
		return &NotFoundError{What: "comment", ID: id}
	}
	f.comments[i].Body = body
	f.comments[i].Edits++
	return nil
}

// Transitions implements API, offering only the moves available from the status
// the issue is actually in.
func (f *Fake) Transitions(_ context.Context, ref Ref) ([]Transition, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return nil, f.Err
	}
	out := make([]Transition, 0, 3)
	for i, to := range f.workflow()[f.statusOf(ref)] {
		out = append(out, Transition{
			ID: strconv.Itoa(11 + i*10),
			// Jira's transition names and its status names are related but not
			// equal, which is exactly the confusion the action has to resolve.
			Name: transitionName(to),
			To:   to,
		})
	}
	return out, nil
}

// Transition implements API.
func (f *Fake) Transition(_ context.Context, ref Ref, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	for i, to := range f.workflow()[f.statusOf(ref)] {
		if strconv.Itoa(11+i*10) == id {
			f.status[ref.String()] = to
			return nil
		}
	}
	return &NotFoundError{What: "transition", ID: id}
}

// Assign implements API.
func (f *Fake) Assign(_ context.Context, ref Ref, accountID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	if accountID == "" {
		delete(f.assignee, ref.String())
		return nil
	}
	f.assignee[ref.String()] = accountID
	return nil
}

// Comments returns every comment, oldest first.
func (f *Fake) Comments() []FakeComment {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeComment(nil), f.comments...)
}

// Status returns an issue's current status.
func (f *Fake) Status(ref Ref) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statusOf(ref)
}

// SetStatus puts an issue into a status, so a test can start from one.
func (f *Fake) SetStatus(ref Ref, status string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status[ref.String()] = status
}

// Assignee returns an issue's assignee account id, empty when unassigned.
func (f *Fake) Assignee(ref Ref) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.assignee[ref.String()]
}

func (f *Fake) statusOf(ref Ref) string {
	if s, ok := f.status[ref.String()]; ok {
		return s
	}
	return DefaultStatus
}

func (f *Fake) workflow() map[string][]string {
	if f.Workflow != nil {
		return f.Workflow
	}
	return DefaultWorkflow
}

func transitionName(to string) string {
	switch to {
	case "In Progress":
		return "Start Progress"
	case "To Do":
		return "Reopen"
	default:
		return to
	}
}

// NotFoundError is the fake's 404, so a test can tell "Jira said no" from "the
// connector lost track of which comment it owned".
type NotFoundError struct {
	What string
	ID   string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("jira: no such %s %q", e.What, e.ID)
}
