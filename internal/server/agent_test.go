package server

import (
	"context"

	"fmt"
	"strings"
	"testing"

	"connectrpc.com/connect"

	opentagv1 "github.com/urmzd/opentag/gen/opentag/v1"
)

func specMsg(name string) *opentagv1.AgentSpec {
	return &opentagv1.AgentSpec{
		Name:         name,
		Description:  "answers questions about the docs",
		Model:        "claude-sonnet-4-6",
		Provider:     "anthropic",
		SystemPrompt: "be brief",
		Tools:        []string{"github_comment"},
		Sources: []*opentagv1.Source{{
			Name:    "docs",
			Uri:     "github://urmzd/opentag/docs",
			Options: map[string]string{"branch": "main"},
		}},
		Access: &opentagv1.Access{
			Spawn:          []string{"research-bot"},
			WorkspaceAreas: []string{"runs/docs-bot/"},
		},
	}
}

// An agent name is unique within a tenant and invisible outside it: two tenants
// may both own "docs-bot", and neither can read or revise the other's.
func TestAgentsAreScopedToTheCredentialsTenant(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()

	if _, err := h.agentClient(tokenAcme).CreateAgent(ctx, connect.NewRequest(&opentagv1.CreateAgentRequest{
		Spec: specMsg("docs-bot"),
	})); err != nil {
		t.Fatalf("acme create: %v", err)
	}

	_, err := h.agentClient(tokenOther).GetAgent(ctx, connect.NewRequest(&opentagv1.GetAgentRequest{Name: "docs-bot"}))
	requireCode(t, err, connect.CodeNotFound)

	// The name being taken in one tenant does not take it in another.
	created, err := h.agentClient(tokenOther).CreateAgent(ctx, connect.NewRequest(&opentagv1.CreateAgentRequest{
		Spec: specMsg("docs-bot"),
	}))
	if err != nil {
		t.Fatalf("other create: %v", err)
	}
	if got := created.Msg.GetRevision().GetRev(); got != 1 {
		t.Fatalf("want the other tenant's first revision to be 1, got %d", got)
	}
}

// Authorship is a property of the credential, not a field the caller fills in:
// the wire has no author field at all.
func TestRevisionAuthorshipComesFromTheCredential(t *testing.T) {
	h := newHarness(t, nil)
	res, err := h.agentClient(tokenAcme).CreateAgent(context.Background(), connect.NewRequest(&opentagv1.CreateAgentRequest{
		Spec: specMsg("docs-bot"),
	}))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := res.Msg.GetRevision().GetCreatedBy(); got != subjectAcme {
		t.Fatalf("want created_by %q, got %q", subjectAcme, got)
	}
}

// A revise appends: the previous revision stays readable at its own number, and
// its spec is untouched.
func TestRevisingAppendsAndLeavesHistoryReadable(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	client := h.agentClient(tokenAcme)

	if _, err := client.CreateAgent(ctx, connect.NewRequest(&opentagv1.CreateAgentRequest{Spec: specMsg("docs-bot")})); err != nil {
		t.Fatalf("create: %v", err)
	}
	revised := specMsg("docs-bot")
	revised.SystemPrompt = "be thorough"
	res, err := client.ReviseAgent(ctx, connect.NewRequest(&opentagv1.ReviseAgentRequest{Spec: revised, ExpectedRev: 1}))
	if err != nil {
		t.Fatalf("revise: %v", err)
	}
	if got := res.Msg.GetRevision().GetRev(); got != 2 {
		t.Fatalf("want rev 2, got %d", got)
	}

	first, err := client.GetAgent(ctx, connect.NewRequest(&opentagv1.GetAgentRequest{Name: "docs-bot", Rev: 1}))
	if err != nil {
		t.Fatalf("get rev 1: %v", err)
	}
	if got := first.Msg.GetRevision().GetSpec().GetSystemPrompt(); got != "be brief" {
		t.Fatalf("revision 1 changed underneath the revise: got prompt %q", got)
	}

	history, err := client.GetAgentHistory(ctx, connect.NewRequest(&opentagv1.GetAgentHistoryRequest{Name: "docs-bot"}))
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if got := len(history.Msg.GetRevisions()); got != 2 {
		t.Fatalf("want 2 revisions in history, got %d", got)
	}

	// A stale expected revision is a conflict, not a silent overwrite.
	_, err = client.ReviseAgent(ctx, connect.NewRequest(&opentagv1.ReviseAgentRequest{Spec: revised, ExpectedRev: 1}))
	requireCode(t, err, connect.CodeAborted)
}

// A spec survives the trip to the wire and back unchanged, including the fields
// a lazy conversion would drop: the repeated ones and the nested messages.
func TestAgentSpecRoundTripsThroughTheWire(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	client := h.agentClient(tokenAcme)

	want := specMsg("docs-bot")
	if _, err := client.CreateAgent(ctx, connect.NewRequest(&opentagv1.CreateAgentRequest{Spec: want})); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := client.GetAgent(ctx, connect.NewRequest(&opentagv1.GetAgentRequest{Name: "docs-bot"}))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	spec := got.Msg.GetRevision().GetSpec()
	if spec.GetDescription() != want.GetDescription() {
		t.Errorf("description: got %q, want %q", spec.GetDescription(), want.GetDescription())
	}
	if spec.GetModel() != want.GetModel() {
		t.Errorf("model: got %q, want %q", spec.GetModel(), want.GetModel())
	}
	if spec.GetProvider() != want.GetProvider() {
		t.Errorf("provider: got %q, want %q", spec.GetProvider(), want.GetProvider())
	}
	if strings.Join(spec.GetTools(), ",") != strings.Join(want.GetTools(), ",") {
		t.Errorf("tools: got %v, want %v", spec.GetTools(), want.GetTools())
	}
	if len(spec.GetSources()) != 1 || spec.GetSources()[0].GetOptions()["branch"] != "main" {
		t.Errorf("sources did not survive the round trip: %v", spec.GetSources())
	}
	if strings.Join(spec.GetAccess().GetSpawn(), ",") != "research-bot" {
		t.Errorf("access.spawn: got %v", spec.GetAccess().GetSpawn())
	}
	if strings.Join(spec.GetAccess().GetWorkspaceAreas(), ",") != "runs/docs-bot/" {
		t.Errorf("access.workspace_areas: got %v", spec.GetAccess().GetWorkspaceAreas())
	}
	if got := got.Msg.GetRevision().GetHash(); got == "" {
		t.Error("want the store's content hash on the revision, got none")
	}
}

// Every failure a store reports reaches the client as the status that describes
// it, and an unclassified failure reaches it with no detail at all.
func TestStoreFailuresMapToStatusCodes(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want connect.Code
	}{
		{"missing agent", fmt.Errorf("%w: agent", ErrNotFound), connect.CodeNotFound},
		{"name taken", fmt.Errorf("%w: agent", ErrExists), connect.CodeAlreadyExists},
		{"stale revision", fmt.Errorf("%w: agent", ErrConflict), connect.CodeAborted},
		{"unusable spec", fmt.Errorf("%w: tool", ErrInvalid), connect.CodeInvalidArgument},
		{"store down", fmt.Errorf("%w: dial tcp", ErrUnavailable), connect.CodeUnavailable},
		{"unclassified", errInjected, connect.CodeInternal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHarness(t, nil)
			h.store.err = tt.err
			_, err := h.agentClient(tokenAcme).CreateAgent(context.Background(),
				connect.NewRequest(&opentagv1.CreateAgentRequest{Spec: specMsg("docs-bot")}))
			requireCode(t, err, tt.want)
			if tt.want == connect.CodeInternal && strings.Contains(err.Error(), errInjected.Error()) {
				t.Fatalf("an unclassified failure leaked its detail to the client: %v", err)
			}
		})
	}
}

// A request that names no agent is refused by the edge, without the store being
// asked to interpret an empty name.
func TestAgentRequestsWithoutANameAreRefused(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	client := h.agentClient(tokenAcme)

	tests := map[string]func() error{
		"get": func() error {
			_, err := client.GetAgent(ctx, connect.NewRequest(&opentagv1.GetAgentRequest{}))
			return err
		},
		"history": func() error {
			_, err := client.GetAgentHistory(ctx, connect.NewRequest(&opentagv1.GetAgentHistoryRequest{}))
			return err
		},
		"delete": func() error {
			_, err := client.DeleteAgent(ctx, connect.NewRequest(&opentagv1.DeleteAgentRequest{}))
			return err
		},
		"create without a spec": func() error {
			_, err := client.CreateAgent(ctx, connect.NewRequest(&opentagv1.CreateAgentRequest{}))
			return err
		},
	}
	for name, call := range tests {
		t.Run(name, func(t *testing.T) {
			requireCode(t, call(), connect.CodeInvalidArgument)
		})
	}
}

// Listing pages in name order and hands back the token that continues it.
func TestListAgentsPagesInNameOrder(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	client := h.agentClient(tokenAcme)
	for _, name := range []string{"c-bot", "a-bot", "b-bot"} {
		if _, err := client.CreateAgent(ctx, connect.NewRequest(&opentagv1.CreateAgentRequest{Spec: specMsg(name)})); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	first, err := client.ListAgents(ctx, connect.NewRequest(&opentagv1.ListAgentsRequest{PageSize: 2}))
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if got := names(first.Msg.GetAgents()); strings.Join(got, ",") != "a-bot,b-bot" {
		t.Fatalf("first page: got %v", got)
	}
	if first.Msg.GetNextPageToken() == "" {
		t.Fatal("want a continuation token after a partial page")
	}
	second, err := client.ListAgents(ctx, connect.NewRequest(&opentagv1.ListAgentsRequest{
		PageSize:  2,
		PageToken: first.Msg.GetNextPageToken(),
	}))
	if err != nil {
		t.Fatalf("list page 2: %v", err)
	}
	if got := names(second.Msg.GetAgents()); strings.Join(got, ",") != "c-bot" {
		t.Fatalf("second page: got %v", got)
	}
}

func names(revs []*opentagv1.Revision) []string {
	out := make([]string, 0, len(revs))
	for _, r := range revs {
		out = append(out, r.GetSpec().GetName())
	}
	return out
}

// Deleting retires the name without erasing the history behind it.
func TestDeletingAnAgentIsScopedAndReportsAbsence(t *testing.T) {
	h := newHarness(t, nil)
	ctx := context.Background()
	if _, err := h.agentClient(tokenAcme).CreateAgent(ctx, connect.NewRequest(&opentagv1.CreateAgentRequest{Spec: specMsg("docs-bot")})); err != nil {
		t.Fatalf("create: %v", err)
	}
	// Another tenant cannot delete it, and learns only that it is absent.
	requireCode(t, errFromDelete(h, tokenOther, "docs-bot"), connect.CodeNotFound)
	if err := errFromDelete(h, tokenAcme, "docs-bot"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	requireCode(t, errFromDelete(h, tokenAcme, "docs-bot"), connect.CodeNotFound)
}

func errFromDelete(h *harness, token, name string) error {
	_, err := h.agentClient(token).DeleteAgent(context.Background(),
		connect.NewRequest(&opentagv1.DeleteAgentRequest{Name: name}))
	return err
}
