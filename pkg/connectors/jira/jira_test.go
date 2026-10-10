package jira_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/internal/inbound"
	"github.com/urmzd/mandatum/pkg/connectors/jira"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

const (
	secret     = "the header Jira was configured to send"
	botAccount = "557058:8b4a1e0f-docs-bot"
	human      = "5b10a2844c20165700ede21g"
)

var sites = map[string]jira.Site{
	"acme": {BaseURL: "https://acme.atlassian.net", Email: "ops@acme.example", Token: "token"},
}

// deliver builds a webhook delivery the way Jira sends one: no signature,
// because Jira has none, and a shared secret in a header somebody added by hand.
func deliver(t *testing.T, identifier, body string) *http.Request {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/jira/webhook", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(jira.HeaderSecret, secret)
	if identifier != "" {
		r.Header.Set(jira.HeaderIdentifier, identifier)
	}
	return r
}

func start(t *testing.T, cfg jira.Config) (*jira.Connector, <-chan envelope.Tag) {
	t.Helper()
	c := build(t, cfg)
	tags := make(chan envelope.Tag, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = c.Ingest(ctx, tags)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	for range 2000 {
		if c.Ingesting() {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !c.Ingesting() {
		t.Fatal("Ingest never claimed the channel")
	}
	return c, tags
}

func build(t *testing.T, cfg jira.Config) *jira.Connector {
	t.Helper()
	if cfg.API == nil {
		cfg.API = jira.NewFake()
	}
	if cfg.Sites == nil {
		cfg.Sites = sites
	}
	if cfg.Verifier == nil {
		cfg.Verifier = inbound.Secret{Header: jira.HeaderSecret, Value: secret}
	}
	c, err := jira.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

// commentCreated is a realistic Jira Cloud comment_created delivery, with the
// mention in Jira's own account-id markup and repeated mid-sentence.
const commentCreated = `{
  "timestamp": 1784601000000,
  "webhookEvent": "comment_created",
  "comment": {
    "self": "https://acme.atlassian.net/rest/api/2/issue/10002/comment/10101",
    "id": "10101",
    "author": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd", "accountType": "atlassian"},
    "body": "[~accountid:557058:8b4a1e0f-docs-bot] summarize what changed here, and ask [~accountid:557058:8b4a1e0f-docs-bot] to check the retry path too",
    "created": "2026-07-20T12:30:00.000+0000",
    "updated": "2026-07-20T12:30:00.000+0000"
  },
  "issue": {
    "id": "10002",
    "self": "https://acme.atlassian.net/rest/api/2/issue/10002",
    "key": "PROJ-5",
    "fields": {
      "summary": "Retries double-post to Slack",
      "description": "Steps to reproduce...",
      "status": {"name": "In Progress"},
      "issuetype": {"name": "Bug"},
      "priority": {"name": "High"},
      "project": {"key": "PROJ", "name": "Platform"},
      "assignee": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd"},
      "reporter": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd"},
      "labels": ["connectors", "bug"]
    }
  }
}`

func botConfig() jira.Config {
	return jira.Config{
		Bots:   map[string]string{botAccount: "docs-bot"},
		Agents: mention.NewNames("docs-bot"),
	}
}

func TestACommentMentioningAnAgentBecomesATagAddressedAtTheIssue(t *testing.T) {
	c, tags := start(t, botConfig())

	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "b8a5f0c2-1111-2222-3333-444455556666", commentCreated))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", w.Code, w.Body.String())
	}

	tag := receive(t, tags)
	if err := tag.Validate(); err != nil {
		t.Fatalf("invalid tag: %v", err)
	}
	if tag.ID != "b8a5f0c2-1111-2222-3333-444455556666" {
		t.Errorf("Tag.ID = %q, want Atlassian's own delivery identifier", tag.ID)
	}
	if tag.Agent != "docs-bot" || tag.Origin != "jira" {
		t.Errorf("agent/origin = %q/%q", tag.Agent, tag.Origin)
	}
	want := "summarize what changed here, and ask to check the retry path too"
	if tag.Text != want {
		t.Errorf("Tag.Text = %q, want %q with both account-id mentions resolved and stripped", tag.Text, want)
	}
	if got, want := tag.Source.String(), "jira://acme/PROJ-5"; got != want {
		t.Errorf("Tag.Source = %q, want %q — the SHORT SITE KEY, never the hostname", got, want)
	}
	if tag.Actor.ID != human || tag.Actor.Display != "Urmzd" || tag.Actor.Bot {
		t.Errorf("Tag.Actor = %+v", tag.Actor)
	}
	if !tag.At.Equal(time.Date(2026, 7, 20, 12, 30, 0, 0, time.UTC)) {
		t.Errorf("Tag.At = %s, want the comment's own timestamp", tag.At)
	}
	for key, want := range map[string]string{
		"jira_event":        "comment_created",
		"jira_site":         "acme",
		"jira_key":          "PROJ-5",
		"jira_summary":      "Retries double-post to Slack",
		"jira_status":       "In Progress",
		"jira_type":         "Bug",
		"jira_priority":     "High",
		"jira_project":      "PROJ",
		"jira_project_name": "Platform",
		"jira_labels":       "connectors,bug",
		"jira_comment_id":   "10101",
		"jira_url":          "https://acme.atlassian.net/browse/PROJ-5",
	} {
		if got := tag.Meta[key]; got != want {
			t.Errorf("Meta[%q] = %q, want %q", key, got, want)
		}
	}
}

// adfComment is the same request in the other encoding Jira uses: an Atlassian
// Document Format tree, delivered as jira:issue_updated rather than
// comment_created because that is how the newer webhook registration reports it.
const adfComment = `{
  "timestamp": 1784601000000,
  "webhookEvent": "jira:issue_updated",
  "issue_event_type_name": "issue_commented",
  "user": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd", "accountType": "atlassian"},
  "comment": {
    "id": "10102",
    "author": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd", "accountType": "atlassian"},
    "created": "2026-07-20T13:00:00.000+0000",
    "body": {
      "type": "doc",
      "version": 1,
      "content": [
        {"type": "paragraph", "content": [
          {"type": "mention", "attrs": {"id": "557058:8b4a1e0f-docs-bot", "text": "@Docs Bot"}},
          {"type": "text", "text": " please triage this before standup"}
        ]},
        {"type": "paragraph", "content": [
          {"type": "text", "text": "See "},
          {"type": "inlineCard", "attrs": {"url": "https://acme.atlassian.net/browse/PROJ-4"}}
        ]}
      ]
    }
  },
  "issue": {
    "id": "10002",
    "self": "https://acme.atlassian.net/rest/api/2/issue/10002",
    "key": "PROJ-5",
    "fields": {"summary": "Retries double-post to Slack", "status": {"name": "To Do"}}
  }
}`

func TestAnAtlassianDocumentFormatBodyReadsTheSameAsWikiMarkup(t *testing.T) {
	c, tags := start(t, botConfig())
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "adf-1", adfComment))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	tag := receive(t, tags)

	if tag.Agent != "docs-bot" {
		t.Errorf("Tag.Agent = %q, want the mention node resolved to an agent", tag.Agent)
	}
	if !strings.HasPrefix(tag.Text, "please triage this before standup") {
		t.Errorf("Tag.Text = %q, want the mention node stripped from the front", tag.Text)
	}
	// Paragraph structure survives as a blank line, so a summary cannot run into
	// a description.
	if !strings.Contains(tag.Text, "\n\nSee https://acme.atlassian.net/browse/PROJ-4") {
		t.Errorf("Tag.Text = %q, want the second paragraph and its card url", tag.Text)
	}
	if tag.Meta["jira_comment_id"] != "10102" {
		t.Errorf("Meta[jira_comment_id] = %q", tag.Meta["jira_comment_id"])
	}
}

// assigned is the case with no message: a human handed the ticket to the agent.
const assigned = `{
  "timestamp": 1784602000000,
  "webhookEvent": "jira:issue_updated",
  "issue_event_type_name": "issue_assigned",
  "user": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd", "accountType": "atlassian"},
  "issue": {
    "id": "10009",
    "self": "https://acme.atlassian.net/rest/api/2/issue/10009",
    "key": "PROJ-9",
    "fields": {
      "summary": "Coalesce sink edits",
      "description": "Buffers deltas and flushes on an interval.\n\ncc [~accountid:5b10a2844c20165700ede21g]",
      "status": {"name": "To Do"},
      "issuetype": {"name": "Task"},
      "project": {"key": "PROJ", "name": "Platform"}
    }
  },
  "changelog": {
    "id": "10200",
    "items": [
      {"field": "assignee", "fieldtype": "jira", "from": null, "fromString": null,
       "to": "557058:8b4a1e0f-docs-bot", "toString": "Docs Bot"}
    ]
  }
}`

func TestAssigningAnIssueToAnAgentRaisesATagWithNoMessage(t *testing.T) {
	c, tags := start(t, botConfig())
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "assign-1", assigned))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	tag := receive(t, tags)

	if tag.Agent != "docs-bot" {
		t.Errorf("Tag.Agent = %q, want the assignee's agent", tag.Agent)
	}
	if got, want := tag.Source.String(), "jira://acme/PROJ-9"; got != want {
		t.Errorf("Tag.Source = %q, want %q", got, want)
	}
	// Nobody typed a message, so the ticket is the request.
	if !strings.HasPrefix(tag.Text, "Coalesce sink edits") || !strings.Contains(tag.Text, "flushes on an interval") {
		t.Errorf("Tag.Text = %q, want the summary and description", tag.Text)
	}
	// A mention of a human in the description is normalized but not removed: the
	// agent may be being asked to involve them.
	if !strings.Contains(tag.Text, "[~accountid:5b10a2844c20165700ede21g]") {
		t.Errorf("Tag.Text = %q, want the human mention preserved", tag.Text)
	}
	if tag.Meta["jira_assigned"] != "true" {
		t.Error("Meta[jira_assigned] is unset, so the agent cannot tell nobody asked it a question")
	}
}

const issueCreated = `{
  "timestamp": 1784603000000,
  "webhookEvent": "jira:issue_created",
  "issue_event_type_name": "issue_created",
  "user": {"accountId": "5b10a2844c20165700ede21g", "displayName": "Urmzd", "accountType": "atlassian"},
  "issue": {
    "id": "10011",
    "self": "https://acme.atlassian.net/rest/api/2/issue/10011",
    "key": "PROJ-11",
    "fields": {
      "summary": "@docs-bot please triage this",
      "description": "The sink posts twice on retry.",
      "status": {"name": "To Do"}
    }
  }
}`

func TestAnIssueCreatedWithATextualMentionRaisesATag(t *testing.T) {
	// An agent is not a Jira user, so "@docs-bot" stays literal text rather than
	// becoming an account-id mention. Config.Agents is what makes that
	// resolvable without every "@" starting a run.
	c, tags := start(t, botConfig())
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "created-1", issueCreated))
	tag := receive(t, tags)

	if tag.Agent != "docs-bot" {
		t.Errorf("Tag.Agent = %q", tag.Agent)
	}
	if tag.Text != "please triage this\n\nThe sink posts twice on retry." {
		t.Errorf("Tag.Text = %q", tag.Text)
	}
	if got, want := tag.Source.String(), "jira://acme/PROJ-11"; got != want {
		t.Errorf("Tag.Source = %q, want %q", got, want)
	}
}

func TestDeliveriesThatMustNotRaiseTags(t *testing.T) {
	tests := []struct{ name, body string }{
		{
			name: "our own comment coming back, which is how a mesh loops forever",
			body: strings.Replace(commentCreated, `"accountType": "atlassian"`, `"accountType": "app"`, 1),
		},
		{
			name: "a comment mentioning nobody we run",
			body: strings.ReplaceAll(commentCreated, botAccount, human),
		},
		{
			name: "an issue updated that is not an assignment, which would run an agent per field edit",
			body: strings.Replace(assigned,
				`{"field": "assignee", "fieldtype": "jira", "from": null, "fromString": null,
       "to": "557058:8b4a1e0f-docs-bot", "toString": "Docs Bot"}`,
				`{"field": "priority", "from": "3", "fromString": "Medium", "to": "2", "toString": "High"}`, 1),
		},
		{
			name: "an issue assigned to a human",
			body: strings.Replace(assigned, `"to": "557058:8b4a1e0f-docs-bot"`, `"to": "`+human+`"`, 1),
		},
		{
			name: "an issue unassigned",
			body: strings.Replace(assigned, `"to": "557058:8b4a1e0f-docs-bot"`, `"to": null`, 1),
		},
		{
			name: "a delivery from a Jira instance this connector does not serve",
			body: strings.ReplaceAll(commentCreated, "acme.atlassian.net", "someone-else.atlassian.net"),
		},
		{
			name: "a payload whose key is not an issue key",
			body: strings.Replace(commentCreated, `"key": "PROJ-5"`, `"key": "not an issue"`, 1),
		},
		{
			name: "an event kind that names no agent",
			body: strings.Replace(commentCreated, `"webhookEvent": "comment_created"`, `"webhookEvent": "jira:worklog_updated"`, 1),
		},
	}

	// Two sites, so the "instance we do not serve" case is about configuration
	// rather than about there being only one answer available.
	cfg := botConfig()
	cfg.Sites = map[string]jira.Site{
		"acme":  sites["acme"],
		"other": {BaseURL: "https://other.atlassian.net", Token: "t"},
	}
	c, tags := start(t, cfg)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c.ServeHTTP(w, deliver(t, "d-"+tc.name, tc.body))
			if w.Code != http.StatusOK {
				t.Errorf("status = %d, want 200 so Jira stops retrying", w.Code)
			}
			select {
			case tag := <-tags:
				t.Errorf("raised a tag: %+v", tag)
			default:
			}
		})
	}
}

func TestAnUnauthenticatedDeliveryIsRefusedBeforeItIsParsed(t *testing.T) {
	c, tags := start(t, botConfig())

	tests := []struct {
		name  string
		mutit func(*http.Request)
	}{
		{"no secret at all", func(r *http.Request) { r.Header.Del(jira.HeaderSecret) }},
		{"the wrong secret", func(r *http.Request) { r.Header.Set(jira.HeaderSecret, "guessed") }},
		{"a secret that is a prefix of ours", func(r *http.Request) { r.Header.Set(jira.HeaderSecret, secret[:10]) }},
		{"the header twice, which two hops could disagree about", func(r *http.Request) {
			r.Header.Add(jira.HeaderSecret, secret)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := deliver(t, "unauth", commentCreated)
			tc.mutit(r)
			w := httptest.NewRecorder()
			c.ServeHTTP(w, r)
			if w.Code != http.StatusUnauthorized {
				t.Errorf("status = %d, want 401", w.Code)
			}
			select {
			case tag := <-tags:
				t.Errorf("an unauthenticated delivery raised a tag: %+v", tag)
			default:
			}
		})
	}
}

func TestWithoutAnAtlassianIdentifierTheIdComesFromTheChangeItself(t *testing.T) {
	// Not from a clock and not from a counter: a key containing "now" would make
	// every redelivery a new run, which is the failure Tag.ID exists to prevent.
	c, tags := start(t, botConfig())

	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "", commentCreated))
	first := receive(t, tags)
	if first.ID != "comment_created:PROJ-5:comment:10101" {
		t.Fatalf("Tag.ID = %q, want a key derived from the comment's own identity", first.ID)
	}

	w = httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "", commentCreated))
	second := receive(t, tags)
	if second.ID != first.ID {
		t.Errorf("a redelivery produced %q, want the same id as %q", second.ID, first.ID)
	}
}

// ── Sink ────────────────────────────────────────────────────────────

var target = address.MustParse("jira://acme/PROJ-5")

func event(seq uint64, kind envelope.Kind, payload string) envelope.Event {
	return envelope.Event{
		Seq:     seq,
		Topic:   topic.MustParse("agent:docs-bot:run_1"),
		RunID:   "run_1",
		Agent:   "docs-bot",
		Origin:  "jira",
		Kind:    kind,
		Payload: []byte(payload),
		At:      time.Unix(1784600000, 0).Add(time.Duration(seq) * time.Second),
	}
}

func textEvent(seq uint64, s string) envelope.Event {
	encoded, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return event(seq, envelope.KindText, `{"text":`+string(encoded)+`}`)
}

func sinkConnector(t *testing.T, api jira.API) *jira.Connector {
	t.Helper()
	return build(t, jira.Config{API: api, Interval: -1})
}

func TestDeliveringTheSameEventsTwiceKeepsOneEditedComment(t *testing.T) {
	fake := jira.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	events := []envelope.Event{
		event(1, envelope.KindAccepted, ""),
		textEvent(2, "The retry path posts twice "),
		textEvent(3, "because the handle was lost."),
		event(4, envelope.KindCitation, `{"title":"sink.go","uri":"https://github.com/urmzd/mandatum/blob/main/sink.go"}`),
		event(5, envelope.KindCompleted, ""),
	}
	for range 2 {
		for _, e := range events {
			if err := c.Deliver(ctx, target, e); err != nil {
				t.Fatalf("Deliver(seq %d): %v", e.Seq, err)
			}
		}
	}

	comments := fake.Comments()
	if len(comments) != 1 {
		t.Fatalf("jira holds %d comments, want exactly 1 — the run owns one comment and edits it", len(comments))
	}
	got := comments[0]
	if got.Ref != (jira.Ref{Site: "acme", Issue: "PROJ-5"}) {
		t.Errorf("comment ref = %+v", got.Ref)
	}
	if n := strings.Count(got.Body, "The retry path posts twice because the handle was lost."); n != 1 {
		t.Errorf("body contains the answer %d times, want 1:\n%s", n, got.Body)
	}
	if got.Edits == 0 {
		t.Error("the comment was never edited, so nothing streamed")
	}
}

func TestTheAnswerIsRenderedInJiraWikiMarkup(t *testing.T) {
	fake := jira.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	for _, e := range []envelope.Event{
		textEvent(1, "Fixed in {code}sink.go{code}, see [the doc]."),
		event(2, envelope.KindCitation, `{"title":"sink.go","uri":"https://example.com/sink.go"}`),
		event(3, envelope.KindActionTaken, `{"summary":"moved acme/PROJ-5 to Done","address":"jira://acme/PROJ-5"}`),
		event(4, envelope.KindCompleted, ""),
	} {
		if err := c.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}

	body := fake.Comments()[0].Body
	if !strings.Contains(body, "*docs-bot* _(done)_") {
		t.Errorf("header is not wiki markup:\n%s", body)
	}
	// A citation is a wiki link, which is the whole point of carrying citations
	// as their own kind: the reader needs [1] to be followable here.
	if !strings.Contains(body, "[1] [sink.go|https://example.com/sink.go]") {
		t.Errorf("citation is not a wiki link:\n%s", body)
	}
	if !strings.Contains(body, "moved acme/PROJ-5 to Done") {
		t.Errorf("the action is not reported:\n%s", body)
	}
	// The model's own braces and brackets are neutralized, because a brace opens
	// a macro and a bracket opens a link, and either would otherwise swallow the
	// rest of the comment.
	if strings.Contains(body, "Fixed in {code}") || strings.Contains(body, "see [the doc]") {
		t.Errorf("wiki metacharacters in the answer were not escaped:\n%s", body)
	}
	if !strings.Contains(body, `Fixed in \{code}sink.go\{code}, see \[the doc\].`) {
		t.Errorf("the escaped form is missing:\n%s", body)
	}
	// The markup this package composed is NOT escaped, or the citation link
	// would render as literal brackets.
	if strings.Contains(body, `\[1\]`) {
		t.Errorf("the layout's own markup was escaped along with the prose:\n%s", body)
	}
}

func TestOutOfOrderDeliveryDoesNotRenderStaleTextOverFresh(t *testing.T) {
	fake := jira.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	for _, e := range []envelope.Event{textEvent(3, "second"), textEvent(2, "first "), event(4, envelope.KindCompleted, "")} {
		if err := c.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}
	if body := fake.Comments()[0].Body; !strings.Contains(body, "first second") {
		t.Errorf("body = %q, want sequence order", body)
	}
}

func TestDeliveryToAnUnservedTargetIsRefused(t *testing.T) {
	c := sinkConnector(t, jira.NewFake())
	ctx := context.Background()

	tests := []struct{ name, target string }{
		{"another site", "jira://someone-else/PROJ-1"},
		{"another connector", "github://urmzd/mandatum/issues/1"},
		{"no issue key", "jira://acme"},
		{"something that is not an issue key", "jira://acme/latest"},
		{"a project with no number", "jira://acme/PROJ"},
		{"too many path segments", "jira://acme/PROJ-5/comments"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := c.Deliver(ctx, address.MustParse(tc.target), textEvent(1, "hi"))
			if !errors.Is(err, connector.ErrUndeliverable) {
				t.Errorf("Deliver error = %v, want ErrUndeliverable", err)
			}
		})
	}
}

// ── Actor ───────────────────────────────────────────────────────────

func actions(t *testing.T, c *jira.Connector) map[string]connector.Action {
	t.Helper()
	out := map[string]connector.Action{}
	for _, a := range c.Actions() {
		out[a.Name] = a
	}
	if len(out) != 3 {
		t.Fatalf("actions = %v, want three", out)
	}
	return out
}

func TestActionsChangeJiraAndReportWhatTheyChanged(t *testing.T) {
	fake := jira.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()
	act := actions(t, c)
	ref := jira.Ref{Site: "acme", Issue: "PROJ-5"}

	// The model names a status, not a transition id it could not possibly know.
	res, err := act["jira_transition"].Invoke(ctx, target, json.RawMessage(`{"to":"In Progress"}`))
	if err != nil {
		t.Fatalf("jira_transition: %v", err)
	}
	if fake.Status(ref) != "In Progress" {
		t.Errorf("status = %q, want In Progress", fake.Status(ref))
	}
	if res.Address.String() != "jira://acme/PROJ-5" {
		t.Errorf("Result.Address = %q", res.Address.String())
	}
	if !strings.Contains(res.Summary, "In Progress") {
		t.Errorf("Result.Summary = %q, want the status it landed in", res.Summary)
	}

	// A transition named by its button rather than its destination resolves too.
	if _, err := act["jira_transition"].Invoke(ctx, target, json.RawMessage(`{"to":"Done"}`)); err != nil {
		t.Fatalf("jira_transition to Done: %v", err)
	}
	if fake.Status(ref) != "Done" {
		t.Errorf("status = %q, want Done", fake.Status(ref))
	}

	res, err = act["jira_comment"].Invoke(ctx, target, json.RawMessage(`{"body":"looked into it"}`))
	if err != nil {
		t.Fatalf("jira_comment: %v", err)
	}
	if got := res.Address.String(); !strings.HasPrefix(got, "jira://acme/PROJ-5?comment=") {
		t.Errorf("Result.Address = %q, want the comment it created", got)
	}
	if len(fake.Comments()) != 1 || fake.Comments()[0].Body != "looked into it" {
		t.Errorf("comments = %+v", fake.Comments())
	}

	if _, err := act["jira_assign"].Invoke(ctx, target, json.RawMessage(`{"assignee":"`+human+`"}`)); err != nil {
		t.Fatalf("jira_assign: %v", err)
	}
	if fake.Assignee(ref) != human {
		t.Errorf("assignee = %q", fake.Assignee(ref))
	}
	// A model asked to clear an assignee writes a word, not an empty string.
	if _, err := act["jira_assign"].Invoke(ctx, target, json.RawMessage(`{"assignee":"unassigned"}`)); err != nil {
		t.Fatalf("jira_assign unassigned: %v", err)
	}
	if fake.Assignee(ref) != "" {
		t.Errorf("assignee = %q, want it cleared", fake.Assignee(ref))
	}

	// An action may name another issue on the same site.
	if _, err := act["jira_comment"].Invoke(ctx, target, json.RawMessage(`{"body":"related","key":"PROJ-9"}`)); err != nil {
		t.Fatalf("jira_comment on another issue: %v", err)
	}
	if got := fake.Comments()[1].Ref.Issue; got != "PROJ-9" {
		t.Errorf("comment landed on %q, want PROJ-9", got)
	}
}

func TestATransitionTheWorkflowDoesNotOfferIsRefusedWithTheOnesItDoes(t *testing.T) {
	// The error is the interesting part: a model that gets told what IS possible
	// can retry, and one that gets "no" cannot.
	c := sinkConnector(t, jira.NewFake())
	_, err := actions(t, c)["jira_transition"].Invoke(context.Background(), target, json.RawMessage(`{"to":"Shipped"}`))
	if err == nil {
		t.Fatal("a status outside the workflow was applied")
	}
	for _, want := range []string{"Shipped", "In Progress", "Done"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to mention %q", err, want)
		}
	}
}

func TestEveryActionIsSelfDescribing(t *testing.T) {
	c := sinkConnector(t, jira.NewFake())
	for _, a := range c.Actions() {
		t.Run(a.Name, func(t *testing.T) {
			if !strings.HasPrefix(a.Name, "jira_") {
				t.Errorf("Name = %q, want the connector prefix", a.Name)
			}
			if a.Description == "" || a.Invoke == nil {
				t.Error("an action a model cannot understand or call")
			}
			var schema map[string]any
			if err := json.Unmarshal(a.Schema, &schema); err != nil {
				t.Fatalf("schema is not valid JSON: %v", err)
			}
			if schema["type"] != "object" {
				t.Errorf("schema type = %v, want object", schema["type"])
			}
		})
	}
}

func TestAnActionCannotReachIntoAnotherSite(t *testing.T) {
	c := sinkConnector(t, jira.NewFake())
	other := address.MustParse("jira://someone-else/SEC-1")
	args := json.RawMessage(`{"body":"x","to":"Done","assignee":"x"}`)
	for _, a := range c.Actions() {
		if _, err := a.Invoke(context.Background(), other, args); !errors.Is(err, connector.ErrUndeliverable) {
			t.Errorf("%s against another site: error = %v, want ErrUndeliverable", a.Name, err)
		}
	}
}

// ── Faces and configuration ─────────────────────────────────────────

func TestJiraImplementsAllThreeFaces(t *testing.T) {
	c := sinkConnector(t, jira.NewFake())
	roles := connector.RolesOf(c)
	if !roles.Trigger || !roles.Sink || !roles.Actor {
		t.Fatalf("RolesOf(jira) = %+v, want all three faces", roles)
	}

	registry := connector.NewRegistry()
	if err := registry.Register(c); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := registry.Sink(target); err != nil {
		t.Errorf("Registry.Sink(jira://...) = %v", err)
	}
	if len(registry.Triggers()) != 1 || len(registry.Actions()) != 3 {
		t.Errorf("triggers = %d, actions = %d", len(registry.Triggers()), len(registry.Actions()))
	}
}

func TestTheIssueAddressRoundTripsThroughPkgAddress(t *testing.T) {
	uri := jira.Address(jira.Ref{Site: "acme", Issue: "PROJ-5"}).String()
	if uri != "jira://acme/PROJ-5" {
		t.Fatalf("Address = %q", uri)
	}
	parsed, err := address.Parse(uri)
	if err != nil {
		t.Fatalf("Parse(%q): %v", uri, err)
	}
	if parsed.String() != uri || parsed.Connector != jira.Name || parsed.Workspace != "acme" || parsed.Resource() != "PROJ-5" {
		t.Errorf("round trip = %+v", parsed)
	}
}

func TestASiteIsKeyedByAShortNameBecauseAHostnameCannotBeAWorkspace(t *testing.T) {
	// This is the constraint the whole site-key indirection exists for: a "."
	// is not subject-token safe, so "acme.atlassian.net" cannot be a workspace.
	if err := address.ValidWorkspace("acme.atlassian.net"); err == nil {
		t.Fatal("pkg/address accepted a hostname as a workspace, which would remove the reason for site keys")
	}

	tests := []struct {
		name string
		cfg  jira.Config
	}{
		{
			name: "a site keyed by its hostname",
			cfg: jira.Config{Sites: map[string]jira.Site{
				"acme.atlassian.net": {BaseURL: "https://acme.atlassian.net", Token: "t"},
			}},
		},
		{
			name: "no sites at all",
			cfg:  jira.Config{Sites: map[string]jira.Site{}},
		},
		{
			name: "a site with no usable base url",
			cfg:  jira.Config{Sites: map[string]jira.Site{"acme": {BaseURL: "not a url", Token: "t"}}},
		},
		{
			name: "two keys pointing at one instance, which an inbound delivery could not be attributed to",
			cfg: jira.Config{Sites: map[string]jira.Site{
				"acme":  {BaseURL: "https://acme.atlassian.net", Token: "t"},
				"acme2": {BaseURL: "https://acme.atlassian.net/", Token: "t"},
			}},
		},
		{
			name: "a bot mapped to an unusable agent name",
			cfg:  jira.Config{Sites: sites, Bots: map[string]string{"a": "docs bot"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.API = jira.NewFake()
			if _, err := jira.New(cfg); err == nil {
				t.Fatal("New accepted an unusable configuration")
			}
		})
	}
}

func receive(t *testing.T, tags <-chan envelope.Tag) envelope.Tag {
	t.Helper()
	select {
	case tag := <-tags:
		return tag
	case <-time.After(2 * time.Second):
		t.Fatal("no tag was raised")
		return envelope.Tag{}
	}
}
