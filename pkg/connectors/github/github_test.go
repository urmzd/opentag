package github_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/github"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/signature"
	"github.com/urmzd/mandatum/pkg/topic"
)

const secret = "It's a Secret to Everybody"

// deliver builds a webhook delivery signed the way GitHub signs one. The real
// verifier from pkg/signature is used rather than a permissive double: this
// signature is the trigger's only security boundary.
func deliver(t *testing.T, event, delivery, body string) *http.Request {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(body))

	r := httptest.NewRequest(http.MethodPost, "/github/webhook", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(github.HeaderEvent, event)
	r.Header.Set(github.HeaderDelivery, delivery)
	r.Header.Set(signature.HeaderGitHubSignature, "sha256="+hex.EncodeToString(mac.Sum(nil)))
	return r
}

func start(t *testing.T, cfg github.Config) (*github.Connector, <-chan envelope.Tag) {
	t.Helper()
	if cfg.API == nil {
		cfg.API = github.NewFake()
	}
	if cfg.Verifier == nil {
		cfg.Verifier = signature.NewGitHub(secret)
	}
	c, err := github.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

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
	for range 1000 {
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

// issueComment is a realistic issue_comment delivery on an issue, with the
// mention mid-sentence and repeated.
const issueComment = `{
  "action": "created",
  "issue": {
    "number": 42,
    "title": "Retries double-post to Slack",
    "body": "Steps to reproduce...",
    "state": "open",
    "user": {"login": "urmzd", "type": "User"},
    "labels": [{"name": "bug"}, {"name": "connectors"}],
    "html_url": "https://github.com/urmzd/mandatum/issues/42",
    "created_at": "2026-07-20T10:00:00Z",
    "updated_at": "2026-07-20T11:00:00Z"
  },
  "comment": {
    "id": 2201234567,
    "body": "can @docs-bot summarize what changed here, and @docs-bot check the retry path too?",
    "user": {"login": "urmzd", "type": "User"},
    "html_url": "https://github.com/urmzd/mandatum/issues/42#issuecomment-2201234567",
    "created_at": "2026-07-20T12:30:00Z"
  },
  "repository": {
    "name": "mandatum",
    "full_name": "urmzd/mandatum",
    "private": false,
    "owner": {"login": "urmzd", "type": "User"}
  },
  "sender": {"login": "urmzd", "type": "User"},
  "installation": {"id": 55555}
}`

func TestIssueCommentBecomesATagAddressedAtTheIssue(t *testing.T) {
	c, tags := start(t, github.Config{Agents: mention.NewNames("docs-bot")})

	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "issue_comment", "d1f2e3c4-0000-1111-2222-333344445555", issueComment))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", w.Code, w.Body.String())
	}

	tag := receive(t, tags)
	if tag.ID != "d1f2e3c4-0000-1111-2222-333344445555" {
		t.Errorf("Tag.ID = %q, want the X-GitHub-Delivery guid", tag.ID)
	}
	if tag.Agent != "docs-bot" || tag.Origin != "github" {
		t.Errorf("Tag agent/origin = %q/%q", tag.Agent, tag.Origin)
	}
	want := "can summarize what changed here, and check the retry path too?"
	if tag.Text != want {
		t.Errorf("Tag.Text = %q, want %q with both mentions stripped", tag.Text, want)
	}
	if got, want := tag.Source.String(), "github://urmzd/mandatum/issues/42"; got != want {
		t.Errorf("Tag.Source = %q, want %q", got, want)
	}
	if tag.Actor.ID != "urmzd" || tag.Actor.Bot {
		t.Errorf("Tag.Actor = %+v", tag.Actor)
	}
	if !tag.At.Equal(time.Date(2026, 7, 20, 12, 30, 0, 0, time.UTC)) {
		t.Errorf("Tag.At = %s, want the comment's own timestamp", tag.At)
	}
	for key, want := range map[string]string{
		"github_event":           "issue_comment",
		"github_action":          "created",
		"github_owner":           "urmzd",
		"github_repo":            "mandatum",
		"github_number":          "42",
		"github_kind":            "issue",
		"github_title":           "Retries double-post to Slack",
		"github_state":           "open",
		"github_author":          "urmzd",
		"github_labels":          "bug,connectors",
		"github_comment_id":      "2201234567",
		"github_installation_id": "55555",
	} {
		if got := tag.Meta[key]; got != want {
			t.Errorf("Meta[%q] = %q, want %q", key, got, want)
		}
	}
	if err := tag.Validate(); err != nil {
		t.Errorf("invalid tag: %v", err)
	}
}

func TestACommentOnAPullRequestAddressesPullNotIssues(t *testing.T) {
	// GitHub delivers a comment on a PR as an issue_comment whose issue carries
	// a pull_request member. The address must say what the agent is looking at.
	body := strings.Replace(issueComment,
		`"state": "open",`,
		`"state": "open", "pull_request": {"url": "https://api.github.com/repos/urmzd/mandatum/pulls/42"},`, 1)

	c, tags := start(t, github.Config{Agents: mention.NewNames("docs-bot")})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "issue_comment", "d2", body))
	tag := receive(t, tags)

	if got, want := tag.Source.String(), "github://urmzd/mandatum/pull/42"; got != want {
		t.Errorf("Tag.Source = %q, want %q", got, want)
	}
	if tag.Meta["github_kind"] != "pull" {
		t.Errorf("Meta[github_kind] = %q, want pull", tag.Meta["github_kind"])
	}
}

// reviewRequested is a realistic pull_request review_requested delivery: the
// agent is named by GitHub, and there is no message at all.
const reviewRequested = `{
  "action": "review_requested",
  "number": 77,
  "pull_request": {
    "number": 77,
    "title": "Coalesce sink edits",
    "body": "Buffers deltas and flushes on an interval.\n\ncc @urmzd",
    "state": "open",
    "draft": false,
    "user": {"login": "urmzd", "type": "User"},
    "labels": [{"name": "connectors"}],
    "html_url": "https://github.com/urmzd/mandatum/pull/77",
    "base": {"ref": "main"},
    "head": {"ref": "coalesce-edits", "sha": "deadbeef"},
    "created_at": "2026-07-21T09:00:00Z",
    "updated_at": "2026-07-21T09:30:00Z"
  },
  "requested_reviewer": {"login": "docs-bot[bot]", "type": "Bot"},
  "repository": {
    "name": "mandatum",
    "full_name": "urmzd/mandatum",
    "owner": {"login": "urmzd", "type": "User"}
  },
  "sender": {"login": "urmzd", "type": "User"}
}`

func TestAReviewRequestNamesTheAgentWithoutAnyMention(t *testing.T) {
	c, tags := start(t, github.Config{
		// The app's login carries the [bot] suffix in the payload; mapping the
		// bare login covers both spellings.
		Bots: map[string]string{"docs-bot": "docs-bot"},
	})

	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "pull_request", "d3", reviewRequested))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	tag := receive(t, tags)

	if tag.Agent != "docs-bot" {
		t.Errorf("Tag.Agent = %q, want the requested reviewer's agent", tag.Agent)
	}
	if got, want := tag.Source.String(), "github://urmzd/mandatum/pull/77"; got != want {
		t.Errorf("Tag.Source = %q, want %q", got, want)
	}
	// With no message, the pull request is the request.
	if !strings.HasPrefix(tag.Text, "Coalesce sink edits") || !strings.Contains(tag.Text, "flushes on an interval") {
		t.Errorf("Tag.Text = %q, want the pull request title and body", tag.Text)
	}
	// A mention of a human in the description is left alone.
	if !strings.Contains(tag.Text, "@urmzd") {
		t.Errorf("Tag.Text = %q, want the human mention preserved", tag.Text)
	}
	for key, want := range map[string]string{
		"github_kind":     "pull",
		"github_number":   "77",
		"github_base_ref": "main",
		"github_head_ref": "coalesce-edits",
		"github_action":   "review_requested",
	} {
		if got := tag.Meta[key]; got != want {
			t.Errorf("Meta[%q] = %q, want %q", key, got, want)
		}
	}
}

func TestAReviewRequestedFromAHumanRaisesNoTag(t *testing.T) {
	body := strings.Replace(reviewRequested,
		`"requested_reviewer": {"login": "docs-bot[bot]", "type": "Bot"}`,
		`"requested_reviewer": {"login": "urmzd", "type": "User"}`, 1)

	c, tags := start(t, github.Config{Bots: map[string]string{"docs-bot": "docs-bot"}})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "pull_request", "d4", body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	select {
	case tag := <-tags:
		t.Errorf("a review request from a human raised a tag for %q", tag.Agent)
	default:
	}
}

func TestDeliveriesThatMustNotRaiseTags(t *testing.T) {
	tests := []struct {
		name  string
		event string
		body  string
	}{
		{
			name:  "our own comment coming back, which is how a mesh loops forever",
			event: "issue_comment",
			body:  strings.Replace(issueComment, `"sender": {"login": "urmzd", "type": "User"}`, `"sender": {"login": "docs-bot[bot]", "type": "Bot"}`, 1),
		},
		{
			name:  "an edited comment, because editing is not asking again",
			event: "issue_comment",
			body:  strings.Replace(issueComment, `"action": "created"`, `"action": "edited"`, 1),
		},
		{
			name:  "a comment mentioning nobody we run",
			event: "issue_comment",
			body:  strings.ReplaceAll(issueComment, "@docs-bot", "@urmzd"),
		},
		{
			name:  "a push, which names no agent and would run one per commit",
			event: "push",
			body:  `{"repository":{"name":"mandatum","owner":{"login":"urmzd"}},"sender":{"login":"urmzd","type":"User"}}`,
		},
		{
			name:  "a pull request being closed",
			event: "pull_request",
			body:  strings.Replace(reviewRequested, `"action": "review_requested"`, `"action": "closed"`, 1),
		},
		{
			name:  "an owner that could not be an address workspace",
			event: "issue_comment",
			body:  strings.ReplaceAll(issueComment, `"login": "urmzd", "type": "User"`, `"login": "urm/zd", "type": "User"`),
		},
	}

	c, tags := start(t, github.Config{
		Agents: mention.NewNames("docs-bot"),
		Bots:   map[string]string{"docs-bot": "docs-bot"},
	})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c.ServeHTTP(w, deliver(t, tc.event, "d-"+tc.name, tc.body))
			if w.Code != http.StatusOK {
				t.Errorf("status = %d, want 200 so GitHub stops retrying", w.Code)
			}
			select {
			case tag := <-tags:
				t.Errorf("raised a tag: %+v", tag)
			default:
			}
		})
	}
}

func TestAnIssueOpenedWithAMentionRaisesATag(t *testing.T) {
	body := `{
	  "action": "opened",
	  "issue": {
	    "number": 9, "title": "@docs-bot please triage this", "body": "The sink posts twice on retry.",
	    "state": "open", "user": {"login": "urmzd", "type": "User"},
	    "created_at": "2026-07-22T08:00:00Z", "updated_at": "2026-07-22T08:00:00Z"
	  },
	  "repository": {"name": "mandatum", "full_name": "urmzd/mandatum", "owner": {"login": "urmzd", "type": "User"}},
	  "sender": {"login": "urmzd", "type": "User"}
	}`

	c, tags := start(t, github.Config{Agents: mention.NewNames("docs-bot")})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "issues", "d5", body))
	tag := receive(t, tags)

	if tag.Agent != "docs-bot" {
		t.Errorf("Tag.Agent = %q", tag.Agent)
	}
	if tag.Text != "please triage this\n\nThe sink posts twice on retry." {
		t.Errorf("Tag.Text = %q", tag.Text)
	}
	if got, want := tag.Source.String(), "github://urmzd/mandatum/issues/9"; got != want {
		t.Errorf("Tag.Source = %q, want %q", got, want)
	}
}

func TestAnUnsignedDeliveryIsRefusedBeforeItIsParsed(t *testing.T) {
	c, tags := start(t, github.Config{Agents: mention.NewNames("docs-bot")})

	r := deliver(t, "issue_comment", "d6", issueComment)
	r.Header.Del(signature.HeaderGitHubSignature)
	w := httptest.NewRecorder()
	c.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}

	// A signature over different bytes must not pass either.
	r = deliver(t, "issue_comment", "d7", issueComment)
	r.Header.Set(signature.HeaderGitHubSignature, "sha256="+strings.Repeat("ab", 32))
	w = httptest.NewRecorder()
	c.ServeHTTP(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("tampered status = %d, want 401", w.Code)
	}

	select {
	case tag := <-tags:
		t.Errorf("an unauthenticated delivery raised a tag: %+v", tag)
	default:
	}
}

func TestThePingGitHubSendsWhenAHookIsCreatedIsAnswered(t *testing.T) {
	c, _ := start(t, github.Config{Agents: mention.NewNames("docs-bot")})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, deliver(t, "ping", "d8", `{"zen":"Non-blocking is better than blocking.","hook_id":1}`))
	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
}

// ── Sink ────────────────────────────────────────────────────────────

var target = address.MustParse("github://urmzd/mandatum/issues/42")

func event(seq uint64, kind envelope.Kind, payload string) envelope.Event {
	return envelope.Event{
		Seq:     seq,
		Topic:   topic.MustParse("agent:docs-bot:run_1"),
		RunID:   "run_1",
		Agent:   "docs-bot",
		Origin:  "github",
		Kind:    kind,
		Payload: []byte(payload),
		At:      time.Unix(1700000100, 0).Add(time.Duration(seq) * time.Second),
	}
}

func textEvent(seq uint64, s string) envelope.Event {
	b, err := json.Marshal(s)
	if err != nil {
		t := &testing.T{}
		t.Fatal(err)
	}
	return event(seq, envelope.KindText, `{"text":`+string(b)+`}`)
}

func sinkConnector(t *testing.T, api github.API) *github.Connector {
	t.Helper()
	c, err := github.New(github.Config{API: api, Owners: []string{"urmzd"}, Interval: -1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestDeliveringTheSameEventsTwiceKeepsOneEditedComment(t *testing.T) {
	fake := github.NewFake()
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
		t.Fatalf("github holds %d comments, want exactly 1", len(comments))
	}
	comment := comments[0]
	if comment.Ref != (github.Ref{Owner: "urmzd", Repo: "mandatum", Number: 42}) {
		t.Errorf("comment ref = %+v", comment.Ref)
	}
	if got := strings.Count(comment.Body, "The retry path posts twice because the handle was lost."); got != 1 {
		t.Errorf("body contains the answer %d times, want 1:\n%s", got, comment.Body)
	}
	if !strings.Contains(comment.Body, "**docs-bot** _(done)_") {
		t.Errorf("body is not GitHub markdown:\n%s", comment.Body)
	}
	if !strings.Contains(comment.Body, "[1] [sink.go](https://github.com/urmzd/mandatum/blob/main/sink.go)") {
		t.Errorf("citation is not a markdown footnote:\n%s", comment.Body)
	}
	if comment.Edits == 0 {
		t.Error("the comment was never edited, so nothing streamed")
	}
}

func TestOutOfOrderDeliveryDoesNotRenderStaleTextOverFresh(t *testing.T) {
	fake := github.NewFake()
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

func TestTheAnswersOwnMarkdownIsPassedThroughUnescaped(t *testing.T) {
	fake := github.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	answer := "Here is the fix:\n\n```go\nif err != nil {\n\treturn err\n}\n```\n\nSee **sink.go**."
	if err := c.Deliver(ctx, target, textEvent(1, answer)); err != nil {
		t.Fatal(err)
	}
	if body := fake.Comments()[0].Body; !strings.Contains(body, answer) {
		t.Errorf("the answer was rewritten:\n%s", body)
	}
}

func TestDeliveryToAnUnservedTargetIsRefused(t *testing.T) {
	c := sinkConnector(t, github.NewFake())
	ctx := context.Background()

	tests := []struct{ name, target string }{
		{name: "another owner", target: "github://someone-else/mandatum/issues/1"},
		{name: "another connector", target: "slack://T01/C02"},
		{name: "no number", target: "github://urmzd/mandatum/issues"},
		{name: "not a number", target: "github://urmzd/mandatum/issues/latest"},
		{name: "an unknown resource kind", target: "github://urmzd/mandatum/discussions/4"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := c.Deliver(ctx, address.MustParse(tc.target), textEvent(1, "hi"))
			if err == nil || !strings.Contains(err.Error(), "undeliverable") {
				t.Errorf("Deliver error = %v, want ErrUndeliverable", err)
			}
		})
	}
}

// ── Actor ───────────────────────────────────────────────────────────

func TestActionsChangeGitHubAndReportWhatTheyChanged(t *testing.T) {
	fake := github.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	actions := map[string]connector.Action{}
	for _, a := range c.Actions() {
		actions[a.Name] = a
	}
	if len(actions) != 3 {
		t.Fatalf("actions = %v, want three", actions)
	}

	res, err := actions["github_comment"].Invoke(ctx, target, json.RawMessage(`{"body":"looked into it"}`))
	if err != nil {
		t.Fatalf("github_comment: %v", err)
	}
	if got := res.Address.String(); !strings.HasPrefix(got, "github://urmzd/mandatum/issues/42?comment=") {
		t.Errorf("Result.Address = %q, want the comment it created", got)
	}
	if len(fake.Comments()) != 1 || fake.Comments()[0].Body != "looked into it" {
		t.Errorf("comments = %+v", fake.Comments())
	}

	pull := address.MustParse("github://urmzd/mandatum/pull/77")
	if _, err := actions["github_request_review"].Invoke(ctx, pull, json.RawMessage(`{"reviewers":["urmzd"],"teams":["platform"]}`)); err != nil {
		t.Fatalf("github_request_review: %v", err)
	}
	if got := fake.Reviewers(github.Ref{Owner: "urmzd", Repo: "mandatum", Number: 77}); len(got) != 2 {
		t.Errorf("reviewers = %v, want the user and the team", got)
	}

	if _, err := actions["github_add_labels"].Invoke(ctx, target, json.RawMessage(`{"labels":["bug","connectors"]}`)); err != nil {
		t.Fatalf("github_add_labels: %v", err)
	}
	// Adding the same label again is safe, which is what makes the action
	// retryable.
	if _, err := actions["github_add_labels"].Invoke(ctx, target, json.RawMessage(`{"labels":["bug"]}`)); err != nil {
		t.Fatalf("github_add_labels again: %v", err)
	}
	if got := fake.Labels(github.Ref{Owner: "urmzd", Repo: "mandatum", Number: 42}); len(got) != 2 {
		t.Errorf("labels = %v, want two", got)
	}
}

func TestEveryActionIsSelfDescribing(t *testing.T) {
	c := sinkConnector(t, github.NewFake())
	for _, a := range c.Actions() {
		t.Run(a.Name, func(t *testing.T) {
			if !strings.HasPrefix(a.Name, "github_") {
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

func TestAnActionCannotReachIntoAnotherRepository(t *testing.T) {
	c := sinkConnector(t, github.NewFake())
	other := address.MustParse("github://someone-else/private/issues/1")
	args := json.RawMessage(`{"body":"x","labels":["x"],"reviewers":["x"]}`)
	for _, a := range c.Actions() {
		if _, err := a.Invoke(context.Background(), other, args); err == nil || !strings.Contains(err.Error(), "undeliverable") {
			t.Errorf("%s against another owner: error = %v, want ErrUndeliverable", a.Name, err)
		}
	}
}

func TestGitHubImplementsAllThreeFaces(t *testing.T) {
	roles := connector.RolesOf(sinkConnector(t, github.NewFake()))
	if !roles.Trigger || !roles.Sink || !roles.Actor {
		t.Errorf("RolesOf(github) = %+v, want all three faces", roles)
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
