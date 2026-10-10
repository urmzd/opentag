package slack_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connector"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/connectors/slack"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/signature"
	"github.com/urmzd/mandatum/pkg/topic"
)

const secret = "8f742231b10e8888abcd99yyyzzz85a5"

var signedAt = time.Unix(1700000100, 0)

// post builds a request signed the way Slack signs one. Using the real verifier
// from pkg/signature rather than a permissive double is deliberate: the trigger's
// only security boundary is this check, and a test that stubbed it would prove
// the parsing works while saying nothing about whether the boundary is wired up.
func post(t *testing.T, body string) *http.Request {
	t.Helper()
	ts := strconv.FormatInt(signedAt.Unix(), 10)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte("v0:" + ts + ":" + body))

	r := httptest.NewRequest(http.MethodPost, "/slack/events", strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set(signature.HeaderSlackTimestamp, ts)
	r.Header.Set(signature.HeaderSlackSignature, "v0="+hex.EncodeToString(mac.Sum(nil)))
	return r
}

func verifier() *signature.Slack {
	return &signature.Slack{Secret: []byte(secret), Now: func() time.Time { return signedAt }}
}

// start brings up a connector with an Ingest running, and returns the channel
// tags arrive on.
func start(t *testing.T, cfg slack.Config) (*slack.Connector, <-chan envelope.Tag) {
	t.Helper()
	if cfg.API == nil {
		cfg.API = slack.NewFake()
	}
	if cfg.Verifier == nil {
		cfg.Verifier = verifier()
	}
	c, err := slack.New(cfg)
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
	// Wait for Ingest to claim the channel before the first request.
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

// appMention is a realistic app_mention delivery: a mention mid-sentence, in a
// thread, with Slack's own entity escaping and markup.
const appMention = `{
  "token": "z26uFbvR1xHJEdHE1OQiO6t8",
  "team_id": "T01ABCDEF",
  "api_app_id": "A08APPID",
  "event": {
    "type": "app_mention",
    "user": "U0HUMAN1",
    "text": "hey <@U08BOTID> can you summarize <https://example.com/design|the design doc> for <@U0HUMAN2>? &lt;3",
    "ts": "1700000090.001200",
    "team": "T01ABCDEF",
    "thread_ts": "1700000000.000100",
    "channel": "C02CHANNEL",
    "event_ts": "1700000090.001200"
  },
  "type": "event_callback",
  "event_id": "Ev08MNEMONIC",
  "event_time": 1700000090,
  "authorizations": [{"enterprise_id": null, "team_id": "T01ABCDEF", "user_id": "U08BOTID", "is_bot": true}],
  "is_ext_shared_channel": false
}`

func TestAppMentionBecomesATagNamingTheAgentItMentioned(t *testing.T) {
	c, tags := start(t, slack.Config{
		Team: "T01ABCDEF",
		Bots: map[string]string{"U08BOTID": "docs-bot"},
	})

	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, appMention))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", w.Code, w.Body.String())
	}

	tag := receive(t, tags)
	if tag.ID != "Ev08MNEMONIC" {
		t.Errorf("Tag.ID = %q, want Slack's event_id so a retry dedupes", tag.ID)
	}
	if tag.Agent != "docs-bot" {
		t.Errorf("Tag.Agent = %q, want docs-bot", tag.Agent)
	}
	if tag.Origin != "slack" {
		t.Errorf("Tag.Origin = %q, want slack", tag.Origin)
	}
	// The mention is gone, the link is its label, the other human survives as a
	// mention, and the entity is decoded.
	want := "hey can you summarize the design doc for @U0HUMAN2? <3"
	if tag.Text != want {
		t.Errorf("Tag.Text = %q, want %q", tag.Text, want)
	}
	if got, want := tag.Source.String(), "slack://T01ABCDEF/C02CHANNEL?thread=1700000000.000100"; got != want {
		t.Errorf("Tag.Source = %q, want %q", got, want)
	}
	if tag.Actor.ID != "U0HUMAN1" {
		t.Errorf("Tag.Actor.ID = %q, want the human who typed it", tag.Actor.ID)
	}
	if !tag.At.Equal(time.Unix(1700000090, 1200000).UTC()) {
		t.Errorf("Tag.At = %s, want the event timestamp with its microseconds", tag.At)
	}
	for key, want := range map[string]string{
		"slack_team":      "T01ABCDEF",
		"slack_channel":   "C02CHANNEL",
		"slack_user":      "U0HUMAN1",
		"slack_thread_ts": "1700000000.000100",
		"slack_event":     "app_mention",
		"slack_app_id":    "A08APPID",
	} {
		if got := tag.Meta[key]; got != want {
			t.Errorf("Meta[%q] = %q, want %q", key, got, want)
		}
	}
	if err := tag.Validate(); err != nil {
		t.Errorf("the tag the core will receive is invalid: %v", err)
	}
}

func TestSourceAddressRoundTripsThroughTheAddressPackage(t *testing.T) {
	c, tags := start(t, slack.Config{Bots: map[string]string{"U08BOTID": "docs-bot"}})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, appMention))
	tag := receive(t, tags)

	parsed, err := address.Parse(tag.Source.String())
	if err != nil {
		t.Fatalf("Parse(%q): %v", tag.Source, err)
	}
	if parsed.String() != tag.Source.String() {
		t.Errorf("round trip = %q, want %q", parsed, tag.Source)
	}
	if parsed.Connector != "slack" || parsed.Workspace != "T01ABCDEF" || parsed.Resource() != "C02CHANNEL" {
		t.Errorf("parsed = %+v, want the slack team and channel", parsed)
	}
	if thread, ok := parsed.Param("thread"); !ok || thread != "1700000000.000100" {
		t.Errorf("thread param = %q (%v), want the thread timestamp", thread, ok)
	}
}

func TestAMentionWithoutAThreadAnswersUnderTheMentionItself(t *testing.T) {
	body := strings.ReplaceAll(appMention, `"thread_ts": "1700000000.000100",`, "")
	c, tags := start(t, slack.Config{Bots: map[string]string{"U08BOTID": "docs-bot"}})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	tag := receive(t, tags)
	if thread, _ := tag.Source.Param("thread"); thread != "1700000090.001200" {
		t.Errorf("thread = %q, want the mention's own ts so the answer opens a thread", thread)
	}
}

func TestTheAgentCanBeNamedInTheTextAfterTheAppMention(t *testing.T) {
	body := strings.ReplaceAll(appMention,
		`"text": "hey <@U08BOTID> can you summarize <https://example.com/design|the design doc> for <@U0HUMAN2>? &lt;3"`,
		`"text": "<@U08APPID> @triage please look at this"`)

	c, tags := start(t, slack.Config{
		// The app's own user is not mapped to an agent, so resolution falls
		// through to the text.
		Agents: mention.NewNames("triage", "docs-bot"),
	})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, body))
	tag := receive(t, tags)

	if tag.Agent != "triage" {
		t.Errorf("Tag.Agent = %q, want triage", tag.Agent)
	}
	if tag.Text != "@U08APPID please look at this" {
		t.Errorf("Tag.Text = %q", tag.Text)
	}
}

func TestEventsThatMustNotRaiseTags(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "a message from a bot, which is how a mesh would answer itself",
			body: `{"type":"event_callback","team_id":"T01ABCDEF","event_id":"Ev1","event":{
				"type":"app_mention","bot_id":"B08SELF","user":"U08BOTID","text":"<@U08BOTID> hi",
				"channel":"C02CHANNEL","ts":"1700000090.000100"}}`,
		},
		{
			name: "a plain channel message, because joining a channel is not subscribing to it",
			body: `{"type":"event_callback","team_id":"T01ABCDEF","event_id":"Ev2","event":{
				"type":"message","channel_type":"channel","user":"U0HUMAN1","text":"<@U08BOTID> hi",
				"channel":"C02CHANNEL","ts":"1700000090.000100"}}`,
		},
		{
			name: "a message subtype: a join, an edit, a file share",
			body: `{"type":"event_callback","team_id":"T01ABCDEF","event_id":"Ev3","event":{
				"type":"message","channel_type":"im","subtype":"message_changed","user":"U0HUMAN1",
				"text":"<@U08BOTID> hi","channel":"D02DM","ts":"1700000090.000100"}}`,
		},
		{
			name: "an event type this connector does not act on",
			body: `{"type":"event_callback","team_id":"T01ABCDEF","event_id":"Ev4","event":{
				"type":"reaction_added","user":"U0HUMAN1","channel":"C02CHANNEL","ts":"1700000090.000100"}}`,
		},
		{
			name: "a mention naming no agent we run",
			body: `{"type":"event_callback","team_id":"T01ABCDEF","event_id":"Ev5","event":{
				"type":"app_mention","user":"U0HUMAN1","text":"<@U0UNKNOWN> hi",
				"channel":"C02CHANNEL","ts":"1700000090.000100"}}`,
		},
		{
			name: "a team id that could not be an address workspace",
			body: `{"type":"event_callback","team_id":"T01:BAD","event_id":"Ev6","event":{
				"type":"app_mention","user":"U0HUMAN1","text":"<@U08BOTID> hi",
				"channel":"C02CHANNEL","ts":"1700000090.000100"}}`,
		},
	}

	c, tags := start(t, slack.Config{Bots: map[string]string{"U08BOTID": "docs-bot"}})
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			c.ServeHTTP(w, post(t, tc.body))
			// Acknowledged, so Slack does not retry an event we will never want.
			if w.Code != http.StatusOK {
				t.Errorf("status = %d, want 200", w.Code)
			}
			select {
			case tag := <-tags:
				t.Errorf("raised a tag: %+v", tag)
			default:
			}
		})
	}
}

func TestADirectMessageIsAMentionByContext(t *testing.T) {
	body := `{"type":"event_callback","team_id":"T01ABCDEF","event_id":"EvDM","event":{
		"type":"message","channel_type":"im","user":"U0HUMAN1","text":"what changed yesterday?",
		"channel":"D02DM","ts":"1700000090.000100","event_ts":"1700000090.000100"}}`

	c, tags := start(t, slack.Config{DefaultAgent: "docs-bot"})
	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, body))
	tag := receive(t, tags)

	if tag.Agent != "docs-bot" {
		t.Errorf("Tag.Agent = %q, want the default agent", tag.Agent)
	}
	if tag.Text != "what changed yesterday?" {
		t.Errorf("Tag.Text = %q", tag.Text)
	}
	if tag.Meta["slack_channel_type"] != "im" {
		t.Errorf("Meta[slack_channel_type] = %q, want im", tag.Meta["slack_channel_type"])
	}
}

func TestAnUnsignedOrTamperedRequestIsRefusedBeforeItIsParsed(t *testing.T) {
	c, tags := start(t, slack.Config{Bots: map[string]string{"U08BOTID": "docs-bot"}})

	tests := []struct {
		name    string
		mutate  func(*http.Request)
		wantSts int
	}{
		{
			name:    "no signature at all",
			mutate:  func(r *http.Request) { r.Header.Del(signature.HeaderSlackSignature) },
			wantSts: http.StatusUnauthorized,
		},
		{
			name:    "a signature over different bytes",
			mutate:  func(r *http.Request) { r.Header.Set(signature.HeaderSlackSignature, "v0="+strings.Repeat("ab", 32)) },
			wantSts: http.StatusUnauthorized,
		},
		{
			name:    "a stale timestamp, outside the replay window",
			mutate:  func(r *http.Request) { r.Header.Set(signature.HeaderSlackTimestamp, "1600000000") },
			wantSts: http.StatusUnauthorized,
		},
		{
			name:    "a method Slack never uses",
			mutate:  func(r *http.Request) { r.Method = http.MethodGet },
			wantSts: http.StatusMethodNotAllowed,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := post(t, appMention)
			tc.mutate(r)
			w := httptest.NewRecorder()
			c.ServeHTTP(w, r)
			if w.Code != tc.wantSts {
				t.Errorf("status = %d, want %d", w.Code, tc.wantSts)
			}
			select {
			case tag := <-tags:
				t.Errorf("an unauthenticated request raised a tag: %+v", tag)
			default:
			}
		})
	}
}

func TestAConnectorWithNoVerifierRefusesEveryRequest(t *testing.T) {
	c, err := slack.New(slack.Config{API: slack.NewFake()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, appMention))
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500: an unconfigured webhook is broken, not open", w.Code)
	}
}

func TestURLVerificationIsAnsweredButOnlyWhenSigned(t *testing.T) {
	c, _ := start(t, slack.Config{})
	body := `{"type":"url_verification","token":"tok","challenge":"3eZbrw1aB"}`

	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, body))
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}
	var got map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["challenge"] != "3eZbrw1aB" {
		t.Errorf("challenge = %q, want it echoed", got["challenge"])
	}

	unsigned := httptest.NewRequest(http.MethodPost, "/slack/events", strings.NewReader(body))
	w = httptest.NewRecorder()
	c.ServeHTTP(w, unsigned)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("unsigned challenge status = %d, want 401", w.Code)
	}
}

func TestARequestArrivingBeforeIngestAsksSlackToRetry(t *testing.T) {
	c, err := slack.New(slack.Config{
		API:      slack.NewFake(),
		Verifier: verifier(),
		Bots:     map[string]string{"U08BOTID": "docs-bot"},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// No Ingest is running: the tag has nowhere to go, and 503 is the answer
	// that makes Slack redeliver an event we would otherwise drop.
	w := httptest.NewRecorder()
	c.ServeHTTP(w, post(t, appMention))
	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", w.Code)
	}
}

// ── Sink ────────────────────────────────────────────────────────────

var target = address.MustParse("slack://T01ABCDEF/C02CHANNEL?thread=1700000000.000100")

func event(seq uint64, kind envelope.Kind, payload string) envelope.Event {
	return envelope.Event{
		Seq:     seq,
		Topic:   topic.MustParse("agent:docs-bot:run_1"),
		RunID:   "run_1",
		Agent:   "docs-bot",
		Origin:  "slack",
		Kind:    kind,
		Payload: []byte(payload),
		At:      time.Unix(1700000100, 0).Add(time.Duration(seq) * time.Second),
	}
}

func textEvent(seq uint64, s string) envelope.Event {
	return event(seq, envelope.KindText, `{"text":`+jsonString(s)+`}`)
}

func jsonString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func sinkConnector(t *testing.T, api slack.API) *slack.Connector {
	t.Helper()
	c, err := slack.New(slack.Config{API: api, Team: "T01ABCDEF", Interval: -1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c
}

func TestDeliveringTheSameEventsTwiceKeepsOneEditedMessage(t *testing.T) {
	fake := slack.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	events := []envelope.Event{
		event(1, envelope.KindAccepted, ""),
		textEvent(2, "The design doc says "),
		textEvent(3, "three things."),
		event(4, envelope.KindCitation, `{"title":"Design doc","uri":"https://example.com/design"}`),
		event(5, envelope.KindCompleted, ""),
	}
	for range 2 {
		for _, e := range events {
			if err := c.Deliver(ctx, target, e); err != nil {
				t.Fatalf("Deliver(seq %d): %v", e.Seq, err)
			}
		}
	}

	messages := fake.Messages()
	if len(messages) != 1 {
		t.Fatalf("slack holds %d messages, want exactly 1: %+v", len(messages), messages)
	}
	msg := messages[0]
	if msg.Channel != "C02CHANNEL" {
		t.Errorf("channel = %q", msg.Channel)
	}
	if msg.ThreadTS != "1700000000.000100" {
		t.Errorf("thread_ts = %q, want the answer threaded under the mention", msg.ThreadTS)
	}
	if got := strings.Count(msg.Text, "The design doc says three things."); got != 1 {
		t.Errorf("body contains the answer %d times, want 1:\n%s", got, msg.Text)
	}
	if !strings.Contains(msg.Text, "*docs-bot* _(done)_") {
		t.Errorf("body is not mrkdwn:\n%s", msg.Text)
	}
	if !strings.Contains(msg.Text, "[1] <https://example.com/design|Design doc>") {
		t.Errorf("citation is not a mrkdwn footnote link:\n%s", msg.Text)
	}
}

func TestOutOfOrderDeliveryDoesNotRenderStaleTextOverFresh(t *testing.T) {
	fake := slack.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	for _, e := range []envelope.Event{
		textEvent(3, "second"),
		textEvent(2, "first "),
		event(4, envelope.KindCompleted, ""),
	} {
		if err := c.Deliver(ctx, target, e); err != nil {
			t.Fatal(err)
		}
	}
	msg := fake.Messages()[0]
	if !strings.Contains(msg.Text, "first second") {
		t.Errorf("body = %q, want the fragments in sequence order", msg.Text)
	}
}

func TestSlackMarkupInAnAnswerIsEscapedRatherThanRendered(t *testing.T) {
	fake := slack.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	// A model that echoes a mention must not produce a real one, and a tool
	// result with an angle bracket must not produce broken markup.
	if err := c.Deliver(ctx, target, textEvent(1, "compare <@U0HUMAN1> & a < b")); err != nil {
		t.Fatal(err)
	}
	msg := fake.Messages()[0]
	if strings.Contains(msg.Text, "<@U0HUMAN1>") {
		t.Errorf("an answer produced a live mention:\n%s", msg.Text)
	}
	if !strings.Contains(msg.Text, "&lt;@U0HUMAN1&gt; &amp; a &lt; b") {
		t.Errorf("body = %q, want the prose escaped", msg.Text)
	}
	// Our own markup is not escaped.
	if !strings.Contains(msg.Text, "*docs-bot*") {
		t.Errorf("the header was escaped along with the prose:\n%s", msg.Text)
	}
}

func TestDeliveryToAWorkspaceThisConnectorDoesNotServeIsRefused(t *testing.T) {
	c := sinkConnector(t, slack.NewFake())
	ctx := context.Background()

	tests := []struct {
		name   string
		target string
	}{
		{name: "another workspace", target: "slack://T99OTHER/C02CHANNEL"},
		{name: "another connector's scheme", target: "github://urmzd/mandatum/issues/1"},
		{name: "no channel", target: "slack://T01ABCDEF"},
		{name: "too much path", target: "slack://T01ABCDEF/C02CHANNEL/extra"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := c.Deliver(ctx, address.MustParse(tc.target), textEvent(1, "hi"))
			if !isUndeliverable(err) {
				t.Errorf("Deliver error = %v, want ErrUndeliverable", err)
			}
		})
	}
}

func isUndeliverable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "undeliverable")
}

// ── Actor ───────────────────────────────────────────────────────────

func TestActionsPostAndReactAndReportWhatTheyChanged(t *testing.T) {
	fake := slack.NewFake()
	c := sinkConnector(t, fake)
	ctx := context.Background()

	actions := map[string]connector.Action{}
	for _, a := range c.Actions() {
		actions[a.Name] = a
	}
	if len(actions) != 2 {
		t.Fatalf("actions = %v, want two", actions)
	}

	post := actions["slack_post_message"]
	res, err := post.Invoke(ctx, target, json.RawMessage(`{"text":"heads up"}`))
	if err != nil {
		t.Fatalf("slack_post_message: %v", err)
	}
	if res.Address.Connector != "slack" || res.Address.Workspace != "T01ABCDEF" {
		t.Errorf("Result.Address = %s, want the message that was posted", res.Address)
	}
	if _, err := address.Parse(res.Address.String()); err != nil {
		t.Errorf("Result.Address does not round trip: %v", err)
	}
	if res.Summary == "" {
		t.Error("Result.Summary is empty, so the agent learns nothing")
	}
	posted := fake.Messages()
	if len(posted) != 1 || posted[0].Text != "heads up" {
		t.Fatalf("messages = %+v", posted)
	}
	if posted[0].ThreadTS != "1700000000.000100" {
		t.Errorf("thread_ts = %q, want the run's thread by default", posted[0].ThreadTS)
	}

	react := actions["slack_add_reaction"]
	ts := posted[0].TS
	if _, err := react.Invoke(ctx, target, json.RawMessage(`{"name":"eyes","ts":"`+ts+`"}`)); err != nil {
		t.Fatalf("slack_add_reaction: %v", err)
	}
	msg, _ := fake.Message(ts)
	if len(msg.Reactions) != 1 || msg.Reactions[0] != "eyes" {
		t.Errorf("reactions = %v, want [eyes]", msg.Reactions)
	}
}

func TestEveryActionIsSelfDescribing(t *testing.T) {
	c := sinkConnector(t, slack.NewFake())
	for _, a := range c.Actions() {
		t.Run(a.Name, func(t *testing.T) {
			if !strings.HasPrefix(a.Name, "slack_") {
				t.Errorf("Name = %q, want the connector prefix", a.Name)
			}
			if a.Description == "" {
				t.Error("no description, so a model cannot know when to call it")
			}
			if a.Invoke == nil {
				t.Fatal("no Invoke")
			}
			var schema map[string]any
			if err := json.Unmarshal(a.Schema, &schema); err != nil {
				t.Fatalf("schema is not valid JSON: %v", err)
			}
			if schema["type"] != "object" {
				t.Errorf("schema type = %v, want object", schema["type"])
			}
			if _, ok := schema["properties"]; !ok {
				t.Error("schema has no properties")
			}
		})
	}
}

func TestAnActionCannotReachIntoAnotherWorkspace(t *testing.T) {
	c := sinkConnector(t, slack.NewFake())
	for _, a := range c.Actions() {
		args := json.RawMessage(`{"text":"x","name":"eyes","ts":"1","channel":"C0OTHER"}`)
		if _, err := a.Invoke(context.Background(), address.MustParse("slack://T99OTHER/C1"), args); !isUndeliverable(err) {
			t.Errorf("%s against another workspace: error = %v, want ErrUndeliverable", a.Name, err)
		}
	}
}

// ── Roles ───────────────────────────────────────────────────────────

func TestSlackImplementsAllThreeFaces(t *testing.T) {
	c := sinkConnector(t, slack.NewFake())
	roles := connector.RolesOf(c)
	if !roles.Trigger || !roles.Sink || !roles.Actor {
		t.Errorf("RolesOf(slack) = %+v, want all three faces", roles)
	}
}

func TestNewRejectsAConfigurationThatCouldNotAddress(t *testing.T) {
	tests := []struct {
		name string
		cfg  slack.Config
	}{
		{name: "no api", cfg: slack.Config{}},
		{name: "a team that cannot be a workspace", cfg: slack.Config{API: slack.NewFake(), Team: "T01:BAD"}},
		{name: "a bot mapped to an unusable agent name", cfg: slack.Config{API: slack.NewFake(), Bots: map[string]string{"U1": "docs bot"}}},
		{name: "an unusable default agent", cfg: slack.Config{API: slack.NewFake(), DefaultAgent: "docs bot"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := slack.New(tc.cfg); err == nil {
				t.Error("New accepted a configuration that cannot work")
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
