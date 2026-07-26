package slack

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/urmzd/opentag/pkg/connectors/internal/httpjson"
)

// API is the slice of Slack's Web API this connector uses: post a message, edit
// it, react to one. Four endpoints, behind an interface, so that every decision
// above the wire — which message a run owns, when to edit it, what the body says
// — is tested without a network and without credentials.
//
// Workspace is on every request so that an implementation serving several Slack
// workspaces can choose a token per request. HTTPAPI holds one token and ignores
// it (see Config.Team).
type API interface {
	// PostMessage posts and returns the message timestamp, which is Slack's
	// handle for editing it later.
	PostMessage(ctx context.Context, m Message) (ts string, err error)
	// UpdateMessage replaces the text of the message at m.TS.
	UpdateMessage(ctx context.Context, m Message) error
	// AddReaction adds an emoji reaction to a message.
	AddReaction(ctx context.Context, r Reaction) error
}

// Message is a message to post or edit.
type Message struct {
	// Workspace is the Slack team id the channel belongs to.
	Workspace string
	Channel   string
	// ThreadTS threads the message under an existing message. Empty posts to
	// the channel.
	ThreadTS string
	// TS identifies the message to edit. Empty posts a new one.
	TS   string
	Text string
}

// Reaction is an emoji reaction on a message.
type Reaction struct {
	Workspace string
	Channel   string
	// TS is the message being reacted to.
	TS string
	// Name is the emoji name without colons ("eyes", not ":eyes:").
	Name string
}

// SlackBaseURL is the Web API root.
const SlackBaseURL = "https://slack.com/api"

// HTTPAPI is the real API, over net/http.
//
// It holds one bot token and therefore serves one workspace. A deployment
// installed in several workspaces holds one HTTPAPI per workspace and an API
// implementation that dispatches on Message.Workspace.
type HTTPAPI struct {
	client *httpjson.Client
}

// NewHTTPAPI returns an API bound to a bot token (xoxb-...).
//
// The token goes in the Authorization header rather than in the body: Slack
// accepts both, and a token in a body is a token in a request log.
func NewHTTPAPI(token string, httpClient *http.Client) *HTTPAPI {
	header := http.Header{}
	header.Set("Authorization", "Bearer "+token)
	header.Set("Accept", "application/json")
	return &HTTPAPI{client: &httpjson.Client{HTTP: httpClient, Base: SlackBaseURL, Header: header}}
}

// Compile-time proof that the real implementation and the fake answer the same
// interface, which is the only reason the fake is worth anything.
var (
	_ API = (*HTTPAPI)(nil)
	_ API = (*Fake)(nil)
)

// response is the envelope every Web API method answers with.
//
// Slack reports application failures as HTTP 200 with ok:false, so a caller that
// only checks the status code treats "channel_not_found" and
// "not_in_channel" as success. Every call here checks ok.
type response struct {
	OK      bool   `json:"ok"`
	Error   string `json:"error"`
	Warning string `json:"warning,omitempty"`
	TS      string `json:"ts,omitempty"`
	Channel string `json:"channel,omitempty"`
}

// PostMessage implements API.
func (a *HTTPAPI) PostMessage(ctx context.Context, m Message) (string, error) {
	req := map[string]any{
		"channel": m.Channel,
		"text":    m.Text,
		// mrkdwn is the default, but saying so keeps a workspace-level setting
		// from silently turning the rendering into literal asterisks.
		"mrkdwn": true,
	}
	if m.ThreadTS != "" {
		req["thread_ts"] = m.ThreadTS
	}
	var out response
	if err := a.call(ctx, "/chat.postMessage", req, &out); err != nil {
		return "", err
	}
	if out.TS == "" {
		return "", fmt.Errorf("slack: chat.postMessage returned no ts, so the message cannot be edited")
	}
	return out.TS, nil
}

// UpdateMessage implements API.
func (a *HTTPAPI) UpdateMessage(ctx context.Context, m Message) error {
	if m.TS == "" {
		return fmt.Errorf("slack: chat.update needs the ts of the message to edit")
	}
	req := map[string]any{"channel": m.Channel, "ts": m.TS, "text": m.Text}
	return a.call(ctx, "/chat.update", req, &response{})
}

// AddReaction implements API.
func (a *HTTPAPI) AddReaction(ctx context.Context, r Reaction) error {
	req := map[string]any{"channel": r.Channel, "timestamp": r.TS, "name": r.Name}
	err := a.call(ctx, "/reactions.add", req, &response{})
	// Adding a reaction that is already there is the expected outcome of a
	// retry, not a failure: the action is idempotent by nature and saying so
	// here keeps every caller from special-casing it.
	if err != nil && errorCode(err) == "already_reacted" {
		return nil
	}
	return err
}

func (a *HTTPAPI) call(ctx context.Context, path string, in any, out *response) error {
	if err := a.client.Do(ctx, http.MethodPost, path, in, out); err != nil {
		return fmt.Errorf("slack: %s: %w", path, err)
	}
	if !out.OK {
		return &Error{Method: path, Code: out.Error}
	}
	return nil
}

// Error is an application-level Slack failure: HTTP 200 with ok:false.
type Error struct {
	Method string
	Code   string
}

func (e *Error) Error() string {
	return fmt.Sprintf("slack: %s: %s", e.Method, e.Code)
}

// errorCode returns the Slack error code from err, or "".
func errorCode(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}
