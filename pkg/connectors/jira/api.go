package jira

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/urmzd/mandatum/pkg/connectors/internal/httpjson"
)

// Site is one Jira instance: where it lives, and the credential for it.
//
// The map key a Site is stored under is the SHORT KEY, and that key — not the
// hostname — is what appears in an address workspace. See the package doc for
// why: "acme.atlassian.net" cannot be an address workspace, because a workspace
// becomes a subject token in the mesh delivery family and a "." is not
// subject-token safe. So a deployment names its Jira sites and the names travel:
//
//	Sites: map[string]jira.Site{"acme": {BaseURL: "https://acme.atlassian.net", ...}}
//	                            ^^^^^^                      ^^^^^^^^^^^^^^^^^^
//	                            jira://acme/PROJ-5          never in an address
//
// One map serves both halves of the connector. The Connector reads BaseURL, to
// recognise which site an inbound webhook came from and to build browse links;
// the API reads all three, to make calls.
type Site struct {
	// BaseURL is the instance root, without a trailing slash:
	// "https://acme.atlassian.net", or a Data Center host with its context path.
	BaseURL string

	// Email is the Atlassian account the token belongs to. Jira Cloud's Basic
	// auth is email plus API token, not username plus password.
	Email string

	// Token is the API token.
	Token string
}

// BrowseURL is the human-facing URL of an issue on this site.
func (s Site) BrowseURL(issue string) string {
	return strings.TrimRight(s.BaseURL, "/") + "/browse/" + url.PathEscape(issue)
}

// Ref names one Jira issue: the short site key it lives in, and its issue key.
type Ref struct {
	// Site is the short key, as configured. Never a hostname.
	Site string
	// Issue is the issue key: "PROJ-5".
	Issue string
}

// String renders the ref the way a human refers to it.
func (r Ref) String() string {
	if r.Site == "" {
		return r.Issue
	}
	return r.Site + "/" + r.Issue
}

// Transition is one move an issue can make from the status it is in now.
//
// Jira's API takes a transition ID, and an ID is meaningless to a model and
// differs per workflow. Listing them so the agent can name "Done" and the
// connector can resolve it is the difference between an action a model can call
// and one it can only guess at.
type Transition struct {
	ID string
	// Name is the button a human would click: "Start Progress", "Done".
	Name string
	// To is the status the issue lands in.
	To string
}

// API is the slice of Jira's REST API this connector uses. Five calls: the two
// that keep one comment per run, and the three native verbs the agent can reach
// for.
type API interface {
	// CreateComment posts a comment on an issue and returns its id, which is the
	// handle for editing it.
	CreateComment(ctx context.Context, ref Ref, body string) (id string, err error)
	// UpdateComment replaces the body of a comment.
	UpdateComment(ctx context.Context, ref Ref, id, body string) error
	// Transitions lists the moves available from the issue's current status.
	Transitions(ctx context.Context, ref Ref) ([]Transition, error)
	// Transition applies one, by id.
	Transition(ctx context.Context, ref Ref, id string) error
	// Assign sets the assignee to an Atlassian account id. An empty id
	// unassigns.
	Assign(ctx context.Context, ref Ref, accountID string) error
}

// apiBase is the REST prefix every call in this file uses.
//
// Version 2, not 3, and the reason is comments. The v3 comment endpoints take an
// Atlassian Document Format tree and nothing else, so posting a rendered body
// through them would mean parsing our own wiki markup back into a document —
// building a structure out of a string we had just flattened. v2 accepts wiki
// markup as a string, which is exactly what the sink renders. The other three
// calls are identical across both versions, so there is no split to keep track
// of.
const apiBase = "/rest/api/2"

// HTTPAPI is the real API, over net/http. It holds one client per site.
type HTTPAPI struct {
	clients map[string]*httpjson.Client
}

var (
	_ API = (*HTTPAPI)(nil)
	_ API = (*Fake)(nil)
)

// ErrUnknownSite reports a call against a site this API holds no credential for.
// It is separate from connector.ErrUndeliverable because it is a configuration
// fault on our side rather than a statement about the address.
type ErrUnknownSite struct{ Site string }

func (e *ErrUnknownSite) Error() string {
	return fmt.Sprintf("jira: no credential configured for site %q", e.Site)
}

// NewHTTPAPI returns an API over the given sites, authenticating with Basic auth
// as Atlassian Cloud requires: the account email as the user, the API token as
// the password.
//
// Pass the same map the Connector is configured with, so a site exists in
// exactly one place.
func NewHTTPAPI(sites map[string]Site, httpClient *http.Client) (*HTTPAPI, error) {
	a := &HTTPAPI{clients: make(map[string]*httpjson.Client, len(sites))}
	for key, site := range sites {
		if site.BaseURL == "" {
			return nil, fmt.Errorf("jira: site %q has no base url", key)
		}
		if site.Token == "" {
			return nil, fmt.Errorf("jira: site %q has no api token", key)
		}
		header := http.Header{}
		header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte(site.Email+":"+site.Token)))
		header.Set("Accept", "application/json")
		a.clients[key] = &httpjson.Client{
			HTTP:   httpClient,
			Base:   strings.TrimRight(site.BaseURL, "/") + apiBase,
			Header: header,
		}
	}
	return a, nil
}

func (a *HTTPAPI) client(ref Ref) (*httpjson.Client, error) {
	c, ok := a.clients[ref.Site]
	if !ok {
		return nil, &ErrUnknownSite{Site: ref.Site}
	}
	return c, nil
}

// commentResponse is the part of a comment response this connector reads. Jira
// ids are strings, not numbers, and treating them as numbers is how a connector
// breaks on a Data Center instance that does not use them.
type commentResponse struct {
	ID   string `json:"id"`
	Self string `json:"self"`
}

// CreateComment implements API.
func (a *HTTPAPI) CreateComment(ctx context.Context, ref Ref, body string) (string, error) {
	client, err := a.client(ref)
	if err != nil {
		return "", err
	}
	var out commentResponse
	path := "/issue/" + url.PathEscape(ref.Issue) + "/comment"
	if err := client.Do(ctx, http.MethodPost, path, map[string]string{"body": body}, &out); err != nil {
		return "", fmt.Errorf("jira: comment on %s: %w", ref, err)
	}
	if out.ID == "" {
		return "", fmt.Errorf("jira: comment on %s returned no id, so it cannot be edited", ref)
	}
	return out.ID, nil
}

// UpdateComment implements API.
func (a *HTTPAPI) UpdateComment(ctx context.Context, ref Ref, id, body string) error {
	client, err := a.client(ref)
	if err != nil {
		return err
	}
	path := "/issue/" + url.PathEscape(ref.Issue) + "/comment/" + url.PathEscape(id)
	if err := client.Do(ctx, http.MethodPut, path, map[string]string{"body": body}, nil); err != nil {
		return fmt.Errorf("jira: edit comment %s on %s: %w", id, ref, err)
	}
	return nil
}

// transitionsResponse is Jira's transition list.
type transitionsResponse struct {
	Transitions []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		To   struct {
			Name string `json:"name"`
		} `json:"to"`
	} `json:"transitions"`
}

// Transitions implements API.
func (a *HTTPAPI) Transitions(ctx context.Context, ref Ref) ([]Transition, error) {
	client, err := a.client(ref)
	if err != nil {
		return nil, err
	}
	var out transitionsResponse
	path := "/issue/" + url.PathEscape(ref.Issue) + "/transitions"
	if err := client.Do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("jira: list transitions for %s: %w", ref, err)
	}
	transitions := make([]Transition, 0, len(out.Transitions))
	for _, t := range out.Transitions {
		transitions = append(transitions, Transition{ID: t.ID, Name: t.Name, To: t.To.Name})
	}
	return transitions, nil
}

// Transition implements API.
func (a *HTTPAPI) Transition(ctx context.Context, ref Ref, id string) error {
	client, err := a.client(ref)
	if err != nil {
		return err
	}
	req := map[string]any{"transition": map[string]string{"id": id}}
	path := "/issue/" + url.PathEscape(ref.Issue) + "/transitions"
	if err := client.Do(ctx, http.MethodPost, path, req, nil); err != nil {
		return fmt.Errorf("jira: transition %s with %s: %w", ref, id, err)
	}
	return nil
}

// Assign implements API.
//
// A nil accountId is how Jira spells "unassigned", which is why the field is
// typed as any: the empty string is a different thing to Jira, and sending it
// fails rather than unassigning.
func (a *HTTPAPI) Assign(ctx context.Context, ref Ref, accountID string) error {
	client, err := a.client(ref)
	if err != nil {
		return err
	}
	req := map[string]any{"accountId": nil}
	if accountID != "" {
		req["accountId"] = accountID
	}
	path := "/issue/" + url.PathEscape(ref.Issue) + "/assignee"
	if err := client.Do(ctx, http.MethodPut, path, req, nil); err != nil {
		return fmt.Errorf("jira: assign %s: %w", ref, err)
	}
	return nil
}
