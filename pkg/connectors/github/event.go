package github

import (
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/connectors/mention"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// GitHub's webhook headers.
const (
	// HeaderEvent names the event type: issue_comment, pull_request, issues.
	HeaderEvent = "X-GitHub-Event"
	// HeaderDelivery is GitHub's own identifier for this delivery, and becomes
	// Tag.ID. A redelivery from the UI or a retry after a timeout carries the
	// same GUID, which is what makes the run idempotency key correct rather than
	// merely unique.
	HeaderDelivery = "X-GitHub-Delivery"
	// HeaderHookID identifies the webhook configuration that sent the event.
	HeaderHookID = "X-GitHub-Hook-ID"
)

// hook is the union of the webhook payloads this connector reads. GitHub sends
// one JSON object per event type with a shared shape; decoding the union and
// switching on the event name is simpler than five near-identical structs, and
// an unknown field is ignored rather than rejected.
type hook struct {
	Action     string     `json:"action"`
	Repository repository `json:"repository"`
	Sender     user       `json:"sender"`

	Issue        *issue   `json:"issue"`
	PullRequest  *pull    `json:"pull_request"`
	Comment      *body    `json:"comment"`
	Review       *body    `json:"review"`
	Label        *label   `json:"label"`
	Reviewer     *user    `json:"requested_reviewer"`
	Team         *teamRef `json:"requested_team"`
	Installation *idOnly  `json:"installation"`
}

type repository struct {
	Name     string `json:"name"`
	FullName string `json:"full_name"`
	Owner    user   `json:"owner"`
	Private  bool   `json:"private"`
}

type user struct {
	Login string `json:"login"`
	// Type is "User", "Bot" or "Organization". A Bot sender is never acted on:
	// this connector's own comments arrive back as issue_comment events, and
	// answering them would make the mesh answer itself.
	Type string `json:"type"`
}

type teamRef struct {
	Slug string `json:"slug"`
}

type idOnly struct {
	ID int64 `json:"id"`
}

type label struct {
	Name string `json:"name"`
}

type issue struct {
	Number int     `json:"number"`
	Title  string  `json:"title"`
	Body   string  `json:"body"`
	State  string  `json:"state"`
	User   user    `json:"user"`
	Labels []label `json:"labels"`
	// PullRequest is present when this "issue" is really a pull request, which
	// is how an issue_comment event on a PR is told apart from one on an issue.
	PullRequest *struct {
		URL string `json:"url"`
	} `json:"pull_request"`
	HTMLURL   string    `json:"html_url"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type pull struct {
	Number  int     `json:"number"`
	Title   string  `json:"title"`
	Body    string  `json:"body"`
	State   string  `json:"state"`
	Draft   bool    `json:"draft"`
	User    user    `json:"user"`
	Labels  []label `json:"labels"`
	HTMLURL string  `json:"html_url"`
	Base    struct {
		Ref string `json:"ref"`
	} `json:"base"`
	Head struct {
		Ref string `json:"ref"`
		SHA string `json:"sha"`
	} `json:"head"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// body is a comment or a review: an author, some text, an id and a timestamp.
// A review dates itself with submitted_at rather than created_at, so both are
// decoded and whichever is present is used.
type body struct {
	ID          int64     `json:"id"`
	Body        string    `json:"body"`
	User        user      `json:"user"`
	HTMLURL     string    `json:"html_url"`
	CreatedAt   time.Time `json:"created_at"`
	SubmittedAt time.Time `json:"submitted_at"`
}

// at returns when this comment or review was written.
func (b *body) at() time.Time {
	if b == nil {
		return time.Time{}
	}
	return latest(b.CreatedAt, b.SubmittedAt)
}

// subject is the issue or pull request an event is about, normalized.
type subject struct {
	number int
	title  string
	body   string
	state  string
	author string
	labels []string
	url    string
	// pull reports whether the address should say /pull/ rather than /issues/.
	pull  bool
	base  string
	head  string
	draft bool
	at    time.Time
}

// tagFrom translates one webhook delivery into a Tag, or reports that it raises
// none.
//
// The events that raise tags are the ones where a human named an agent
// (issue_comment, issues, pull_request opened, a review or review comment) and
// the one where GitHub names it structurally (pull_request review_requested).
// Everything else — a push, a label change, a merge — is acknowledged and
// dropped, because a mesh that ran an agent on every repository event would be a
// denial of service with a bill attached.
func (c *Connector) tagFrom(event, delivery string, h hook) (envelope.Tag, bool) {
	// Loop prevention first, and unconditionally: this connector comments on
	// issues, and its own comment comes back as an issue_comment event.
	if h.Sender.Type == "Bot" {
		return envelope.Tag{}, false
	}
	if h.Repository.Name == "" || h.Repository.Owner.Login == "" {
		return envelope.Tag{}, false
	}
	if err := address.ValidWorkspace(h.Repository.Owner.Login); err != nil {
		return envelope.Tag{}, false
	}

	var (
		subj      subject
		text      string
		agent     string
		commentID int64
		ok        bool
	)

	switch event {
	case "issue_comment":
		if h.Action != "created" || h.Issue == nil || h.Comment == nil {
			return envelope.Tag{}, false
		}
		subj = subjectOf(h)
		commentID = h.Comment.ID
		if agent, text, ok = mention.Find(h.Comment.Body, c.agents()); !ok {
			return envelope.Tag{}, false
		}

	case "pull_request_review_comment":
		if h.Action != "created" || h.PullRequest == nil || h.Comment == nil {
			return envelope.Tag{}, false
		}
		subj = subjectOf(h)
		commentID = h.Comment.ID
		if agent, text, ok = mention.Find(h.Comment.Body, c.agents()); !ok {
			return envelope.Tag{}, false
		}

	case "pull_request_review":
		if h.Action != "submitted" || h.PullRequest == nil || h.Review == nil {
			return envelope.Tag{}, false
		}
		subj = subjectOf(h)
		commentID = h.Review.ID
		if agent, text, ok = mention.Find(h.Review.Body, c.agents()); !ok {
			return envelope.Tag{}, false
		}

	case "issues":
		if h.Action != "opened" || h.Issue == nil {
			return envelope.Tag{}, false
		}
		subj = subjectOf(h)
		if agent, text, ok = mention.Find(h.Issue.Title+"\n\n"+h.Issue.Body, c.agents()); !ok {
			return envelope.Tag{}, false
		}

	case "pull_request":
		if h.PullRequest == nil {
			return envelope.Tag{}, false
		}
		subj = subjectOf(h)
		switch h.Action {
		case "review_requested":
			// The agent is named by GitHub, not by a human: the review was
			// requested from its login. This is the one path where no mention
			// exists, and it is the natural way to tag a reviewer agent.
			login := ""
			if h.Reviewer != nil {
				login = h.Reviewer.Login
			} else if h.Team != nil {
				login = h.Team.Slug
			}
			if agent, ok = c.agents().Lookup(login); !ok {
				return envelope.Tag{}, false
			}
			// A review request carries no message, so the request IS the pull
			// request: its title and description are what the agent reviews.
			text = strings.TrimSpace(subj.title + "\n\n" + subj.body)
			text = mention.Strip(text, agent)
		case "opened", "ready_for_review":
			if agent, text, ok = mention.Find(subj.title+"\n\n"+subj.body, c.agents()); !ok {
				return envelope.Tag{}, false
			}
		default:
			return envelope.Tag{}, false
		}

	default:
		return envelope.Tag{}, false
	}

	if delivery == "" {
		// GitHub always sends a delivery GUID. Without one, the resource plus
		// the comment or subject identity is the best stable key available.
		delivery = event + ":" + h.Repository.FullName + ":" + strconv.Itoa(subj.number) + ":" + strconv.FormatInt(commentID, 10)
	}

	// When the event is a comment or a review, its own timestamp is when the
	// human asked; the subject's updated_at is only when the resource last
	// changed, which for a busy pull request is something else entirely.
	at := latest(h.Comment.at(), h.Review.at())
	if at.IsZero() {
		at = subj.at
	}

	tag := envelope.Tag{
		ID:      delivery,
		Agent:   agent,
		Origin:  Name,
		Source:  sourceOf(h.Repository, subj),
		Text:    strings.TrimSpace(text),
		Actor:   envelope.Actor{ID: h.Sender.Login, Bot: h.Sender.Type == "Bot"},
		Deliver: c.cfg.Deliver,
		Meta:    metaOf(event, h, subj, commentID),
		At:      at,
	}
	return tag, true
}

// subjectOf normalizes the issue or pull request an event is about.
func subjectOf(h hook) subject {
	if h.PullRequest != nil {
		p := h.PullRequest
		return subject{
			number: p.Number,
			title:  p.Title,
			body:   p.Body,
			state:  p.State,
			author: p.User.Login,
			labels: labelNames(p.Labels),
			url:    p.HTMLURL,
			pull:   true,
			base:   p.Base.Ref,
			head:   p.Head.Ref,
			draft:  p.Draft,
			at:     latest(p.UpdatedAt, p.CreatedAt),
		}
	}
	if h.Issue != nil {
		i := h.Issue
		s := subject{
			number: i.Number,
			title:  i.Title,
			body:   i.Body,
			state:  i.State,
			author: i.User.Login,
			labels: labelNames(i.Labels),
			url:    i.HTMLURL,
			// An issue_comment on a pull request arrives as an issue with a
			// pull_request member. Addressing it as an issue would still resolve
			// on GitHub, but it would tell the agent's tools the wrong thing
			// about what it is looking at.
			pull: i.PullRequest != nil,
			at:   latest(i.UpdatedAt, i.CreatedAt),
		}
		return s
	}
	return subject{}
}

// sourceOf builds the address of the surface the tag came from.
func sourceOf(repo repository, subj subject) address.Address {
	kind := "issues"
	if subj.pull {
		kind = "pull"
	}
	return address.Address{
		Connector: Name,
		Workspace: repo.Owner.Login,
		Path:      []string{repo.Name, kind, strconv.Itoa(subj.number)},
	}
}

// metaOf carries the repository context an agent's tools need. Without it the
// agent would have to ask GitHub what it is looking at before it can do
// anything, on every single run.
func metaOf(event string, h hook, subj subject, commentID int64) map[string]string {
	m := map[string]string{
		"github_event":  event,
		"github_action": h.Action,
		"github_owner":  h.Repository.Owner.Login,
		"github_repo":   h.Repository.Name,
		"github_number": strconv.Itoa(subj.number),
		"github_kind":   kindOf(subj),
		"github_title":  subj.title,
		"github_sender": h.Sender.Login,
	}
	set := func(key, value string) {
		if value != "" {
			m[key] = value
		}
	}
	set("github_state", subj.state)
	set("github_author", subj.author)
	set("github_url", subj.url)
	set("github_base_ref", subj.base)
	set("github_head_ref", subj.head)
	set("github_labels", strings.Join(subj.labels, ","))
	if subj.pull && subj.draft {
		m["github_draft"] = "true"
	}
	if commentID != 0 {
		m["github_comment_id"] = strconv.FormatInt(commentID, 10)
	}
	if h.Installation != nil && h.Installation.ID != 0 {
		m["github_installation_id"] = strconv.FormatInt(h.Installation.ID, 10)
	}
	return m
}

func kindOf(subj subject) string {
	if subj.pull {
		return "pull"
	}
	return "issue"
}

func labelNames(labels []label) []string {
	if len(labels) == 0 {
		return nil
	}
	out := make([]string, 0, len(labels))
	for _, l := range labels {
		if l.Name != "" {
			out = append(out, l.Name)
		}
	}
	return out
}

func latest(times ...time.Time) time.Time {
	var out time.Time
	for _, t := range times {
		if t.After(out) {
			out = t
		}
	}
	return out
}
