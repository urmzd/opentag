package slack

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"
)

// Fake is a complete in-memory Slack, not a stub.
//
// It ships in the package rather than in a test file for the same reason the
// in-memory bus does: an integrator wiring the mesh together, a demo, or an
// example should be able to run the whole path — mention in, run, edited message
// out — with no credentials and no network. Everything the sink relies on is
// modelled: a message has a timestamp, an edit replaces its text, an edit of a
// timestamp that does not exist fails the way Slack fails it, and reactions
// accumulate per message.
type Fake struct {
	mu       sync.Mutex
	messages []FakeMessage
	byTS     map[string]int
	seq      int64
	// Err, when set, fails every call. It is how a test drives the retry path.
	Err error
}

// FakeMessage is one message in the fake workspace.
type FakeMessage struct {
	Workspace string
	Channel   string
	ThreadTS  string
	TS        string
	Text      string
	// Edits counts how many times the text was replaced, which is what a test
	// asserting "one message, edited" looks at.
	Edits     int
	Reactions []string
}

// NewFake returns an empty fake workspace.
func NewFake() *Fake { return &Fake{byTS: make(map[string]int)} }

// PostMessage implements API.
func (f *Fake) PostMessage(_ context.Context, m Message) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return "", f.Err
	}
	if m.Channel == "" {
		return "", &Error{Method: "/chat.postMessage", Code: "channel_not_found"}
	}
	f.seq++
	// Slack timestamps are "<seconds>.<microseconds>" and are unique per
	// channel; the fake keeps the shape so that anything parsing one still works.
	ts := strconv.FormatInt(time.Unix(1700000000, 0).Unix(), 10) + "." + fmt.Sprintf("%06d", f.seq)
	f.messages = append(f.messages, FakeMessage{
		Workspace: m.Workspace,
		Channel:   m.Channel,
		ThreadTS:  m.ThreadTS,
		TS:        ts,
		Text:      m.Text,
	})
	f.byTS[ts] = len(f.messages) - 1
	return ts, nil
}

// UpdateMessage implements API.
func (f *Fake) UpdateMessage(_ context.Context, m Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	i, ok := f.byTS[m.TS]
	if !ok {
		return &Error{Method: "/chat.update", Code: "message_not_found"}
	}
	f.messages[i].Text = m.Text
	f.messages[i].Edits++
	return nil
}

// AddReaction implements API.
func (f *Fake) AddReaction(_ context.Context, r Reaction) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Err != nil {
		return f.Err
	}
	i, ok := f.byTS[r.TS]
	if !ok {
		return &Error{Method: "/reactions.add", Code: "message_not_found"}
	}
	for _, have := range f.messages[i].Reactions {
		if have == r.Name {
			return &Error{Method: "/reactions.add", Code: "already_reacted"}
		}
	}
	f.messages[i].Reactions = append(f.messages[i].Reactions, r.Name)
	return nil
}

// Messages returns every message posted, oldest first.
func (f *Fake) Messages() []FakeMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]FakeMessage(nil), f.messages...)
}

// Message returns the message at ts.
func (f *Fake) Message(ts string) (FakeMessage, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	i, ok := f.byTS[ts]
	if !ok {
		return FakeMessage{}, false
	}
	return f.messages[i], true
}
