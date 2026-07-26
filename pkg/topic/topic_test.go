package topic_test

import (
	"errors"
	"testing"

	"github.com/urmzd/opentag/pkg/topic"
)

func TestParseRejectsMalformed(t *testing.T) {
	cases := []struct{ name, in string }{
		{"empty", ""},
		{"wrong root", "run:run_01J"},
		{"bare name without root", "docs-bot"},
		{"too deep", "agent:docs-bot:run_01J:delta"},
		{"empty segment", "agent::run_01J"},
		{"trailing separator", "agent:docs-bot:"},
		{"dot in segment", "agent:docs.bot"},
		{"slash in segment", "agent:urmzd/opentag"},
		{"space in segment", "agent:docs bot"},
		{"star is not a wildcard", "agent:*"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := topic.Parse(tc.in); err == nil {
				t.Fatalf("Parse(%q) = nil error, want ErrInvalid", tc.in)
			} else if !errors.Is(err, topic.ErrInvalid) {
				t.Fatalf("Parse(%q) error = %v, want ErrInvalid", tc.in, err)
			}
		})
	}
}

func TestParseRoundTrip(t *testing.T) {
	for _, in := range []string{
		"agent",
		"agent:docs-bot",
		"agent:docs-bot:run_01J",
		"agent:Review_Bot2:RUN-9",
	} {
		got, err := topic.Parse(in)
		if err != nil {
			t.Fatalf("Parse(%q): %v", in, err)
		}
		if got.String() != in {
			t.Fatalf("Parse(%q).String() = %q", in, got.String())
		}
	}
}

// Covers is the whole subscription model: publish to the most specific topic,
// subscribe to any prefix.
func TestCovers(t *testing.T) {
	cases := []struct {
		name           string
		sub, published string
		want           bool
	}{
		{"root covers everything", "agent", "agent:docs-bot:run_01J", true},
		{"root covers an agent", "agent", "agent:docs-bot", true},
		{"agent covers its runs", "agent:docs-bot", "agent:docs-bot:run_01J", true},
		{"agent covers itself", "agent:docs-bot", "agent:docs-bot", true},
		{"run covers itself", "agent:docs-bot:run_01J", "agent:docs-bot:run_01J", true},

		{"other agent excluded", "agent:docs-bot", "agent:triage-bot:run_01J", false},
		{"other run excluded", "agent:docs-bot:run_01J", "agent:docs-bot:run_02K", false},
		{"run does not cover the agent", "agent:docs-bot:run_01J", "agent:docs-bot", false},
		{"agent does not cover the root", "agent:docs-bot", "agent", false},

		// Segment-wise, not string-wise: this is the bug a naive
		// strings.HasPrefix would ship.
		{"name prefix is not containment", "agent:docs", "agent:docs-bot", false},
		{"name prefix is not containment, deep", "agent:docs", "agent:docs-bot:run_01J", false},
		{"run id prefix is not containment", "agent:docs-bot:run_01", "agent:docs-bot:run_01J", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := topic.MustParse(tc.sub)
			pub := topic.MustParse(tc.published)
			if got := sub.Covers(pub); got != tc.want {
				t.Fatalf("%q.Covers(%q) = %v, want %v", tc.sub, tc.published, got, tc.want)
			}
		})
	}
}

func TestCoversZeroValue(t *testing.T) {
	if (topic.Topic{}).Covers(topic.MustParse("agent:docs-bot")) {
		t.Fatal("zero Topic covered a topic")
	}
	if topic.All().Covers(topic.Topic{}) {
		t.Fatal("root covered the zero Topic")
	}
}

func TestConstructors(t *testing.T) {
	if got := topic.All().String(); got != "agent" {
		t.Fatalf("All() = %q", got)
	}

	a, err := topic.Agent("docs-bot")
	if err != nil {
		t.Fatalf("Agent: %v", err)
	}
	if a.String() != "agent:docs-bot" || a.Name() != "docs-bot" || a.RunID() != "" {
		t.Fatalf("Agent = %q name=%q run=%q", a.String(), a.Name(), a.RunID())
	}

	r, err := topic.Run("docs-bot", "run_01J")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if r.String() != "agent:docs-bot:run_01J" || r.Name() != "docs-bot" || r.RunID() != "run_01J" {
		t.Fatalf("Run = %q name=%q run=%q", r.String(), r.Name(), r.RunID())
	}

	if _, err := topic.Agent(""); err == nil {
		t.Fatal("Agent(\"\") = nil error")
	}
	if _, err := topic.Run("docs-bot", ""); err == nil {
		t.Fatal("Run with empty id = nil error")
	}
}

// The listen-to-an-agent path: one subscription, every run, including runs
// that did not exist when the subscription was opened.
func TestListeningToAnAgentReceivesEveryRun(t *testing.T) {
	sub := topic.MustParse("agent:docs-bot")
	for _, runID := range []string{"run_01J", "run_02K", "run_99Z"} {
		pub, err := topic.Run("docs-bot", runID)
		if err != nil {
			t.Fatalf("Run(%q): %v", runID, err)
		}
		if !sub.Covers(pub) {
			t.Fatalf("agent subscription missed run %q", runID)
		}
	}
}
