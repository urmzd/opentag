package cli

import (
	"errors"
	"testing"
)

// The --deliver syntax is "<address>[|<kind>,...]". The separator is "|" and
// not "=" precisely because an address may already contain "=" inside a query
// parameter: slack://T01/C02?thread=1699.001 has one, and any rule for picking
// which "=" separates kinds gets that case wrong. "|" cannot appear in a URI,
// so the split is unambiguous.
func TestParseRoutesSeparatesKindsWithoutEatingQueryParameters(t *testing.T) {
	cases := []struct {
		name      string
		spec      string
		wantAddr  string
		wantKinds []string
	}{
		{
			name:     "address only",
			spec:     "github://urmzd/mandatum/issues/42",
			wantAddr: "github://urmzd/mandatum/issues/42",
		},
		{
			name:      "address with one kind",
			spec:      "webhook://acme/deploys|lifecycle.completed",
			wantAddr:  "webhook://acme/deploys",
			wantKinds: []string{"lifecycle.completed"},
		},
		{
			name:      "address with several kinds",
			spec:      "github://urmzd/mandatum/issues/42|delta,lifecycle.completed",
			wantAddr:  "github://urmzd/mandatum/issues/42",
			wantKinds: []string{"delta", "lifecycle.completed"},
		},
		{
			// The '=' here belongs to the query and must survive untouched.
			name:     "query parameter is not a kind separator",
			spec:     "slack://T01/C02?thread=1699.001",
			wantAddr: "slack://T01/C02?thread=1699.001",
		},
		{
			name:      "query parameter and a kind filter together",
			spec:      "slack://T01/C02?thread=1699.001|delta",
			wantAddr:  "slack://T01/C02?thread=1699.001",
			wantKinds: []string{"delta"},
		},
		{
			name:      "blank kinds are dropped",
			spec:      "jira://acme/PROJ-5|delta,,",
			wantAddr:  "jira://acme/PROJ-5",
			wantKinds: []string{"delta"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			routes, err := parseRoutes([]string{tc.spec})
			if err != nil {
				t.Fatalf("parseRoutes(%q): %v", tc.spec, err)
			}
			if len(routes) != 1 {
				t.Fatalf("got %d routes, want 1", len(routes))
			}
			target := routes[0].GetTarget()
			got := target.GetConnector() + "://" + target.GetWorkspace()
			for _, seg := range target.GetPath() {
				got += "/" + seg
			}
			wantAddr, _, _ := cut(tc.wantAddr, "?")
			if got != wantAddr {
				t.Errorf("address = %q, want %q", got, wantAddr)
			}
			if len(routes[0].GetKinds()) != len(tc.wantKinds) {
				t.Fatalf("kinds = %v, want %v", routes[0].GetKinds(), tc.wantKinds)
			}
			for i, k := range tc.wantKinds {
				if routes[0].GetKinds()[i] != k {
					t.Errorf("kinds[%d] = %q, want %q", i, routes[0].GetKinds()[i], k)
				}
			}
		})
	}
}

// A thread parameter must survive parsing, because it is what makes a Slack
// reply land in the conversation it answers rather than in the channel.
func TestParseRoutesKeepsQueryParameters(t *testing.T) {
	routes, err := parseRoutes([]string{"slack://T01/C02?thread=1699.001"})
	if err != nil {
		t.Fatalf("parseRoutes: %v", err)
	}
	if got := routes[0].GetTarget().GetParams()["thread"]; got != "1699.001" {
		t.Fatalf("thread param = %q, want 1699.001", got)
	}
}

func TestParseRoutesRejectsGarbageAsUsage(t *testing.T) {
	for _, spec := range []string{"not-an-address", "", "://nohost"} {
		_, err := parseRoutes([]string{spec})
		if err == nil {
			t.Fatalf("parseRoutes(%q) = nil error", spec)
		}
		if !errors.Is(err, errUsage) {
			t.Errorf("parseRoutes(%q) error = %v, want errUsage so it exits %d", spec, err, ExitUsage)
		}
	}
}

// kindFilter must agree with envelope.Kind.Matches: a family selects every kind
// beneath it, an exact kind selects only itself. If the two disagreed, --kinds
// would mean one thing on the client and another on the server.
func TestKindFilterMatchesFamiliesAndExactKinds(t *testing.T) {
	cases := []struct {
		name  string
		kinds []string
		kind  string
		want  bool
	}{
		{"no filter accepts everything", nil, "delta.text", true},
		{"family accepts its member", []string{"delta"}, "delta.citation", true},
		{"family rejects another family", []string{"delta"}, "lifecycle.completed", false},
		{"exact accepts itself", []string{"delta.citation"}, "delta.citation", true},
		{"exact rejects a sibling", []string{"delta.citation"}, "delta.text", false},
		{"any of several selectors", []string{"lifecycle", "delta.text"}, "delta.text", true},
		{"none of several selectors", []string{"lifecycle", "delta.text"}, "delta.citation", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := kindFilter(tc.kinds)(tc.kind); got != tc.want {
				t.Fatalf("kindFilter(%v)(%q) = %v, want %v", tc.kinds, tc.kind, got, tc.want)
			}
		})
	}
}

func TestParseKeyValues(t *testing.T) {
	got, err := parseKeyValues([]string{"repo=mandatum", "pr=42", "title=fix: a=b"})
	if err != nil {
		t.Fatalf("parseKeyValues: %v", err)
	}
	// The value keeps everything after the first '=', so a value containing
	// '=' is not truncated.
	if got["title"] != "fix: a=b" {
		t.Errorf("title = %q, want %q", got["title"], "fix: a=b")
	}
	if got["repo"] != "mandatum" || got["pr"] != "42" {
		t.Errorf("got %v", got)
	}

	if _, err := parseKeyValues([]string{"novalue"}); !errors.Is(err, errUsage) {
		t.Errorf("bare key error = %v, want errUsage", err)
	}
	if _, err := parseKeyValues([]string{"=novalue"}); !errors.Is(err, errUsage) {
		t.Errorf("empty key error = %v, want errUsage", err)
	}
}

func TestValidateTopicRejectsBadTopicsAsUsage(t *testing.T) {
	if _, err := validateTopic("agent:docs-bot"); err != nil {
		t.Fatalf("valid topic rejected: %v", err)
	}
	for _, bad := range []string{"", "run:1", "agent:a:b:c", "agent:docs.bot"} {
		if _, err := validateTopic(bad); !errors.Is(err, errUsage) {
			t.Errorf("validateTopic(%q) error = %v, want errUsage", bad, err)
		}
	}
}

func TestParseFormatIsStrict(t *testing.T) {
	for _, in := range []string{"text", "TEXT", " json ", ""} {
		if _, err := ParseFormat(in); err != nil {
			t.Errorf("ParseFormat(%q): %v", in, err)
		}
	}
	// A typo must fail rather than silently falling back to text, or a script
	// parsing stdout breaks somewhere far from the mistake.
	if _, err := ParseFormat("jsonl"); !errors.Is(err, errUsage) {
		t.Errorf("ParseFormat(\"jsonl\") error = %v, want errUsage", err)
	}
}

// cut is strings.Cut, restated so the test does not import strings for one call.
func cut(s, sep string) (before, after string, found bool) {
	for i := 0; i+len(sep) <= len(s); i++ {
		if s[i:i+len(sep)] == sep {
			return s[:i], s[i+len(sep):], true
		}
	}
	return s, "", false
}
