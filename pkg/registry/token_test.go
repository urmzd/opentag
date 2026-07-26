package registry

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"testing"
)

// This test is inside the package on purpose. The token is opaque to every
// caller, so the only place its encoding can be examined is where it is
// defined; examining it from outside would document a format the package
// promises not to have.

// A token is a position, and the position it decodes to is the one it encoded.
func TestTokenRoundTrips(t *testing.T) {
	t.Parallel()
	cases := []struct{ tenant, name string }{
		{"acme", "docs-bot"},
		{"acme", "z"},
		{"a-very-long-tenant-identifier", "agent-with-a-long-name_2"},
		{"ab", "c"},
		{"a", "bc"},
	}
	seen := make(map[string]string, len(cases))
	for _, tc := range cases {
		token := encodeToken(tc.tenant, tc.name)
		got, err := decodeToken(token, tc.tenant)
		if err != nil {
			t.Fatalf("decodeToken(encodeToken(%q, %q)): %v", tc.tenant, tc.name, err)
		}
		if got != tc.name {
			t.Fatalf("decoded %q, want %q", got, tc.name)
		}
		// The length prefixes are what keep ("ab","c") and ("a","bc")
		// apart. Without them both would encode to the same bytes and
		// one tenant's cursor would validate for the other's.
		if prior, dup := seen[token]; dup {
			t.Fatalf("(%q, %q) collides with %s", tc.tenant, tc.name, prior)
		}
		seen[token] = tc.tenant + "/" + tc.name
	}
	if got, err := decodeToken("", "acme"); got != "" || err != nil {
		t.Fatalf("decodeToken(\"\") = %q, %v; an absent token starts at the beginning", got, err)
	}
}

// Every malformed token is refused, because a token that decodes to a position
// nobody issued is a listing that silently starts somewhere else.
func TestMalformedTokensAreRefused(t *testing.T) {
	t.Parallel()
	valid := encodeToken("acme", "docs-bot")
	raw, err := base64.RawURLEncoding.DecodeString(valid)
	if err != nil {
		t.Fatalf("decoding the token this package just produced: %v", err)
	}
	enc := base64.RawURLEncoding.EncodeToString

	cases := map[string]string{
		"not base64":         "not base64!!",
		"unreadable version": enc([]byte{0x80}),
		"version only":       enc([]byte{tokenVersion}),
		"wrong version":      enc(append(binary.AppendUvarint(nil, tokenVersion+1), raw[1:]...)),
		"truncated tenant":   enc(raw[:2]),
		"truncated name":     enc(raw[:len(raw)-2]),
		"trailing bytes":     enc(append(append([]byte{}, raw...), 0)),
		"no position":        enc(appendField(appendField(binary.AppendUvarint(nil, tokenVersion), "acme"), "")),
		"another tenant":     encodeToken("globex", "docs-bot"),
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := decodeToken(token, "acme"); !errors.Is(err, ErrInvalid) {
				t.Fatalf("decodeToken(%q) = %v, want ErrInvalid", token, err)
			}
		})
	}
}
