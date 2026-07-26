package agentspec

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"slices"
	"strings"
)

// HashAlgorithm names the digest Hash uses. It is carried in the hash string
// itself, "sha256:<hex>", so that changing the algorithm later is visible in
// stored data rather than silently reinterpreting every hash written before the
// change as if it had been produced by the new one.
const HashAlgorithm = "sha256"

// hashDomain separates this encoding from every other thing that might be
// hashed with the same primitive. Two schemes that share a digest and no domain
// separator can be made to collide across schemes; a definition and, say, a
// run's input must never be able to produce the same digest, whatever a caller
// puts in a free-text field.
const hashDomain = "opentag/agentspec/v1"

// Hash is the content address of the definition: the digest of its canonical
// form, and of nothing else.
//
// It is the identity a revise compares against ("is this an edit, or the same
// definition resubmitted") and the evidence a durable run replays against ("is
// revision 7 still what revision 7 was when this run pinned it"). Both uses
// require the same two properties, so both are built in here rather than left
// to a caller to remember:
//
//   - Presentation-independent. The spec is canonicalized first (see
//     Normalize), so case, surrounding whitespace, the order of a tool list,
//     and the iteration order of an options map cannot change the answer. Map
//     iteration order in Go is deliberately randomized, so a hash that
//     digested a map in range order would differ between two computations in
//     the same process, and a store built on it would append a new revision
//     every time the same definition was submitted.
//   - Content-sensitive. Any difference in any semantic field changes the
//     digest. Authorship and timestamps are not semantic content and are not
//     digested: they differ on every submission, and including them would make
//     every resubmission look like an edit.
//
// # Why the encoding is length-prefixed
//
// The fields are written in a fixed order into one digest, so the encoding must
// be injective: two different definitions must not produce the same byte
// stream. Plain concatenation is not injective. With
//
//	Name: "ab", Model: "c"     ->  "ab" + "c"  = "abc"
//	Name: "a",  Model: "bc"    ->  "a" + "bc"  = "abc"
//
// two genuinely different agents hash identically, and a store using the hash
// to decide "same definition" would treat an edit as a no-op and refuse to
// record it. A delimiter byte only moves the problem into the fields, since a
// field may contain the delimiter — a system prompt can contain anything.
// Writing each string as its byte length followed by its bytes has no such
// escape hatch: the reader of the stream (were there one) always knows where a
// field ends without inspecting its content, which is exactly the property that
// makes the encoding unambiguous. Collections are written the same way, count
// first, so that appending a tool cannot be confused with lengthening the
// previous one.
func (s AgentSpec) Hash() string {
	c := s.Normalize()
	h := sha256.New()
	writeString(h, hashDomain)
	writeString(h, c.Name)
	writeString(h, c.Description)
	writeString(h, c.Model)
	writeString(h, c.Provider)
	writeString(h, c.SystemPrompt)
	writeStrings(h, c.Tools)
	writeCount(h, len(c.Sources))
	for _, src := range c.Sources {
		writeString(h, src.Name)
		writeString(h, src.URI)
		writeMap(h, src.Options)
	}
	writeStrings(h, c.Access.Spawn)
	writeStrings(h, c.Access.WorkspaceAreas)
	return HashAlgorithm + ":" + hex.EncodeToString(h.Sum(nil))
}

// writeString writes one length-prefixed field. hash.Hash documents that Write
// never returns an error, which is why none is handled here.
func writeString(h hash.Hash, s string) {
	writeCount(h, len(s))
	h.Write([]byte(s))
}

// writeCount writes a fixed-width count. Fixed width rather than decimal
// because a decimal length would itself need a delimiter, and the delimiter
// problem is the one this encoding exists to avoid.
func writeCount(h hash.Hash, n int) {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	h.Write(buf[:])
}

// writeStrings writes a set-valued field: its cardinality, then its members.
// Normalize has already sorted them, so the order here is the canonical one.
func writeStrings(h hash.Hash, values []string) {
	writeCount(h, len(values))
	for _, v := range values {
		writeString(h, v)
	}
}

// writeMap writes a map in sorted key order, which is the only order a Go map
// has that does not change between iterations.
func writeMap(h hash.Hash, m map[string]string) {
	writeCount(h, len(m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		writeString(h, k)
		writeString(h, m[k])
	}
}

// SameContent reports whether two definitions are the same one, canonical form
// for canonical form. It exists so that callers comparing definitions do so on
// the content address rather than inventing a field-by-field comparison that
// will drift the first time a field is added.
func (s AgentSpec) SameContent(other AgentSpec) bool {
	return s.Hash() == other.Hash()
}

// IsHash reports whether v looks like a hash this package produced. It is a
// shape check, not a verification: it tells a decoder that a stored value is a
// content address rather than a legacy identifier, and nothing more.
func IsHash(v string) bool {
	rest, ok := strings.CutPrefix(v, HashAlgorithm+":")
	if !ok || len(rest) != sha256.Size*2 {
		return false
	}
	_, err := hex.DecodeString(rest)
	return err == nil
}
