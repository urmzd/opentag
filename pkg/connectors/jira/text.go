package jira

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/urmzd/mandatum/pkg/connectors/mention"
)

// This file turns whatever Jira put in a body field into the plain text with
// @name mentions that pkg/connectors/mention reads.
//
// There are two problems, and they are separate:
//
//  1. A body arrives in one of two encodings. Jira's REST v2 and its classic
//     webhooks send wiki markup as a JSON string; Cloud's v3 and the newer
//     webhook payloads send an Atlassian Document Format tree, which is a JSON
//     object. A connector that handles only one of them silently reads an empty
//     comment on half the instances it is pointed at.
//  2. A mention is not an "@name" in either encoding. Wiki markup writes
//     [~accountid:5b10a2...] for Cloud and [~jsmith] for Data Center; ADF writes
//     a mention node carrying the same account id in an attribute. None of those
//     is a name, and pkg/connectors/mention deliberately knows nothing about
//     them: normalizing native markup into the shared @name grammar is each
//     connector's job, so the stripping rules live in one tested place instead
//     of five.
//
// Both roads end in the same place. flatten renders ADF down to wiki-style text,
// including rendering its mention nodes back into [~accountid:...], and normalize
// then rewrites every mention in that text. One rewriting rule, one place.

// text decodes a Jira body field into the plain text an agent should read.
func text(raw json.RawMessage, bots map[string]string) string {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return ""
	}
	switch trimmed[0] {
	case '"':
		var s string
		if err := json.Unmarshal(trimmed, &s); err != nil {
			return ""
		}
		return normalize(s, bots)
	case '{':
		var doc node
		if err := json.Unmarshal(trimmed, &doc); err != nil {
			return ""
		}
		return normalize(strings.TrimSpace(flatten(doc)), bots)
	default:
		return ""
	}
}

// node is one Atlassian Document Format node. ADF is a tree of typed nodes with
// a "content" array; only the handful of types that carry text or identity need
// to be understood, and every other type is walked through.
type node struct {
	Type    string `json:"type"`
	Text    string `json:"text"`
	Content []node `json:"content"`
	Attrs   struct {
		// ID is the account id on a mention node.
		ID string `json:"id"`
		// Text is the display form a mention or emoji node carries, already
		// including its "@" or ":".
		Text string `json:"text"`
		// ShortName is an emoji's ":name:" form.
		ShortName string `json:"shortName"`
		// URL is the target of a card node.
		URL string `json:"url"`
	} `json:"attrs"`
}

// flatten renders an ADF tree as text.
//
// Structure is reduced to blank lines and newlines rather than to markup,
// because the output is about to be read by a model, not rendered. What matters
// is that paragraphs stay separated (so a summary does not run into a
// description) and that mentions survive as something normalize can rewrite.
func flatten(n node) string {
	switch n.Type {
	case "text":
		return n.Text
	case "mention":
		// Rendered back into wiki markup rather than resolved here, so that
		// normalize is the only place a mention is interpreted.
		if n.Attrs.ID != "" {
			return "[~accountid:" + n.Attrs.ID + "]"
		}
		return n.Attrs.Text
	case "emoji":
		if n.Attrs.Text != "" {
			return n.Attrs.Text
		}
		return n.Attrs.ShortName
	case "hardBreak":
		return "\n"
	case "inlineCard", "blockCard", "embedCard":
		return n.Attrs.URL
	}

	parts := make([]string, 0, len(n.Content))
	for _, child := range n.Content {
		if s := flatten(child); s != "" {
			parts = append(parts, s)
		}
	}
	switch n.Type {
	case "doc", "blockquote", "bulletList", "orderedList", "panel", "table":
		// Block containers: their children are separate paragraphs.
		return strings.Join(parts, "\n\n")
	case "listItem", "tableRow":
		return strings.Join(parts, "\n")
	default:
		// Inline containers — paragraph, heading, codeBlock, tableCell, and any
		// node type invented after this was written — concatenate.
		return strings.Join(parts, "")
	}
}

// normalize rewrites Jira's mention markup into the @name grammar.
//
// Three outcomes, in order of how much is known:
//
//   - The account maps to an agent: "[~accountid:5b10a2]" becomes "@docs-bot",
//     which is the whole point — the mention package can then find it and strip
//     it out of the prompt.
//   - It does not, but the token looks like a name: "[~jsmith]" becomes
//     "@jsmith". Data Center mentions humans by username, and that is both a
//     plausible agent name (so Config.Agents gets a chance at it) and readable
//     if it is not.
//   - It is an unmapped account id: left exactly as it arrived. There is nothing
//     to call it, and dropping it would lose the fact that somebody was
//     addressed. An opaque id in the prompt is unfortunate; a silently deleted
//     participant is worse.
func normalize(text string, bots map[string]string) string {
	if !strings.Contains(text, "[~") {
		return text
	}
	var b strings.Builder
	b.Grow(len(text))
	for {
		i := strings.Index(text, "[~")
		if i < 0 {
			b.WriteString(text)
			return b.String()
		}
		j := strings.IndexByte(text[i:], ']')
		if j < 0 {
			b.WriteString(text)
			return b.String()
		}
		j += i
		b.WriteString(text[:i])
		b.WriteString(rewrite(text[i+2:j], bots))
		text = text[j+1:]
	}
}

// rewrite turns the inside of one "[~...]" into its replacement.
func rewrite(token string, bots map[string]string) string {
	id := strings.TrimPrefix(token, "accountid:")
	if agent, ok := bots[strings.ToLower(id)]; ok {
		return "@" + agent
	}
	if id != token {
		// An account id we do not recognise. Put it back as it came.
		return "[~" + token + "]"
	}
	if mention.Valid(token) {
		return "@" + token
	}
	return "[~" + token + "]"
}
