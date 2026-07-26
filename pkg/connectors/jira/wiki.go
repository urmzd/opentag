package jira

import "strings"

// wiki is Jira's wiki markup, the surface's native alphabet. It resembles
// Markdown just enough to be dangerous: bold is a single asterisk, a link is a
// pipe-separated pair inside brackets, and a brace opens a macro.
type wiki struct{}

func (wiki) Bold(s string) string   { return "*" + s + "*" }
func (wiki) Italic(s string) string { return "_" + s + "_" }

// Code renders an inline monospace span. A "}}" inside would close it early and
// leave the rest of the line as markup, so the closing pair is broken up rather
// than the span being escaped away entirely.
func (wiki) Code(s string) string {
	if strings.Contains(s, "}}") {
		s = strings.ReplaceAll(s, "}}", "} }")
	}
	return "{{" + s + "}}"
}

// Link renders [text|uri]. Both "|" and "]" inside the label would end it early.
func (wiki) Link(text, uri string) string {
	if text == "" || text == uri {
		return "[" + uri + "]"
	}
	text = strings.NewReplacer("|", "-", "]", ")", "[", "(").Replace(text)
	return "[" + text + "|" + uri + "]"
}

func (wiki) Item(s string) string { return "* " + s }

// escape neutralizes the wiki-markup characters that would swallow the rest of a
// line.
//
// It runs over prose that came from a model or a tool result — never over the
// markup this package composes — which is what sink.MapText exists to separate.
// Two characters matter and the rest deliberately do not:
//
//   - "{" opens a macro. An answer containing "{code}" or "{panel}" would turn
//     everything after it into a rendered block, and an unclosed one takes the
//     rest of the comment with it.
//   - "[" opens a link, and "]" closes one. An answer containing "[1]" — which
//     is what a citation footnote looks like, so this is not hypothetical —
//     renders as a broken link to a page named "1".
//
// "*" and "_" are left alone on purpose. The model writes Markdown, so its text
// is full of them, and escaping every one would fill a ticket with backslashes
// to prevent a purely cosmetic mis-render. The trade is deliberate: structural
// damage is escaped, emphasis is not.
func escape(s string) string {
	if !strings.ContainsAny(s, `\{[]`) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s) + 8)
	for _, r := range s {
		switch r {
		case '\\', '{', '[', ']':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}
