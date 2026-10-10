package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
)

// Format is how a command renders its result.
//
// There are exactly two, and the split is not cosmetic: text is for a person
// reading a terminal and may change between releases, json is a contract and
// may not. Anything a script needs must be reachable in json, and nothing in
// json may exist only because it looked good in a table.
type Format string

const (
	// FormatText renders for a human, coloured only on a terminal.
	FormatText Format = "text"
	// FormatJSON renders one JSON document per command, or one JSON object per
	// line for the streaming commands, where a single document would never
	// close.
	FormatJSON Format = "json"
)

// ParseFormat resolves the --format flag. It is strict rather than
// forgiving: a typo silently falling back to text would make a script that
// parses stdout fail somewhere far from the mistake.
func ParseFormat(s string) (Format, error) {
	switch Format(strings.ToLower(strings.TrimSpace(s))) {
	case FormatText, "":
		return FormatText, nil
	case FormatJSON:
		return FormatJSON, nil
	default:
		return "", fmt.Errorf("%w: unknown --format %q, want %q or %q", errUsage, s, FormatText, FormatJSON)
	}
}

// ANSI escapes. They are written only when colour is enabled, which is decided
// once at construction rather than at each call site, so a non-TTY run is
// byte-for-byte free of escapes instead of nearly free.
const (
	ansiReset  = "\x1b[0m"
	ansiBold   = "\x1b[1m"
	ansiDim    = "\x1b[2m"
	ansiRed    = "\x1b[31m"
	ansiGreen  = "\x1b[32m"
	ansiYellow = "\x1b[33m"
	ansiBlue   = "\x1b[34m"
	ansiCyan   = "\x1b[36m"
)

// ui is a command's two output channels and its rendering decisions.
//
// The separation is the whole point. Results go to out, and diagnostics —
// progress, warnings, the address a server bound to — go to err, so that
// `mandatum listen agent:docs-bot --format json | jq` sees a clean stream of
// events even while the same process is logging a reconnect. A command that
// prints a diagnostic to out has broken its own contract, which is why nothing
// here offers a single "print" that guesses which one you meant.
type ui struct {
	out    io.Writer
	err    io.Writer
	format Format
	color  bool
}

// newUI decides where output goes and whether it is coloured.
//
// Colour is enabled only when the caller asked for text, stdout is a terminal,
// and NO_COLOR is unset. NO_COLOR wins over the terminal check because it is
// the user's explicit statement about their environment, and any value at all —
// including the empty string — means the same thing under the convention.
func newUI(out, errw io.Writer, format Format) *ui {
	return &ui{out: out, err: errw, format: format, color: format == FormatText && isTerminal(out) && !noColor()}
}

func noColor() bool {
	_, set := os.LookupEnv("NO_COLOR")
	return set
}

// isTerminal reports whether w is a character device. It works through the
// os.File it is given and answers false for anything else, which is the honest
// answer for a pipe, a buffer, or a test's strings.Builder.
func isTerminal(w io.Writer) bool {
	f, ok := w.(*os.File)
	if !ok {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return info.Mode()&os.ModeCharDevice != 0
}

// paint wraps s in an escape when colour is on, and returns it untouched when
// colour is off.
func (u *ui) paint(code, s string) string {
	if !u.color || s == "" {
		return s
	}
	return code + s + ansiReset
}

func (u *ui) bold(s string) string   { return u.paint(ansiBold, s) }
func (u *ui) dim(s string) string    { return u.paint(ansiDim, s) }
func (u *ui) red(s string) string    { return u.paint(ansiRed, s) }
func (u *ui) green(s string) string  { return u.paint(ansiGreen, s) }
func (u *ui) yellow(s string) string { return u.paint(ansiYellow, s) }
func (u *ui) blue(s string) string   { return u.paint(ansiBlue, s) }
func (u *ui) cyan(s string) string   { return u.paint(ansiCyan, s) }

// printf writes a result to stdout.
func (u *ui) printf(format string, args ...any) {
	fmt.Fprintf(u.out, format, args...)
}

// logf writes a diagnostic to stderr.
func (u *ui) logf(format string, args ...any) {
	fmt.Fprintf(u.err, format, args...)
}

// warnf writes a warning to stderr, marked so it is distinguishable from
// ordinary progress at a glance.
func (u *ui) warnf(format string, args ...any) {
	fmt.Fprint(u.err, u.yellow("warning: "))
	fmt.Fprintf(u.err, format, args...)
}

// json writes one indented JSON document to stdout. It is used by the
// commands that produce a single result.
func (u *ui) json(v any) error {
	enc := json.NewEncoder(u.out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("cli: encode json: %w", err)
	}
	return nil
}

// jsonLine writes one compact JSON object followed by a newline, and flushes.
//
// The streaming commands use this rather than json: an event stream has no end,
// so it cannot be one document, and a consumer piping it into jq wants a line
// it can act on now rather than a prefix of an array that will never close.
func (u *ui) jsonLine(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("cli: encode json: %w", err)
	}
	if _, err := u.out.Write(append(b, '\n')); err != nil {
		return fmt.Errorf("cli: write: %w", err)
	}
	u.flush()
	return nil
}

// flush pushes buffered output to the terminal. Streaming commands call it
// after every record, because a delta the user cannot see until the next one
// arrives is not a stream.
func (u *ui) flush() {
	if f, ok := u.out.(interface{ Sync() error }); ok {
		_ = f.Sync()
	}
	if f, ok := u.out.(interface{ Flush() error }); ok {
		_ = f.Flush()
	}
}

// table renders aligned columns for text output. It pads to the widest cell in
// each column and leaves the last column unpadded, so trailing whitespace never
// reaches the terminal.
func (u *ui) table(header []string, rows [][]string) {
	widths := make([]int, len(header))
	for i, h := range header {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}
	write := func(cells []string, paint func(string) string) {
		var b strings.Builder
		for i, cell := range cells {
			if i == len(cells)-1 {
				b.WriteString(paint(cell))
				break
			}
			b.WriteString(paint(cell))
			b.WriteString(strings.Repeat(" ", widths[i]-len(cell)+2))
		}
		b.WriteString("\n")
		fmt.Fprint(u.out, b.String())
	}
	write(header, u.dim)
	for _, row := range rows {
		write(row, func(s string) string { return s })
	}
}
