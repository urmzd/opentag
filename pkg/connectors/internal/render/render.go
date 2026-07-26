// Package render accumulates one run's event stream into a document a sink can
// draw, repeatedly, without ever drawing something stale.
//
// It exists because at-least-once delivery and unordered delivery are the same
// problem wearing two hats, and because both are solved once here rather than
// three times in three connectors:
//
//   - A redelivered event must not double a line. Every event is filed under
//     its Event.Seq, and a Seq already filed is dropped, so applying an event
//     twice leaves the document identical.
//   - A late event must not append stale text after fresh text. Text fragments
//     are held in Seq order, not arrival order, so a delta that overtakes its
//     predecessor still renders in the position it was produced in.
//
// The consequence for sinks is the important part: a sink never applies a diff
// to a surface. It re-renders the whole document and replaces the message body.
// Idempotency then holds by construction — the same set of events always
// produces the same bytes — instead of resting on the sink's bookkeeping being
// right about what it already drew.
//
// # Bounded memory, and the reorder window that follows from it
//
// A run can emit an unbounded number of events, so a document cannot remember
// every sequence number it has seen. It remembers the newest Window of them
// above a low-water mark and folds everything older into that mark. Sequence
// numbers at or below the mark are treated as already applied: a redelivery is
// still dropped (correct), and an event arriving more than Window events late
// is also dropped (a lost fragment, in exchange for a fixed memory bound).
//
// The window is counted in events rather than in sequence numbers because a
// route with a Kinds selector delivers a sparse subsequence — a sink receiving
// only lifecycle events may see Seq 1, 40, 900 — and a scheme keyed on gaps in
// the numbering would either grow without bound or fold immediately.
//
// # Payload tolerance
//
// Event.Payload is bytes and the bus never decodes it, so the runtime and these
// connectors have to agree by convention rather than by type. The decoders here
// accept the documented JSON shape and degrade to treating the payload as plain
// text when it is not JSON, because a text delta that renders as its own bytes
// is right far more often than one that renders as nothing. See payload.go for
// the exact shapes.
//
// This package depends only on the envelope, the address, and the standard
// library. It knows nothing about any surface's markup: that is the sink's job,
// and it reads a View.
package render

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/opentag/pkg/envelope"
)

// DefaultWindow is how many sequence numbers above the mark a document
// remembers, and therefore how far out of order an event may arrive and still
// land in its correct position. It is generous relative to any real reordering
// (a bus stream is ordered per run; reordering comes from a resubscribe after
// ErrSlowConsumer) and small enough to be irrelevant to memory.
const DefaultWindow = 64

// Status is where a run stands, as far as the events a sink received say. It is
// derived from lifecycle kinds rather than read from a field, because a sink
// selecting only "delta" never learns the run's status at all and must render
// something honest in that case (StatusUnknown).
type Status string

// Run statuses, in the order a healthy run passes through them.
const (
	StatusUnknown  Status = ""
	StatusAccepted Status = "accepted"
	StatusRunning  Status = "running"
	StatusParked   Status = "parked"
	StatusDone     Status = "completed"
	StatusFailed   Status = "failed"
)

// Terminal reports whether no further events are expected. A sink uses it to
// decide that this is the last edit it will make, which is when it must flush
// regardless of any coalescing interval.
func (s Status) Terminal() bool { return s == StatusDone || s == StatusFailed }

// Tool is one tool call the agent made, joined from its call and done events.
type Tool struct {
	ID     string
	Name   string
	Args   string
	Result string
	Error  string
	// Done reports whether the matching delta.tool.done arrived.
	Done bool
	// seq of the earliest event mentioning this call, so ordering is by when
	// the agent called the tool rather than by which event arrived first.
	seq uint64
}

// Citation is one source the agent cited.
type Citation struct {
	// N is the footnote number, assigned in sequence order so that the same
	// events always number the same way.
	N int
	// Label is what the agent's own text refers to, when it supplied one.
	// Renderers prefer it over N.
	Label   string
	Title   string
	URI     string
	Snippet string
	seq     uint64
}

// Ref returns the footnote marker for this citation: the agent's own label when
// it gave one, otherwise the assigned number.
func (c Citation) Ref() string {
	if c.Label != "" {
		return c.Label
	}
	return strconv.Itoa(c.N)
}

// Action is one thing the agent changed on a real surface.
type Action struct {
	Summary string
	// Address is the address URI of what changed, in string form: it is
	// rendered as a link and never parsed back.
	Address string
	seq     uint64
}

// View is an immutable snapshot of a document. Renderers take a View, so a
// renderer cannot mutate the document it is drawing and two renderers can draw
// the same snapshot for two different surfaces.
type View struct {
	Agent  string
	RunID  string
	Rev    int
	Origin string
	Tenant string

	Status Status
	// Error is the failure message from lifecycle.failed, empty otherwise.
	Error string

	// Text is the agent's answer so far, assembled in sequence order.
	Text string
	// Thinking is the agent's extended reasoning, if the route asked for it.
	Thinking string

	Tools     []Tool
	Citations []Citation
	Actions   []Action

	// Seq is the highest sequence number applied, which is the cursor a sink
	// can report back for resume.
	Seq uint64
	// At is the timestamp of the most recent event applied.
	At time.Time
}

// Terminal reports whether the run has finished.
func (v View) Terminal() bool { return v.Status.Terminal() }

// Empty reports whether there is nothing to show yet: no text, no tool calls,
// no actions, no failure. A sink still renders an empty view (as a placeholder
// saying the agent is working) so a human sees an acknowledgement immediately.
func (v View) Empty() bool {
	return v.Text == "" && v.Thinking == "" && len(v.Tools) == 0 && len(v.Actions) == 0 && v.Error == ""
}

// stream distinguishes the ordered text channels a document assembles.
type stream uint8

const (
	streamText stream = iota
	streamThinking
)

// piece is one ordered fragment waiting for its neighbours.
type piece struct {
	seq uint64
	str stream
	txt string
}

// Doc is a run's accumulated document. It is not safe for concurrent use: the
// sink engine owns one per (run, target) and serializes access to it.
type Doc struct {
	// Window overrides DefaultWindow. Zero means the default.
	Window int

	agent, runID, origin, tenant string
	rev                          int

	status    Status
	statusSeq uint64
	errText   string

	text     strings.Builder
	thinking strings.Builder

	// pending holds fragments whose sequence number is above mark, sorted by
	// sequence number.
	pending []piece
	// mark is the low-water mark: every sequence number at or below it is
	// considered applied, whether it arrived or was folded away.
	mark uint64
	// seen holds the sequence numbers applied above mark.
	seen map[uint64]struct{}

	tools  []Tool
	toolAt map[string]int

	cites  []Citation
	citeAt map[string]int

	actions []Action

	seq uint64
	at  time.Time
}

// New returns an empty document.
func New() *Doc { return &Doc{} }

// Apply files e into the document and reports whether the document changed.
//
// It returns false for an event already applied and for a kind the document
// does not render, which is what lets a sink skip a network round trip it does
// not need — the single most common case in a live stream is a redelivery after
// a resubscribe.
func (d *Doc) Apply(e envelope.Event) bool {
	if e.Seq != 0 {
		if e.Seq <= d.mark {
			return false
		}
		if _, dup := d.seen[e.Seq]; dup {
			return false
		}
	}

	if d.runID == "" {
		d.runID, d.agent, d.origin, d.tenant, d.rev = e.RunID, e.Agent, e.Origin, e.Tenant, e.Rev
	}

	changed := d.effect(e)

	if e.Seq != 0 {
		if d.seen == nil {
			d.seen = make(map[uint64]struct{})
		}
		d.seen[e.Seq] = struct{}{}
		d.compact()
	}
	if e.Seq > d.seq {
		d.seq = e.Seq
	}
	if e.At.After(d.at) {
		d.at = e.At
	}
	return changed
}

// effect applies the kind-specific change and reports whether anything moved.
func (d *Doc) effect(e envelope.Event) bool {
	switch e.Kind {
	case envelope.KindText:
		return d.addText(streamText, e.Seq, decodeText(e.Payload))
	case envelope.KindThinking:
		return d.addText(streamThinking, e.Seq, decodeText(e.Payload))
	case envelope.KindToolCall:
		return d.addToolCall(e)
	case envelope.KindToolDone:
		return d.addToolDone(e)
	case envelope.KindCitation:
		return d.addCitations(e)
	case envelope.KindActionTaken:
		return d.addAction(e)
	case envelope.KindAccepted:
		return d.setStatus(e.Seq, StatusAccepted, "")
	case envelope.KindStarted, envelope.KindResumed:
		return d.setStatus(e.Seq, StatusRunning, "")
	case envelope.KindParked:
		return d.setStatus(e.Seq, StatusParked, "")
	case envelope.KindCompleted:
		return d.setStatus(e.Seq, StatusDone, "")
	case envelope.KindFailed:
		return d.setStatus(e.Seq, StatusFailed, decodeError(e.Payload))
	default:
		// An unknown kind is not an error: kinds are an open set precisely so
		// that a connector released today can be sent an event kind invented
		// tomorrow. It contributes nothing to render, and saying so avoids an
		// edit that would change no bytes.
		return false
	}
}

func (d *Doc) addText(str stream, seq uint64, txt string) bool {
	if txt == "" {
		return false
	}
	if seq == 0 {
		// Without a sequence number there is no position to file the fragment
		// under and no way to detect its redelivery, so it is appended as it
		// arrives. The bus assigns sequence numbers, so this is only reachable
		// for a document fed by hand.
		d.builder(str).WriteString(txt)
		return true
	}
	p := piece{seq: seq, str: str, txt: txt}
	i := sort.Search(len(d.pending), func(i int) bool { return d.pending[i].seq >= seq })
	d.pending = append(d.pending, piece{})
	copy(d.pending[i+1:], d.pending[i:])
	d.pending[i] = p
	return true
}

func (d *Doc) builder(str stream) *strings.Builder {
	if str == streamThinking {
		return &d.thinking
	}
	return &d.text
}

func (d *Doc) addToolCall(e envelope.Event) bool {
	p := decodeTool(e.Payload)
	if p.Name == "" && p.ID == "" {
		return false
	}
	t := d.tool(p.ID, e.Seq)
	if p.Name != "" {
		d.tools[t].Name = p.Name
	}
	if p.Args != "" {
		d.tools[t].Args = p.Args
	}
	return true
}

func (d *Doc) addToolDone(e envelope.Event) bool {
	p := decodeTool(e.Payload)
	t := d.tool(p.ID, e.Seq)
	if p.Name != "" && d.tools[t].Name == "" {
		d.tools[t].Name = p.Name
	}
	d.tools[t].Result = p.Result
	d.tools[t].Error = p.Error
	d.tools[t].Done = true
	return true
}

// tool returns the index of the call with this id, creating it if needed. An id
// is the join key between a call and its completion; without one, each event
// gets its own entry, which renders as two lines rather than as one wrong line.
func (d *Doc) tool(id string, seq uint64) int {
	if id != "" {
		if i, ok := d.toolAt[id]; ok {
			if seq != 0 && seq < d.tools[i].seq {
				d.tools[i].seq = seq
			}
			return i
		}
	}
	d.tools = append(d.tools, Tool{ID: id, seq: seq})
	i := len(d.tools) - 1
	if id != "" {
		if d.toolAt == nil {
			d.toolAt = make(map[string]int)
		}
		d.toolAt[id] = i
	}
	return i
}

func (d *Doc) addCitations(e envelope.Event) bool {
	cites := decodeCitations(e.Payload)
	changed := false
	for _, c := range cites {
		if c.URI == "" && c.Title == "" {
			continue
		}
		key := c.URI + "\x00" + c.Title
		if i, ok := d.citeAt[key]; ok {
			// The same source cited twice is one footnote. Keep the earliest
			// sequence number so numbering does not shuffle on redelivery.
			if e.Seq != 0 && e.Seq < d.cites[i].seq {
				d.cites[i].seq = e.Seq
				changed = true
			}
			continue
		}
		c.seq = e.Seq
		d.cites = append(d.cites, c)
		if d.citeAt == nil {
			d.citeAt = make(map[string]int)
		}
		d.citeAt[key] = len(d.cites) - 1
		changed = true
	}
	return changed
}

func (d *Doc) addAction(e envelope.Event) bool {
	a := decodeAction(e.Payload)
	if a.Summary == "" && a.Address == "" {
		return false
	}
	a.seq = e.Seq
	d.actions = append(d.actions, a)
	return true
}

// setStatus moves the run's status forward.
//
// The guard is on the sequence number, not on a status ordering: the bus is the
// authority on what happened last, and an out-of-order lifecycle.started must
// not undo a lifecycle.completed that arrived first.
func (d *Doc) setStatus(seq uint64, s Status, errText string) bool {
	if seq != 0 && seq < d.statusSeq {
		return false
	}
	if d.status == s && errText == d.errText {
		return false
	}
	d.status, d.statusSeq = s, seq
	if s == StatusFailed {
		d.errText = errText
	}
	return true
}

// compact advances the mark and folds settled fragments into their builders.
//
// Two rules, in order. The contiguous rule is the fast path: while the next
// sequence number is present, nothing can arrive before it any more, so it
// settles. The window rule is the bound: once more sequence numbers are held
// above the mark than the window allows, the oldest are declared settled even
// though the numbers between them never arrived — which is the case for every
// route with a Kinds selector, since it is only ever sent a subsequence.
func (d *Doc) compact() {
	for {
		if _, ok := d.seen[d.mark+1]; !ok {
			break
		}
		delete(d.seen, d.mark+1)
		d.mark++
	}

	if window := d.window(); len(d.seen) > window {
		seqs := make([]uint64, 0, len(d.seen))
		for s := range d.seen {
			seqs = append(seqs, s)
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
		mark := seqs[len(seqs)-window-1]
		for _, s := range seqs {
			if s > mark {
				break
			}
			delete(d.seen, s)
		}
		d.mark = mark
	}

	settled := 0
	for settled < len(d.pending) && d.pending[settled].seq <= d.mark {
		p := d.pending[settled]
		d.builder(p.str).WriteString(p.txt)
		settled++
	}
	if settled > 0 {
		d.pending = append(d.pending[:0], d.pending[settled:]...)
	}
}

func (d *Doc) window() int {
	if d.Window > 0 {
		return d.Window
	}
	return DefaultWindow
}

// View returns a snapshot of the document.
func (d *Doc) View() View {
	v := View{
		Agent:  d.agent,
		RunID:  d.runID,
		Rev:    d.rev,
		Origin: d.origin,
		Tenant: d.tenant,
		Status: d.status,
		Error:  d.errText,
		Seq:    d.seq,
		At:     d.at,
	}

	v.Text = assemble(&d.text, d.pending, streamText)
	v.Thinking = assemble(&d.thinking, d.pending, streamThinking)

	if len(d.tools) > 0 {
		v.Tools = append([]Tool(nil), d.tools...)
		sort.SliceStable(v.Tools, func(i, j int) bool { return v.Tools[i].seq < v.Tools[j].seq })
	}
	if len(d.cites) > 0 {
		v.Citations = append([]Citation(nil), d.cites...)
		sort.SliceStable(v.Citations, func(i, j int) bool { return v.Citations[i].seq < v.Citations[j].seq })
		for i := range v.Citations {
			v.Citations[i].N = i + 1
		}
	}
	if len(d.actions) > 0 {
		v.Actions = append([]Action(nil), d.actions...)
		sort.SliceStable(v.Actions, func(i, j int) bool { return v.Actions[i].seq < v.Actions[j].seq })
	}
	return v
}

// assemble joins the settled prefix with the fragments still in the window.
func assemble(settled *strings.Builder, pending []piece, str stream) string {
	var b strings.Builder
	b.WriteString(settled.String())
	for _, p := range pending {
		if p.str == str {
			b.WriteString(p.txt)
		}
	}
	return b.String()
}
