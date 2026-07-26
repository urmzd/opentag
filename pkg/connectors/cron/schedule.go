package cron

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/connectors/mention"
	"github.com/urmzd/opentag/pkg/envelope"
)

// ErrSchedule reports a schedule that cannot be run. Callers match with
// errors.Is.
var ErrSchedule = errors.New("cron: invalid schedule")

// Schedule is one standing request: run this agent, with this text, on this
// expression, delivering to these routes.
//
// It is the whole of what a schedule is. There is no per-schedule state here —
// no "last run", no "next run" — because a schedule is a declaration and the
// position in it belongs to whoever is ticking, which is the connector.
type Schedule struct {
	// Name identifies the schedule and appears in two places that outlive this
	// process: the source address and the occurrence id. It is held to the
	// address workspace charset (letters, digits, "-", "_") so that both stay
	// readable and unescaped.
	Name string

	// Expression is a standard 5-field cron expression. See Parse.
	Expression string

	// Agent is the agent to run.
	Agent string

	// Text is the request the agent receives. A schedule has no human typing a
	// message, so this is the message: "summarize yesterday's incidents".
	Text string

	// Tenant is the authorization scope the schedule was created under, and it
	// becomes both Tag.Tenant and the workspace of the source address.
	//
	// A schedule is the one trigger where carrying a tenant is not "trusting the
	// wire": nothing arrives from outside. The schedule is a stored object,
	// written by a caller whose credential was already checked, and this field is
	// that credential's scope recorded at creation time.
	Tenant string

	// Routes are where this schedule's events go. Empty is meaningful and
	// correct: a schedule has no surface to answer on, so a schedule with no
	// routes still runs, still publishes every event to the bus, and simply has
	// nowhere to be rendered. Anyone subscribed to the agent's topic sees it.
	Routes []envelope.Route

	// Meta is extra context for the agent's tools, merged under the cron_* keys
	// this connector sets.
	Meta map[string]string
}

// Validate reports whether the schedule can be run.
func (s Schedule) Validate() error {
	switch {
	case s.Name == "":
		return fmt.Errorf("%w: schedule has no name", ErrSchedule)
	case s.Agent == "":
		return fmt.Errorf("%w: schedule %q names no agent", ErrSchedule, s.Name)
	case s.Tenant == "":
		return fmt.Errorf("%w: schedule %q has no tenant, so it has no scope and no source address", ErrSchedule, s.Name)
	}
	if err := address.ValidWorkspace(s.Name); err != nil {
		return fmt.Errorf("%w: schedule name %q is not usable in an address or an occurrence id: %w", ErrSchedule, s.Name, err)
	}
	if err := address.ValidWorkspace(s.Tenant); err != nil {
		return fmt.Errorf("%w: schedule %q has a tenant that cannot be an address workspace: %w", ErrSchedule, s.Name, err)
	}
	if !mention.Valid(s.Agent) {
		return fmt.Errorf("%w: schedule %q names agent %q, which is not a usable agent name", ErrSchedule, s.Name, s.Agent)
	}
	if _, err := Parse(s.Expression); err != nil {
		return fmt.Errorf("%w: schedule %q: %w", ErrSchedule, s.Name, err)
	}
	for i, r := range s.Routes {
		if err := r.Validate(); err != nil {
			return fmt.Errorf("%w: schedule %q route %d: %w", ErrSchedule, s.Name, i, err)
		}
	}
	return nil
}

// Source is the address a schedule's tags come from: cron://<tenant>/<name>.
//
// It is not a deliverable address — this connector implements no Sink, and
// Registry.Sink refuses it — which is the point. Tag.Routes falls back to
// Tag.Source only when the source is set, so a cron tag with no routes reaching
// a source that cannot receive would be a delivery loop with no destination. The
// address exists to say WHERE the tag came from, for audit and for the agent's
// own context, and pkg/router never resolves it because a cron tag with no
// routes produces no routes at all.
func (s Schedule) Source() address.Address {
	return address.Address{Connector: Name, Workspace: s.Tenant, Path: []string{s.Name}}
}

// OccurrenceID is the idempotency key for one firing of one schedule.
//
// This is the entire correctness story of the cron trigger, so it is worth being
// exact about what it buys. The id is derived from the SCHEDULED instant, never
// from the instant we noticed it was due, and the scheduled instant is a
// property of the expression rather than of this process. Two consequences
// follow, and they are the two failures a scheduler has:
//
//   - A process restart cannot double-fire. If 03:00 already produced
//     "nightly@1769913600" and a restarted process were to emit 03:00 again, it
//     would emit the same id, which is the run's idempotency key: the redelivery
//     joins the run that exists instead of starting a second one.
//   - Two processes running the same schedule cannot double-fire either, for the
//     same reason. Leader election makes that efficient; it is not what makes it
//     correct.
//
// Unix seconds rather than RFC 3339 because the id travels as a run key and a
// log field: it is short, it never contains a separator, and it is the same
// string in every time zone.
func OccurrenceID(schedule string, occurrence time.Time) string {
	return schedule + "@" + strconv.FormatInt(occurrence.Unix(), 10)
}

// tag renders one occurrence of this schedule as a Tag.
func (s Schedule) tag(occurrence time.Time) envelope.Tag {
	meta := map[string]string{
		"cron_schedule":   s.Name,
		"cron_expression": s.Expression,
		// The scheduled instant, not the observed one. An agent asked to
		// "summarize yesterday" needs to know which day it was supposed to run
		// for, which is not the same as when it actually got the message.
		"cron_occurrence": occurrence.UTC().Format(time.RFC3339),
	}
	for k, v := range s.Meta {
		if _, taken := meta[k]; !taken {
			meta[k] = v
		}
	}
	return envelope.Tag{
		ID:     OccurrenceID(s.Name, occurrence),
		Tenant: s.Tenant,
		Agent:  s.Agent,
		Origin: Name,
		Source: s.Source(),
		Text:   s.Text,
		// The actor is the schedule itself, marked as automation so a sink
		// rendering "who asked" does not invent a human.
		Actor:   envelope.Actor{ID: Name + ":" + s.Name, Display: "schedule " + s.Name, Bot: true},
		Deliver: s.Routes,
		Meta:    meta,
		At:      occurrence,
	}
}

// Store holds the schedules a deployment runs.
//
// It is an interface with one method because a deployment's schedules live
// wherever its control plane keeps them — a table, a config file, an agent spec
// — and the ticker only ever needs to ask what they are right now. Reading them
// on every tick rather than caching is deliberate: a schedule added, edited or
// removed takes effect on the next tick with no reload path to get wrong.
type Store interface {
	Schedules(ctx context.Context) ([]Schedule, error)
}

// Memory is a complete in-process schedule store, safe for concurrent use.
//
// It is not a test double: a single-node deployment that keeps its schedules in
// its own configuration can run on it forever. What it does not do is survive a
// restart, which costs nothing here — the connector does not backfill missed
// occurrences anyway, so the state worth persisting is the schedule list itself,
// and that belongs to whatever wrote it.
type Memory struct {
	mu     sync.Mutex
	byName map[string]Schedule
}

var _ Store = (*Memory)(nil)

// NewMemory returns a store holding the given schedules. Every schedule is
// validated, so a typo in an expression is a startup error rather than a
// schedule that silently never fires.
func NewMemory(schedules ...Schedule) (*Memory, error) {
	m := &Memory{byName: make(map[string]Schedule, len(schedules))}
	for _, s := range schedules {
		if err := m.Add(s); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// Add stores a schedule, replacing any schedule of the same name.
func (m *Memory) Add(s Schedule) error {
	if err := s.Validate(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.byName == nil {
		m.byName = make(map[string]Schedule)
	}
	m.byName[s.Name] = s
	return nil
}

// Remove deletes a schedule and reports whether it was there.
func (m *Memory) Remove(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.byName[name]
	delete(m.byName, name)
	return ok
}

// Schedules implements Store, returning the schedules in name order so that a
// tick's emissions are deterministic.
func (m *Memory) Schedules(context.Context) ([]Schedule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Schedule, 0, len(m.byName))
	for _, s := range m.byName {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
