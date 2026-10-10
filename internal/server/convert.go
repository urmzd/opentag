package server

import (
	"fmt"
	"math"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/address"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

// This file is the only place in the package where a generated type meets a
// domain type. Keeping the translation here, rather than inline in the
// handlers, is what lets the handlers read as policy — authenticate, stamp the
// tenant, forward — and what makes the wire's shape a single thing to review.
//
// The direction of each function is in its name: fromProto parses what a client
// sent and can fail, toProto renders what the server owns and cannot. Nothing
// in the fromProto direction reads a tenant, because no request message has
// one.

// eventToProto renders an event for the wire.
func eventToProto(e envelope.Event) *mandatumv1.Event {
	return &mandatumv1.Event{
		Seq:     e.Seq,
		Topic:   e.Topic.String(),
		RunId:   e.RunID,
		Tenant:  e.Tenant,
		Agent:   e.Agent,
		Rev:     int32Of(e.Rev),
		Origin:  e.Origin,
		Kind:    string(e.Kind),
		Payload: e.Payload,
		At:      timeToProto(e.At),
	}
}

// eventFromProto parses an event a client asked to publish.
//
// Seq, At, and Tenant are deliberately not read. The broker orders a run, the
// server owns the clock, and the tenant comes from the credential; a client
// that sets any of the three is ignored rather than rejected, because rejecting
// would break a client that faithfully echoed an event it received.
func eventFromProto(p *mandatumv1.Event) (envelope.Event, error) {
	if p == nil {
		return envelope.Event{}, fmt.Errorf("%w: no event", ErrInvalid)
	}
	t, err := topic.Parse(p.GetTopic())
	if err != nil {
		return envelope.Event{}, fmt.Errorf("event topic: %w", err)
	}
	return envelope.Event{
		Topic:   t,
		RunID:   p.GetRunId(),
		Agent:   p.GetAgent(),
		Rev:     int(p.GetRev()),
		Origin:  p.GetOrigin(),
		Kind:    envelope.Kind(p.GetKind()),
		Payload: p.GetPayload(),
	}, nil
}

// subscriptionFromProto parses a subscription request.
func subscriptionFromProto(p *mandatumv1.Subscription) (envelope.Subscription, error) {
	if p == nil {
		return envelope.Subscription{}, fmt.Errorf("%w: no subscription", ErrInvalid)
	}
	t, err := topic.Parse(p.GetTopic())
	if err != nil {
		return envelope.Subscription{}, fmt.Errorf("subscription topic: %w", err)
	}
	return envelope.Subscription{
		Topic:  t,
		Filter: filterFromProto(p.GetFilter()),
		From:   p.GetFrom(),
	}, nil
}

// filterFromProto parses a filter. A nil filter is the identity filter, which
// is what an absent message should mean: unset fields are wildcards.
func filterFromProto(p *mandatumv1.Filter) envelope.Filter {
	if p == nil {
		return envelope.Filter{}
	}
	return envelope.Filter{
		Kinds:  p.GetKinds(),
		Rev:    int(p.GetRev()),
		Origin: p.GetOrigin(),
		RunID:  p.GetRunId(),
	}
}

// tagFromProto parses a tag a client raised.
//
// Tenant is absent from the wire and is stamped by the caller of this function
// from the credential. At is defaulted by the handler, not here, so that the
// clock seam stays in one place.
func tagFromProto(p *mandatumv1.Tag) (envelope.Tag, error) {
	if p == nil {
		return envelope.Tag{}, fmt.Errorf("%w: no tag", ErrInvalid)
	}
	t := envelope.Tag{
		ID:     p.GetId(),
		Agent:  p.GetAgent(),
		Origin: p.GetOrigin(),
		Text:   p.GetText(),
		Actor:  actorFromProto(p.GetActor()),
		Meta:   p.GetMeta(),
		At:     timeFromProto(p.GetAt()),
	}
	if src := p.GetSource(); src != nil {
		a, err := addressFromProto(src)
		if err != nil {
			return envelope.Tag{}, fmt.Errorf("tag source: %w", err)
		}
		t.Source = a
	}
	for i, r := range p.GetDeliver() {
		route, err := routeFromProto(r)
		if err != nil {
			return envelope.Tag{}, fmt.Errorf("tag route %d: %w", i, err)
		}
		t.Deliver = append(t.Deliver, route)
	}
	return t, nil
}

// tagToProto renders a tag, so a run's record can carry the request verbatim.
func tagToProto(t envelope.Tag) *mandatumv1.Tag {
	p := &mandatumv1.Tag{
		Id:     t.ID,
		Agent:  t.Agent,
		Origin: t.Origin,
		Text:   t.Text,
		Actor:  &mandatumv1.Actor{Id: t.Actor.ID, Display: t.Actor.Display, Bot: t.Actor.Bot},
		Meta:   t.Meta,
		At:     timeToProto(t.At),
	}
	if !t.Source.IsZero() {
		p.Source = addressToProto(t.Source)
	}
	for _, r := range t.Deliver {
		p.Deliver = append(p.Deliver, &mandatumv1.Route{Target: addressToProto(r.Target), Kinds: r.Kinds})
	}
	return p
}

func actorFromProto(p *mandatumv1.Actor) envelope.Actor {
	if p == nil {
		return envelope.Actor{}
	}
	return envelope.Actor{ID: p.GetId(), Display: p.GetDisplay(), Bot: p.GetBot()}
}

// addressFromProto rebuilds an address and revalidates it. The structured form
// on the wire is convenient, not trusted: it is rendered to its URI and parsed
// back, so a workspace that is not token-safe is refused here exactly as it
// would be if it had arrived as a string.
func addressFromProto(p *mandatumv1.Address) (address.Address, error) {
	if p == nil {
		return address.Address{}, fmt.Errorf("%w: no address", ErrInvalid)
	}
	a := address.Address{
		Connector: p.GetConnector(),
		Workspace: p.GetWorkspace(),
		Path:      p.GetPath(),
		Params:    p.GetParams(),
	}
	if a.Connector == "" {
		return address.Address{}, fmt.Errorf("%w: address has no connector", address.ErrInvalid)
	}
	if err := address.ValidWorkspace(a.Workspace); err != nil {
		return address.Address{}, err
	}
	// Round-tripping catches anything the field-by-field check above does not,
	// and guarantees the address a sink receives is one that could have been
	// written down.
	parsed, err := address.Parse(a.String())
	if err != nil {
		return address.Address{}, err
	}
	return parsed, nil
}

func addressToProto(a address.Address) *mandatumv1.Address {
	return &mandatumv1.Address{
		Connector: a.Connector,
		Workspace: a.Workspace,
		Path:      a.Path,
		Params:    a.Params,
	}
}

func routeFromProto(p *mandatumv1.Route) (envelope.Route, error) {
	if p == nil {
		return envelope.Route{}, fmt.Errorf("%w: no route", ErrInvalid)
	}
	target, err := addressFromProto(p.GetTarget())
	if err != nil {
		return envelope.Route{}, err
	}
	r := envelope.Route{Target: target, Kinds: p.GetKinds()}
	if err := r.Validate(); err != nil {
		return envelope.Route{}, err
	}
	return r, nil
}

// specFromProto parses an agent definition. The server validates only what it
// owns — that the document is present and names an agent — and leaves the rest
// to the Store: whether a tool exists, whether a source resolves, and what the
// content hash is are all the control plane's business, and duplicating any of
// them here would create a second, quietly diverging answer.
func specFromProto(p *mandatumv1.AgentSpec) (Spec, error) {
	if p == nil {
		return Spec{}, fmt.Errorf("%w: no agent spec", ErrInvalid)
	}
	if p.GetName() == "" {
		return Spec{}, fmt.Errorf("%w: agent spec has no name", ErrInvalid)
	}
	s := Spec{
		Name:         p.GetName(),
		Description:  p.GetDescription(),
		Model:        p.GetModel(),
		Provider:     p.GetProvider(),
		SystemPrompt: p.GetSystemPrompt(),
		Tools:        p.GetTools(),
	}
	for _, src := range p.GetSources() {
		s.Sources = append(s.Sources, Source{
			Name:    src.GetName(),
			URI:     src.GetUri(),
			Options: src.GetOptions(),
		})
	}
	if a := p.GetAccess(); a != nil {
		s.Access = Access{Spawn: a.GetSpawn(), WorkspaceAreas: a.GetWorkspaceAreas()}
	}
	return s, nil
}

func specToProto(s Spec) *mandatumv1.AgentSpec {
	p := &mandatumv1.AgentSpec{
		Name:         s.Name,
		Description:  s.Description,
		Model:        s.Model,
		Provider:     s.Provider,
		SystemPrompt: s.SystemPrompt,
		Tools:        s.Tools,
		Access:       &mandatumv1.Access{Spawn: s.Access.Spawn, WorkspaceAreas: s.Access.WorkspaceAreas},
	}
	for _, src := range s.Sources {
		p.Sources = append(p.Sources, &mandatumv1.Source{
			Name:    src.Name,
			Uri:     src.URI,
			Options: src.Options,
		})
	}
	return p
}

func revisionToProto(r Revision) *mandatumv1.Revision {
	return &mandatumv1.Revision{
		Spec:      specToProto(r.Spec),
		Rev:       int32Of(r.Rev),
		Hash:      r.Hash,
		CreatedAt: timeToProto(r.CreatedAt),
		CreatedBy: r.CreatedBy,
	}
}

// runToProto renders a durable run record. The tag is carried verbatim because
// it is the input half of the audit trail: with the pinned revision it is
// everything needed to explain what the agent did.
func runToProto(r Run) *mandatumv1.Run {
	return &mandatumv1.Run{
		RunId:     r.ID,
		Tenant:    r.Tenant,
		Agent:     r.Agent,
		Rev:       int32Of(r.Rev),
		Origin:    r.Origin,
		Topic:     r.Topic.String(),
		Status:    statusToProto(r.Status),
		Tag:       tagToProto(r.Tag),
		LastSeq:   r.LastSeq,
		Error:     r.Error,
		CreatedAt: timeToProto(r.CreatedAt),
		StartedAt: timeToProto(r.StartedAt),
		EndedAt:   timeToProto(r.EndedAt),
	}
}

// statusToProto maps a run status onto the wire enum. An unrecognised status
// becomes UNSPECIFIED rather than a guess: the enum's zero value exists exactly
// so that "I do not know this state" is expressible.
func statusToProto(s Status) mandatumv1.RunStatus {
	switch s {
	case StatusAccepted:
		return mandatumv1.RunStatus_RUN_STATUS_ACCEPTED
	case StatusRunning:
		return mandatumv1.RunStatus_RUN_STATUS_RUNNING
	case StatusParked:
		return mandatumv1.RunStatus_RUN_STATUS_PARKED
	case StatusCompleted:
		return mandatumv1.RunStatus_RUN_STATUS_COMPLETED
	case StatusFailed:
		return mandatumv1.RunStatus_RUN_STATUS_FAILED
	default:
		return mandatumv1.RunStatus_RUN_STATUS_UNSPECIFIED
	}
}

// timeToProto renders a timestamp, mapping the zero time to an absent one.
// Protobuf has no "unset" for a scalar timestamp other than a nil message, and
// a run that has not started must not report having started in 1970.
func timeToProto(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t.UTC())
}

// timeFromProto parses a timestamp, mapping an absent one to the zero time.
func timeFromProto(p *timestamppb.Timestamp) time.Time {
	if p == nil {
		return time.Time{}
	}
	return p.AsTime().UTC()
}

// int32Of narrows a revision for the wire. Revisions are small by construction
// — they increase by one per edit — so this saturates rather than wrapping: a
// number that reads as maximal is obviously wrong, while a negative revision
// looks plausible and would filter as one.
func int32Of(n int) int32 {
	switch {
	case n > math.MaxInt32:
		return math.MaxInt32
	case n < math.MinInt32:
		return math.MinInt32
	default:
		return int32(n)
	}
}
