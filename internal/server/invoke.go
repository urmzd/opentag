package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"connectrpc.com/connect"
	"github.com/urmzd/dispatch/pkg/metrics"

	opentagv1 "github.com/urmzd/opentag/gen/opentag/v1"
	"github.com/urmzd/opentag/pkg/bus"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/topic"
)

// Accepted is what the runtime returns once a tag is durable. It is returned
// before any inference has happened, because the surfaces that raise tags have
// short timeouts and a run outliving its HTTP request is the normal case.
//
// Rev is the revision the run pinned at accept time, and it can never move: a
// replayed run re-executes from the top, so a run whose prompt or tool grant
// changed underneath it would diverge from its own ledger. The edge returns it
// so a caller can see which definition answered.
type Accepted struct {
	// RunID identifies the durable run. On a redelivered tag it is the run the
	// original tag created, which is what makes Invoke safe to retry.
	RunID string
	// Rev is the pinned agent revision.
	Rev int
	// Topic is where the run publishes. Zero is filled in from the agent and
	// run id, so a runtime that has no opinion does not have to build it.
	Topic topic.Topic
	// At is when the run became durable. Zero is stamped by the edge.
	At time.Time
}

// Invoker accepts tags. It is the whole of what the edge needs from the
// runtime, which is why it is declared here rather than imported: the transport
// can be built, tested and reviewed against a struct literal, and the runtime
// can be built against nothing at all.
//
// Invoke must be idempotent on Tag.ID. Every surface that raises tags
// redelivers on timeout, so without idempotency a slow first response makes the
// agent answer twice.
//
// The tag arrives with Tenant already stamped from the caller's credential. A
// runtime must trust that field and must never accept a tag from anywhere the
// tenant was not derived this way.
type Invoker interface {
	Invoke(ctx context.Context, tag envelope.Tag) (Accepted, error)
}

// Status is where a run is in its lifecycle. It is a string rather than an enum
// so a runtime built independently maps its own states onto it without sharing
// an integer space; an unrecognised value reaches the wire as UNSPECIFIED,
// which is exactly what an older client should see.
type Status string

// Run statuses. They mirror the lifecycle.* event kinds: status answers "where
// is it now", the events answer "how did it get there".
const (
	StatusAccepted  Status = "accepted"
	StatusRunning   Status = "running"
	StatusParked    Status = "parked"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

// Run is the durable record of one invocation, as the edge renders it. It is a
// projection of the ledger, so reading one never touches a worker.
type Run struct {
	ID      string
	Tenant  string
	Agent   string
	Rev     int
	Origin  string
	Topic   topic.Topic
	Status  Status
	Tag     envelope.Tag
	LastSeq uint64
	Error   string

	CreatedAt time.Time
	StartedAt time.Time
	EndedAt   time.Time
}

// RunReader reads durable run records. It is separate from Invoker because
// reading a run and starting one are different privileges, and a deployment may
// wire only one of them.
type RunReader interface {
	// Run returns the record, scoped to tenant. A run belonging to another
	// tenant must be reported as ErrNotFound rather than as a permission
	// error: saying "exists, but not yours" confirms existence across the
	// boundary.
	Run(ctx context.Context, tenant, runID string) (Run, error)
}

// invokeService serves opentag.v1.InvokeService: the write path.
type invokeService struct {
	invoker Invoker
	runs    RunReader
	bus     bus.Bus
	srv     *Server
	log     *slog.Logger
}

// Invoke accepts a tag and returns as soon as the run is durable.
func (s *invokeService) Invoke(ctx context.Context, req *connect.Request[opentagv1.Tag]) (*connect.Response[opentagv1.InvokeResponse], error) {
	id, err := s.identity(ctx)
	if err != nil {
		return nil, err
	}
	if s.invoker == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("invoke: %w: no runtime is configured", ErrUnimplemented))
	}
	tag, err := s.tag(req.Msg, id)
	if err != nil {
		return nil, fail(s.log, "invoke", err)
	}
	accepted, err := s.accept(ctx, tag)
	if err != nil {
		return nil, fail(s.log, "invoke", err)
	}
	return connect.NewResponse(&opentagv1.InvokeResponse{
		RunId:      accepted.RunID,
		Rev:        int32Of(accepted.Rev),
		Topic:      accepted.Topic.String(),
		AcceptedAt: timeToProto(accepted.At),
	}), nil
}

// InvokeStream accepts a tag and streams the run's events until it reaches a
// terminal lifecycle event, then ends the stream.
//
// The subscription is opened on the run's own topic immediately after the tag is
// accepted, from sequence zero, so the events emitted between accept and
// subscribe arrive from the bus's retained window rather than being missed. That
// window is what makes this safe, and it is also why a redelivered tag tails the
// existing run from wherever the window starts rather than from event one.
func (s *invokeService) InvokeStream(ctx context.Context, req *connect.Request[opentagv1.Tag], stream *connect.ServerStream[opentagv1.Event]) error {
	id, err := s.identity(ctx)
	if err != nil {
		return err
	}
	if s.invoker == nil || s.bus == nil {
		return connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("invoke stream: %w: it needs both a runtime and a bus", ErrUnimplemented))
	}
	tag, err := s.tag(req.Msg, id)
	if err != nil {
		return fail(s.log, "invoke stream", err)
	}
	accepted, err := s.accept(ctx, tag)
	if err != nil {
		return fail(s.log, "invoke stream", err)
	}
	sub := envelope.Subscription{Topic: accepted.Topic, Filter: envelope.Filter{RunID: accepted.RunID}}
	if err := s.srv.streamEvents(ctx, s.bus, sub, id.Tenant, "invoke_stream", stream.Send, terminalKind); err != nil {
		return fail(s.log, "invoke stream", err)
	}
	return nil
}

// GetRun reads a run's durable record.
func (s *invokeService) GetRun(ctx context.Context, req *connect.Request[opentagv1.GetRunRequest]) (*connect.Response[opentagv1.Run], error) {
	id, err := s.identity(ctx)
	if err != nil {
		return nil, err
	}
	if s.runs == nil {
		return nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("get run: %w: no run reader is configured", ErrUnimplemented))
	}
	if req.Msg.GetRunId() == "" {
		return nil, fail(s.log, "get run", fmt.Errorf("%w: no run id", ErrInvalid))
	}
	run, err := s.runs.Run(ctx, id.Tenant, req.Msg.GetRunId())
	if err != nil {
		return nil, fail(s.log, "get run", err)
	}
	// Defence in depth: the tenant was passed in, but a reader that ignored it
	// must not be able to leak through this handler. A record from another
	// tenant is reported as absent, never as forbidden.
	if run.Tenant != "" && run.Tenant != id.Tenant {
		return nil, fail(s.log, "get run", fmt.Errorf("%w: run %q", ErrNotFound, req.Msg.GetRunId()))
	}
	run.Tenant = id.Tenant
	return connect.NewResponse(runToProto(run)), nil
}

// tag parses a wire tag and stamps what the server owns: the tenant, from the
// credential, and the observation time when the trigger did not set one.
//
// The tenant is assigned rather than merged. opentag.v1.Tag has no tenant field,
// so there is nothing to merge on the protobuf path, but a hand-rolled JSON body
// can still carry one — Connect's JSON codec discards unknown fields — and an
// assignment makes the outcome the same either way.
func (s *invokeService) tag(p *opentagv1.Tag, id Identity) (envelope.Tag, error) {
	tag, err := tagFromProto(p)
	if err != nil {
		return envelope.Tag{}, err
	}
	tag.Tenant = id.Tenant
	if tag.At.IsZero() {
		tag.At = s.srv.now()
	}
	if err := tag.Validate(); err != nil {
		return envelope.Tag{}, err
	}
	return tag, nil
}

func (s *invokeService) accept(ctx context.Context, tag envelope.Tag) (Accepted, error) {
	return s.srv.accept(ctx, s.invoker, tag)
}

// accept hands a tag to the runtime and completes the acknowledgement with what
// the runtime left to the edge: the run's topic and the accept time.
//
// It lives on Server because two entry points raise tags — the Invoke RPCs and
// webhook ingress — and an acknowledgement that differed between them would make
// the topic a caller can rely on depend on how the tag arrived.
func (s *Server) accept(ctx context.Context, invoker Invoker, tag envelope.Tag) (Accepted, error) {
	accepted, err := invoker.Invoke(ctx, tag)
	result := "accepted"
	if err != nil {
		result = "rejected"
	}
	s.metrics.Count(MetricInvocations, 1,
		metrics.Label{Key: "origin", Value: tag.Origin},
		metrics.Label{Key: "result", Value: result},
	)
	if err != nil {
		return Accepted{}, err
	}
	if accepted.RunID == "" {
		return Accepted{}, fmt.Errorf("runtime accepted tag %q without a run id", tag.ID)
	}
	if accepted.Topic.IsZero() {
		t, err := topic.Run(tag.Agent, accepted.RunID)
		if err != nil {
			return Accepted{}, fmt.Errorf("run topic: %w", err)
		}
		accepted.Topic = t
	}
	if accepted.At.IsZero() {
		accepted.At = s.now()
	}
	return accepted, nil
}

func (s *invokeService) identity(ctx context.Context) (Identity, error) {
	id, err := identityOf(ctx)
	if err != nil {
		return Identity{}, connect.NewError(connect.CodeUnauthenticated, err)
	}
	return id, nil
}
