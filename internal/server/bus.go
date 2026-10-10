package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"connectrpc.com/connect"
	"github.com/urmzd/legatus/pkg/metrics"

	mandatumv1 "github.com/urmzd/mandatum/gen/mandatum/v1"
	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// Sequencer is the optional extension a broker implements when it can report
// the sequence it assigned.
//
// bus.Bus.Publish returns only an error, deliberately: the runtime supplies its
// own sequences because duraturo replays a workflow from the top and a replayed
// event must land on the number it landed on the first time. That leaves a wire
// publisher with no way to learn its position, which PublishResponse.seq is
// supposed to report, so a broker that can answer says so by implementing this.
// When the broker cannot, PublishResponse.seq is zero and means "assigned, not
// reported".
type Sequencer interface {
	PublishSeq(ctx context.Context, e envelope.Event) (uint64, error)
}

// busService serves mandatum.v1.BusService: the read path, plus the write seam
// that lets something other than the agent loop put events on a run's topic.
type busService struct {
	bus bus.Bus
	srv *Server
	log *slog.Logger
}

// Subscribe streams every event the subscription accepts and the caller's
// tenant owns, until the client goes away or the server drains.
//
// The tenant check happens here rather than in the bus because the bus
// addresses and does not authorize: a Subscription names a topic, and topics
// are not tenant-scoped by design. So this is the layer that must not hand a
// caller another tenant's events, and it does it by comparison rather than by
// trusting the topic to have narrowed anything.
func (s *busService) Subscribe(ctx context.Context, req *connect.Request[mandatumv1.SubscribeRequest], stream *connect.ServerStream[mandatumv1.Event]) error {
	id, b, err := s.ready(ctx)
	if err != nil {
		return err
	}
	sub, err := subscriptionFromProto(req.Msg.GetSubscription())
	if err != nil {
		return fail(s.log, "subscribe", err)
	}
	if err := s.srv.streamEvents(ctx, b, sub, id.Tenant, "subscribe", stream.Send, nil); err != nil {
		return fail(s.log, "subscribe", err)
	}
	return nil
}

// Publish appends one event to a run's topic.
//
// Three fields are the server's and are overwritten whatever the client sent:
// tenant, because it is the caller's credential and never a claim on the wire;
// at, because clocks disagree; and seq, because only the broker can order a
// run. Zeroing seq is also what stops a client from parking a run's sequence
// space at a number no real event can ever exceed.
func (s *busService) Publish(ctx context.Context, req *connect.Request[mandatumv1.PublishRequest]) (*connect.Response[mandatumv1.PublishResponse], error) {
	id, b, err := s.ready(ctx)
	if err != nil {
		return nil, err
	}
	e, err := eventFromProto(req.Msg.GetEvent())
	if err != nil {
		return nil, fail(s.log, "publish", err)
	}
	e.Tenant = id.Tenant
	e.Seq = 0
	e.At = s.srv.now()

	var seq uint64
	if sequencer, ok := b.(Sequencer); ok {
		seq, err = sequencer.PublishSeq(ctx, e)
	} else {
		err = b.Publish(ctx, e)
	}
	if err != nil {
		return nil, fail(s.log, "publish", err)
	}
	s.srv.metrics.Count(MetricEvents, 1,
		metrics.Label{Key: "transport", Value: "publish"},
		metrics.Label{Key: "kind", Value: string(e.Kind)},
	)
	return connect.NewResponse(&mandatumv1.PublishResponse{Seq: seq, At: timeToProto(e.At)}), nil
}

func (s *busService) ready(ctx context.Context) (Identity, bus.Bus, error) {
	if s.bus == nil {
		return Identity{}, nil, connect.NewError(connect.CodeUnimplemented,
			fmt.Errorf("bus service: %w: no bus is configured", ErrUnimplemented))
	}
	id, err := identityOf(ctx)
	if err != nil {
		return Identity{}, nil, connect.NewError(connect.CodeUnauthenticated, err)
	}
	return id, s.bus, nil
}

// streamEvents is the one event-streaming loop in the package: BusService.
// Subscribe and InvokeService.InvokeStream differ only in how they end, so they
// differ only in the terminal predicate.
//
// It owns the whole lifetime of one stream: the context that ends on client
// disconnect or server drain, the bus subscription, the tenant check on every
// event, and the open-stream gauge. Nothing it starts outlives it, which is why
// there is no goroutine here — Recv already blocks on the context this function
// controls.
//
// A terminal predicate that returns true ends the stream cleanly after the
// event has been sent, so a caller that asked for one run's output gets the
// terminal event and then EOF rather than an open connection to a finished run.
func (s *Server) streamEvents(
	ctx context.Context,
	b bus.Bus,
	sub envelope.Subscription,
	tenant string,
	transport string,
	send func(*mandatumv1.Event) error,
	terminal func(envelope.Event) bool,
) error {
	ctx, release := s.streamContext(ctx)
	defer release()

	stream, err := b.Subscribe(ctx, sub)
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()

	closed := s.streamOpened(transport)
	defer closed()

	for {
		e, err := stream.Recv(ctx)
		if err != nil {
			// The client going away and the server draining are both ends of
			// the stream, not failures: the RPC is over either way, and a
			// consumer resumes with the cursor it already has. Everything else
			// — a slow consumer, a broker fault — is reported, because a
			// consumer that must resubscribe has to be told.
			if errors.Is(err, context.Canceled) || errors.Is(err, bus.ErrClosed) {
				return nil
			}
			return err
		}
		if e.Tenant != tenant {
			continue
		}
		if err := send(eventToProto(e)); err != nil {
			return err
		}
		s.metrics.Count(MetricEvents, 1,
			metrics.Label{Key: "transport", Value: transport},
			metrics.Label{Key: "kind", Value: string(e.Kind)},
		)
		if terminal != nil && terminal(e) {
			return nil
		}
	}
}

// terminalKind reports whether an event ends its run. It is the streaming form
// of a terminal RunStatus, and it is the only thing InvokeStream needs to know
// about a run's lifecycle: the run's own record is the ledger's business.
func terminalKind(e envelope.Event) bool {
	return e.Kind == envelope.KindCompleted || e.Kind == envelope.KindFailed
}
