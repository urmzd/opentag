package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/dispatch/pkg/metrics"
	"github.com/urmzd/opentag/pkg/bus"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/topic"
)

// Server-sent events are the browser-native read path. A dashboard opens one
// with EventSource and needs no client library, no protobuf and no framing code
// of its own, which is the entire reason this endpoint exists alongside
// BusService.Subscribe.
//
// Two properties of SSE shape everything below. Reconnection is the client's,
// automatic and out of our control, so every frame carries the cursor the client
// will resume from (id:) and the server honours Last-Event-ID without being
// asked. And an EventSource cannot set request headers, so the credential has to
// arrive in something a browser sends anyway — which is why Authenticator reads
// the whole header set and a cookie-based deployment works here unchanged.

// SSE framing constants.
const (
	// sseHeartbeat is the comment sent to an idle stream. A comment is a line
	// beginning with ":" and is discarded by every conforming client, so it
	// keeps proxies and load balancers from timing out a stream that is silent
	// while an agent thinks, and it costs the client nothing.
	sseHeartbeat = ": heartbeat"

	// sseWriteTimeout bounds one frame's write. Without it a client that
	// stopped reading but did not close its socket would hold the handler in
	// Write forever, which would in turn hold graceful shutdown open forever.
	sseWriteTimeout = 10 * time.Second
)

// handleSSE serves GET /v1/sse.
//
//	/v1/sse?topic=agent:docs-bot&from=12&kinds=delta,lifecycle.completed&rev=7&origin=slack&run_id=run_1
//
// Every parameter but topic is optional, and each maps onto exactly one field of
// envelope.Subscription: this endpoint is a transport for a subscription, not a
// second query language.
func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	if s.bus == nil {
		s.httpFail(w, "sse", fmt.Errorf("%w: no bus is configured", ErrUnimplemented))
		return
	}
	id, err := s.auth.Authenticate(r.Context(), r.Header)
	if err != nil {
		w.Header().Set("WWW-Authenticate", `Bearer realm="opentag"`)
		s.httpFail(w, "sse", err)
		return
	}
	sub, err := subscriptionFromQuery(r)
	if err != nil {
		s.httpFail(w, "sse", err)
		return
	}

	ctx, release := s.streamContext(r.Context())
	defer release()

	stream, err := s.bus.Subscribe(ctx, sub)
	if err != nil {
		s.httpFail(w, "sse", err)
		return
	}
	defer func() { _ = stream.Close() }()

	sw, err := newSSEWriter(w)
	if err != nil {
		s.httpFail(w, "sse", err)
		return
	}
	closed := s.streamOpened("sse")
	defer closed()

	// Advertise the reconnection delay before anything else, so a client that
	// loses the connection on the next byte already knows how to come back.
	if err := sw.retry(s.retry); err != nil {
		return
	}

	events, failures := recvPump(ctx, stream)
	heartbeat := time.NewTicker(s.heartbeat)
	defer heartbeat.Stop()

	for {
		select {
		case <-ctx.Done():
			// Draining is worth telling the client about: it will reconnect on
			// its own, and the comment makes the reason visible in a browser's
			// network log instead of looking like a dropped connection.
			if s.draining.Load() {
				_ = sw.comment(": draining")
			}
			return

		case <-heartbeat.C:
			if err := sw.comment(sseHeartbeat); err != nil {
				return
			}

		case err := <-failures:
			// A slow consumer is the one failure the client can act on, and the
			// action is exactly what it would do anyway: reconnect, which
			// carries Last-Event-ID and resumes from the last event it saw.
			if errors.Is(err, bus.ErrSlowConsumer) {
				_ = sw.comment(": slow consumer, reconnect to resume")
			}
			return

		case e, ok := <-events:
			if !ok {
				return
			}
			// The bus addresses; it does not authorize. This is the check that
			// keeps one tenant's stream out of another's browser.
			if e.Tenant != id.Tenant {
				continue
			}
			if err := sw.event(e); err != nil {
				return
			}
			s.metrics.Count(MetricEvents, 1,
				metrics.Label{Key: "transport", Value: "sse"},
				metrics.Label{Key: "kind", Value: string(e.Kind)},
			)
		}
	}
}

// recvPump reads a stream into a channel so the caller can select on events, a
// heartbeat and cancellation at once.
//
// The goroutine cannot outlive the stream: Recv returns as soon as ctx is
// cancelled or the stream is closed, and every send is guarded by ctx so a
// caller that has stopped reading does not leave the pump parked on a channel
// send. The caller closing the stream in its own defer is what guarantees the
// first of those.
func recvPump(ctx context.Context, stream bus.Stream) (<-chan envelope.Event, <-chan error) {
	events := make(chan envelope.Event)
	failures := make(chan error, 1)
	go func() {
		defer close(events)
		for {
			e, err := stream.Recv(ctx)
			if err != nil {
				failures <- err
				return
			}
			select {
			case events <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	return events, failures
}

// subscriptionFromQuery builds a subscription from the URL, with Last-Event-ID
// taking precedence over the from parameter.
//
// Precedence goes that way because Last-Event-ID is the browser's automatic
// resume and the from parameter is the page's initial guess: on a reconnect the
// URL is replayed unchanged, so honouring it over the header would restart the
// stream from wherever the page first started and redeliver everything since.
func subscriptionFromQuery(r *http.Request) (envelope.Subscription, error) {
	q := r.URL.Query()
	raw := q.Get("topic")
	if raw == "" {
		return envelope.Subscription{}, fmt.Errorf("%w: sse needs a topic parameter", ErrInvalid)
	}
	t, err := topic.Parse(raw)
	if err != nil {
		return envelope.Subscription{}, err
	}
	sub := envelope.Subscription{
		Topic: t,
		Filter: envelope.Filter{
			Kinds:  kindsFromQuery(q["kinds"]),
			Origin: q.Get("origin"),
			RunID:  q.Get("run_id"),
		},
	}
	if v := q.Get("rev"); v != "" {
		rev, err := strconv.Atoi(v)
		if err != nil || rev < 0 {
			return envelope.Subscription{}, fmt.Errorf("%w: rev %q is not a revision number", ErrInvalid, v)
		}
		sub.Filter.Rev = rev
	}
	if v := q.Get("from"); v != "" {
		from, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return envelope.Subscription{}, fmt.Errorf("%w: from %q is not a sequence number", ErrInvalid, v)
		}
		sub.From = from
	}
	if v := strings.TrimSpace(r.Header.Get("Last-Event-ID")); v != "" {
		from, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return envelope.Subscription{}, fmt.Errorf("%w: Last-Event-ID %q is not a sequence number", ErrInvalid, v)
		}
		sub.From = from
	}
	return sub, nil
}

// kindsFromQuery accepts kinds as repeated parameters, as one comma-separated
// list, or as any mixture, because a hand-written URL and a generated one
// disagree about which is natural and neither is wrong.
func kindsFromQuery(values []string) []string {
	var out []string
	for _, v := range values {
		for _, kind := range strings.Split(v, ",") {
			if kind = strings.TrimSpace(kind); kind != "" {
				out = append(out, kind)
			}
		}
	}
	return out
}

// sseWriter frames events onto a response. Every method writes one complete
// frame and flushes it: an event held in a buffer is an event the browser has
// not received, which defeats the point of streaming.
type sseWriter struct {
	w  http.ResponseWriter
	rc *http.ResponseController
}

// newSSEWriter writes the SSE response headers and proves the response can be
// flushed. A response writer that cannot flush cannot serve SSE, and finding
// that out before the first event is what lets the failure be an HTTP error
// instead of a stream that silently never arrives.
func newSSEWriter(w http.ResponseWriter) (*sseWriter, error) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	// Nginx buffers proxied responses by default, which would hold events until
	// a buffer filled. This is the documented opt-out.
	h.Set("X-Accel-Buffering", "no")

	sw := &sseWriter{w: w, rc: http.NewResponseController(w)}
	w.WriteHeader(http.StatusOK)
	if err := sw.rc.Flush(); err != nil {
		return nil, fmt.Errorf("sse: response cannot be flushed: %w", err)
	}
	return sw, nil
}

// retry advertises the reconnection delay, in whole milliseconds as the format
// requires.
func (s *sseWriter) retry(d time.Duration) error {
	return s.frame(fmt.Sprintf("retry: %d\n\n", d.Milliseconds()))
}

// comment writes a comment frame. The argument carries its own leading ":" so
// that the exact bytes on the wire are readable at the call site.
func (s *sseWriter) comment(text string) error {
	return s.frame(text + "\n\n")
}

// event writes one event as id, event, data, blank line.
//
// The sequence goes in id: because that is what a client sends back as
// Last-Event-ID, so resume needs no separate cursor. The kind goes in event: so
// a browser can addEventListener("delta.text", ...) rather than parsing every
// payload to discard most of them.
func (s *sseWriter) event(e envelope.Event) error {
	data, err := json.Marshal(sseDataOf(e))
	if err != nil {
		return fmt.Errorf("sse: encode event %d: %w", e.Seq, err)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "id: %d\n", e.Seq)
	if e.Kind != "" {
		fmt.Fprintf(&b, "event: %s\n", e.Kind)
	}
	// A data field is one line, so a payload containing a newline becomes
	// several data fields, which the client rejoins with newlines. JSON
	// encoding escapes newlines inside strings, so this loop runs once in
	// practice; it exists because "in practice" is not a framing guarantee.
	for _, line := range strings.Split(string(data), "\n") {
		b.WriteString("data: ")
		b.WriteString(line)
		b.WriteString("\n")
	}
	b.WriteString("\n")
	return s.frame(b.String())
}

// frame writes and flushes one frame under a write deadline, so a client that
// stopped reading without closing its socket cannot hold the handler open.
func (s *sseWriter) frame(text string) error {
	if err := s.rc.SetWriteDeadline(time.Now().Add(sseWriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
		return fmt.Errorf("sse: set write deadline: %w", err)
	}
	if _, err := s.w.Write([]byte(text)); err != nil {
		return fmt.Errorf("sse: write: %w", err)
	}
	if err := s.rc.Flush(); err != nil {
		return fmt.Errorf("sse: flush: %w", err)
	}
	return nil
}

// sseData is the JSON body of a data field: the event as a browser wants it.
//
// It is a hand-written struct rather than protojson output because the bytes
// matter here. A struct gives one deterministic field order, which keeps the
// framing testable byte for byte, and it lets the payload be inlined.
type sseData struct {
	Seq    uint64 `json:"seq"`
	Topic  string `json:"topic"`
	RunID  string `json:"run_id"`
	Tenant string `json:"tenant"`
	Agent  string `json:"agent"`
	Rev    int    `json:"rev"`
	Origin string `json:"origin"`
	Kind   string `json:"kind"`

	// Payload carries a JSON payload inline, so a browser reads
	// JSON.parse(ev.data).payload.text rather than decoding a base64 string
	// that happens to contain JSON.
	Payload json.RawMessage `json:"payload,omitempty"`
	// PayloadB64 carries a payload that is not JSON. The two fields are
	// distinct rather than one polymorphic field so a consumer never has to
	// guess which encoding it received.
	PayloadB64 []byte `json:"payload_b64,omitempty"`

	At time.Time `json:"at"`
}

func sseDataOf(e envelope.Event) sseData {
	d := sseData{
		Seq:    e.Seq,
		Topic:  e.Topic.String(),
		RunID:  e.RunID,
		Tenant: e.Tenant,
		Agent:  e.Agent,
		Rev:    e.Rev,
		Origin: e.Origin,
		Kind:   string(e.Kind),
		At:     e.At.UTC(),
	}
	switch {
	case len(e.Payload) == 0:
	case json.Valid(e.Payload):
		d.Payload = json.RawMessage(e.Payload)
	default:
		d.PayloadB64 = e.Payload
	}
	return d
}
