package server

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/urmzd/mandatum/pkg/bus"
	"github.com/urmzd/mandatum/pkg/envelope"
)

// sseReader reads one frame at a time from a live SSE response. Frames are
// separated by a blank line, and one bufio.Reader is kept for the whole response
// so nothing buffered is lost between reads.
type sseReader struct {
	t  *testing.T
	br *bufio.Reader
}

func newSSEReader(t *testing.T, r io.Reader) *sseReader {
	return &sseReader{t: t, br: bufio.NewReader(r)}
}

// frame returns the next frame verbatim, including its terminating blank line.
func (s *sseReader) frame() string {
	s.t.Helper()
	var b strings.Builder
	for {
		line, err := s.br.ReadString('\n')
		if err != nil {
			s.t.Fatalf("read frame: %v (partial frame %q)", err, b.String())
		}
		b.WriteString(line)
		if line == "\n" {
			return b.String()
		}
	}
}

// event returns the next frame that is not a comment, so a heartbeat cannot make
// an assertion about an event flaky.
func (s *sseReader) event() string {
	s.t.Helper()
	for {
		frame := s.frame()
		if !strings.HasPrefix(frame, ":") {
			return frame
		}
	}
}

// readFrames returns the next n frames.
func readFrames(t *testing.T, r io.Reader, n int) []string {
	t.Helper()
	reader := newSSEReader(t, r)
	out := make([]string, 0, n)
	for range n {
		out = append(out, reader.frame())
	}
	return out
}

// The wire format is the contract with every browser that will ever connect, so
// it is asserted byte for byte: id first because it is the resume cursor, then
// the kind as the event type, then one data line, then the blank line that
// dispatches the event.
func TestSSEFramingIsByteCorrect(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.publish(event(tenantAcme, "run_1", 1, envelope.KindText, `{"text":"hi"}`))

	res, err := h.get(ctx, "/v1/sse?topic=agent:docs-bot:run_1", tokenAcme)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()

	if got := res.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Errorf("content type: got %q", got)
	}
	if got := res.Header.Get("Cache-Control"); got != "no-cache, no-transform" {
		t.Errorf("cache control: got %q", got)
	}

	reader := newSSEReader(t, res.Body)
	if got, want := reader.frame(), "retry: 3000\n\n"; got != want {
		t.Errorf("first frame: got %q, want %q", got, want)
	}

	want := "id: 1\n" +
		"event: delta.text\n" +
		`data: {"seq":1,"topic":"agent:docs-bot:run_1","run_id":"run_1","tenant":"acme",` +
		`"agent":"docs-bot","rev":7,"origin":"slack","kind":"delta.text","payload":{"text":"hi"},` +
		`"at":"2026-01-02T03:04:05Z"}` + "\n" +
		"\n"
	if got := reader.event(); got != want {
		t.Errorf("event frame:\n got %q\nwant %q", got, want)
	}
}

// A payload that is JSON is inlined so a browser reads it directly; one that is
// not is base64-encoded under a different key, so a consumer never has to guess
// which it received.
func TestSSEPayloadEncodingIsUnambiguous(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.publish(event(tenantAcme, "run_1", 1, envelope.KindToolDone, "\x00\x01binary"))

	res, err := h.get(ctx, "/v1/sse?topic=agent:docs-bot:run_1", tokenAcme)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()

	reader := newSSEReader(t, res.Body)
	reader.frame() // retry
	frame := reader.event()
	if !strings.Contains(frame, `"payload_b64":"`) {
		t.Errorf("want a base64 payload for non-JSON bytes, got %q", frame)
	}
	if strings.Contains(frame, `"payload":`) {
		t.Errorf("want no inline payload for non-JSON bytes, got %q", frame)
	}
}

// Resume is the browser's, automatic and unavoidable, so Last-Event-ID wins over
// the from parameter: the URL a page opened with is replayed unchanged on every
// reconnect, and honouring it would redeliver everything since the page loaded.
func TestSSEResumesFromLastEventID(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for seq := uint64(1); seq <= 3; seq++ {
		h.publish(event(tenantAcme, "run_1", seq, envelope.KindText, `{"text":"x"}`))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.http.URL+"/v1/sse?topic=agent:docs-bot:run_1&from=0", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set(HeaderAuthorization, "Bearer "+tokenAcme)
	req.Header.Set("Last-Event-ID", "2")
	res, err := h.http.Client().Do(req)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()

	reader := newSSEReader(t, res.Body)
	reader.frame() // retry
	if got := reader.event(); !strings.HasPrefix(got, "id: 3\n") {
		t.Fatalf("want the stream to resume after seq 2, got frame %q", got)
	}
}

// An idle stream is kept alive by comments, because an agent that is thinking
// emits nothing and a proxy in between counts silence against the connection.
func TestSSEHeartbeatsWhileIdle(t *testing.T) {
	h := newHarness(t, func(cfg *Config) { cfg.Heartbeat = 20 * time.Millisecond })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res, err := h.get(ctx, "/v1/sse?topic=agent:docs-bot", tokenAcme)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()

	frames := readFrames(t, res.Body, 3)
	for _, frame := range frames[1:] {
		if frame != sseHeartbeat+"\n\n" {
			t.Fatalf("want a heartbeat comment, got %q", frame)
		}
	}
}

// A client that goes away releases everything the handler held: the bus
// subscription and the goroutine that was reading it.
func TestSSEEndsOnClientDisconnectWithoutLeakingAGoroutine(t *testing.T) {
	h := newHarness(t, nil)

	// Keep-alives off so the transport's own goroutines end with the request and
	// the count is about this package, not about net/http's connection pool.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	baseline := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	h.publish(event(tenantAcme, "run_1", 1, envelope.KindText, `{"text":"hi"}`))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.http.URL+"/v1/sse?topic=agent:docs-bot", nil)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set(HeaderAuthorization, "Bearer "+tokenAcme)
	res, err := client.Do(req)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	reader := newSSEReader(t, res.Body)
	reader.frame() // retry
	reader.event() // the retained event, so the stream is definitely serving
	if got := h.bus.openStreams(); got != 1 {
		t.Fatalf("want one open bus stream while streaming, got %d", got)
	}

	cancel()
	res.Body.Close()

	eventually(t, "the bus subscription to be released", func() bool { return h.bus.openStreams() == 0 })
	eventually(t, "the reader goroutine to exit", func() bool {
		runtime.Gosched()
		return runtime.NumGoroutine() <= baseline
	})
}

// Draining ends an open stream promptly, and says so, rather than leaving the
// browser to guess at a dropped connection.
func TestDrainingEndsAnSSEStreamAndSaysSo(t *testing.T) {
	h := newHarness(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res, err := h.get(ctx, "/v1/sse?topic=agent:docs-bot", tokenAcme)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()

	reader := newSSEReader(t, res.Body)
	reader.frame() // retry, so the handler is certainly inside its loop

	shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := h.server.Shutdown(shutdown); err != nil {
		t.Fatalf("shutdown: %v", err)
	}

	if got := reader.frame(); got != ": draining\n\n" {
		t.Fatalf("want a draining comment, got %q", got)
	}
	if _, err := io.ReadAll(res.Body); err != nil {
		t.Fatalf("want the stream to end cleanly, got %v", err)
	}
	if got := h.bus.openStreams(); got != 0 {
		t.Fatalf("draining left %d bus streams open", got)
	}
}

// A subscription the server cannot serve is refused before the stream opens, so
// the client gets a status code rather than an empty stream.
func TestSSERejectsARequestItCannotServe(t *testing.T) {
	tests := []struct {
		name   string
		path   string
		token  string
		header map[string]string
		want   int
	}{
		{name: "no topic", path: "/v1/sse", token: tokenAcme, want: http.StatusBadRequest},
		{name: "topic outside the namespace", path: "/v1/sse?topic=run:docs-bot", token: tokenAcme, want: http.StatusBadRequest},
		{name: "topic too deep", path: "/v1/sse?topic=agent:docs-bot:run_1:extra", token: tokenAcme, want: http.StatusBadRequest},
		{name: "from is not a number", path: "/v1/sse?topic=agent&from=soon", token: tokenAcme, want: http.StatusBadRequest},
		{name: "rev is not a number", path: "/v1/sse?topic=agent&rev=latest", token: tokenAcme, want: http.StatusBadRequest},
		{
			name:   "last event id is not a number",
			path:   "/v1/sse?topic=agent",
			token:  tokenAcme,
			header: map[string]string{"Last-Event-ID": "abc"},
			want:   http.StatusBadRequest,
		},
		{name: "no credential", path: "/v1/sse?topic=agent", want: http.StatusUnauthorized},
		{name: "unknown credential", path: "/v1/sse?topic=agent", token: "nope", want: http.StatusUnauthorized},
	}
	h := newHarness(t, nil)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, h.http.URL+tt.path, nil)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if tt.token != "" {
				req.Header.Set(HeaderAuthorization, "Bearer "+tt.token)
			}
			for k, v := range tt.header {
				req.Header.Set(k, v)
			}
			res, err := h.http.Client().Do(req)
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			defer res.Body.Close()
			if res.StatusCode != tt.want {
				t.Fatalf("status: got %d, want %d", res.StatusCode, tt.want)
			}
			if got := h.bus.openStreams(); got != 0 {
				t.Fatalf("a refused request left %d bus streams open", got)
			}
		})
	}
}

// Kinds are accepted as repeated parameters, as a comma-separated list, or as a
// mixture, because a hand-written URL and a generated one disagree about which
// is natural.
func TestSSEQueryParsesKindsAndFilterFields(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  envelope.Filter
		from  uint64
	}{
		{
			name:  "comma separated",
			query: "topic=agent&kinds=delta,lifecycle.completed",
			want:  envelope.Filter{Kinds: []string{"delta", "lifecycle.completed"}},
		},
		{
			name:  "repeated",
			query: "topic=agent&kinds=delta&kinds=lifecycle.completed",
			want:  envelope.Filter{Kinds: []string{"delta", "lifecycle.completed"}},
		},
		{
			name:  "mixed with spaces",
			query: "topic=agent&kinds=delta,%20lifecycle.completed&kinds=action.taken",
			want:  envelope.Filter{Kinds: []string{"delta", "lifecycle.completed", "action.taken"}},
		},
		{
			name:  "every filter field",
			query: "topic=agent:docs-bot&rev=7&origin=slack&run_id=run_1&from=4",
			want:  envelope.Filter{Rev: 7, Origin: "slack", RunID: "run_1"},
			from:  4,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(http.MethodGet, "http://x/v1/sse?"+tt.query, nil)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			sub, err := subscriptionFromQuery(req)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if sub.From != tt.from {
				t.Errorf("from: got %d, want %d", sub.From, tt.from)
			}
			if strings.Join(sub.Filter.Kinds, ",") != strings.Join(tt.want.Kinds, ",") {
				t.Errorf("kinds: got %v, want %v", sub.Filter.Kinds, tt.want.Kinds)
			}
			if sub.Filter.Rev != tt.want.Rev || sub.Filter.Origin != tt.want.Origin || sub.Filter.RunID != tt.want.RunID {
				t.Errorf("filter: got %+v, want %+v", sub.Filter, tt.want)
			}
		})
	}
}

// A consumer that fell behind is told so, in a comment, and the stream ends: the
// recovery is a reconnect, which carries Last-Event-ID and resumes exactly where
// the consumer left off.
func TestSSETellsASlowConsumerToReconnect(t *testing.T) {
	h := newHarness(t, func(cfg *Config) {
		cfg.Bus = &failingBus{fakeBus: newFakeBus(), err: bus.ErrSlowConsumer}
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res, err := h.get(ctx, "/v1/sse?topic=agent:docs-bot", tokenAcme)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()

	reader := newSSEReader(t, res.Body)
	reader.frame() // retry
	if got := reader.frame(); got != ": slow consumer, reconnect to resume\n\n" {
		t.Fatalf("want a slow-consumer comment, got %q", got)
	}
	if _, err := io.ReadAll(res.Body); err != nil {
		t.Fatalf("want the stream to end cleanly, got %v", err)
	}
}

// A broker that cannot serve a subscription at all is reported as a failure of
// ours, not as an empty stream the client would read as "nothing happened".
func TestSSEReportsABrokerThatCannotSubscribe(t *testing.T) {
	broken := newFakeBus()
	broken.subscribeErr = errInjected
	h := newHarness(t, func(cfg *Config) { cfg.Bus = broken })

	res, err := h.get(context.Background(), "/v1/sse?topic=agent:docs-bot", tokenAcme)
	if err != nil {
		t.Fatalf("sse: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status: got %d, want 500", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	if strings.Contains(string(body), errInjected.Error()) {
		t.Fatalf("the failure leaked its detail: %q", body)
	}
}
