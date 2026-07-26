package server

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/urmzd/dispatch/pkg/metrics"

	"github.com/urmzd/opentag/gen/opentag/v1/opentagv1connect"
	"github.com/urmzd/opentag/pkg/bus"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/topic"
)

// The fakes here are complete, not stubs: the store enforces the immutability
// and concurrency rules a real one would, and the bus is a real in-memory
// broker with recording around it. A test that passes against a fake which
// accepts everything proves nothing about a handler whose job is to refuse.

// Credentials every test shares. Two tenants, because most of what this package
// does is keep them apart.
const (
	tokenAcme  = "token-acme"
	tokenOther = "token-other"

	tenantAcme  = "acme"
	tenantOther = "other"

	subjectAcme = "user-1"
)

// fixedTime is the clock every harness runs on, so a stamped timestamp is an
// assertion rather than a tolerance.
var fixedTime = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

// --- store ------------------------------------------------------------------

type storeKey struct {
	tenant string
	name   string
}

// fakeStore is a complete in-memory agent control plane: revisions append, a
// spec is never mutated, and the optimistic concurrency check is real.
type fakeStore struct {
	mu     sync.Mutex
	agents map[storeKey][]Revision
	err    error // injected failure, returned by every method
}

func newFakeStore() *fakeStore {
	return &fakeStore{agents: make(map[storeKey][]Revision)}
}

func (s *fakeStore) Create(_ context.Context, tenant string, spec Spec, by string) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Revision{}, s.err
	}
	key := storeKey{tenant, spec.Name}
	if _, exists := s.agents[key]; exists {
		return Revision{}, fmt.Errorf("%w: agent %q", ErrExists, spec.Name)
	}
	rev := Revision{Spec: spec, Rev: 1, Hash: hashOf(spec), CreatedAt: fixedTime, CreatedBy: by}
	s.agents[key] = []Revision{rev}
	return rev, nil
}

func (s *fakeStore) Revise(_ context.Context, tenant string, spec Spec, expectedRev int, by string) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Revision{}, s.err
	}
	key := storeKey{tenant, spec.Name}
	revs, exists := s.agents[key]
	if !exists {
		return Revision{}, fmt.Errorf("%w: agent %q", ErrNotFound, spec.Name)
	}
	if expectedRev != 0 && expectedRev != len(revs) {
		return Revision{}, fmt.Errorf("%w: agent %q is at revision %d, not %d", ErrConflict, spec.Name, len(revs), expectedRev)
	}
	rev := Revision{Spec: spec, Rev: len(revs) + 1, Hash: hashOf(spec), CreatedAt: fixedTime, CreatedBy: by}
	s.agents[key] = append(revs, rev)
	return rev, nil
}

func (s *fakeStore) Get(_ context.Context, tenant, name string, rev int) (Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return Revision{}, s.err
	}
	revs, exists := s.agents[storeKey{tenant, name}]
	if !exists {
		return Revision{}, fmt.Errorf("%w: agent %q", ErrNotFound, name)
	}
	if rev == 0 {
		return revs[len(revs)-1], nil
	}
	if rev < 1 || rev > len(revs) {
		return Revision{}, fmt.Errorf("%w: agent %q revision %d", ErrNotFound, name, rev)
	}
	return revs[rev-1], nil
}

func (s *fakeStore) List(_ context.Context, tenant string, page Page) ([]Revision, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, "", s.err
	}
	var names []string
	for key := range s.agents {
		if key.tenant == tenant && key.name > page.Token {
			names = append(names, key.name)
		}
	}
	sort.Strings(names)
	size := page.Size
	if size <= 0 || size > len(names) {
		size = len(names)
	}
	out := make([]Revision, 0, size)
	for _, name := range names[:size] {
		revs := s.agents[storeKey{tenant, name}]
		out = append(out, revs[len(revs)-1])
	}
	var next string
	if size < len(names) {
		next = names[size-1]
	}
	return out, next, nil
}

func (s *fakeStore) History(_ context.Context, tenant, name string) ([]Revision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	revs, exists := s.agents[storeKey{tenant, name}]
	if !exists {
		return nil, fmt.Errorf("%w: agent %q", ErrNotFound, name)
	}
	return append([]Revision(nil), revs...), nil
}

func (s *fakeStore) Delete(_ context.Context, tenant, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return s.err
	}
	key := storeKey{tenant, name}
	if _, exists := s.agents[key]; !exists {
		return fmt.Errorf("%w: agent %q", ErrNotFound, name)
	}
	delete(s.agents, key)
	return nil
}

func hashOf(spec Spec) string {
	sum := sha256.Sum256([]byte(spec.Name + "\x00" + spec.SystemPrompt))
	return fmt.Sprintf("sha256:%x", sum[:8])
}

// --- bus --------------------------------------------------------------------

// fakeBus is bus.Memory with instrumentation: it counts open streams so a test
// can prove a handler released its subscription, and it can be made to fail.
type fakeBus struct {
	mem *bus.Memory

	mu           sync.Mutex
	subs         []envelope.Subscription
	open         int
	subscribeErr error
	publishErr   error
}

func newFakeBus(opts ...bus.Option) *fakeBus {
	return &fakeBus{mem: bus.NewMemory(opts...)}
}

func (b *fakeBus) Publish(ctx context.Context, e envelope.Event) error {
	b.mu.Lock()
	err := b.publishErr
	b.mu.Unlock()
	if err != nil {
		return err
	}
	return b.mem.Publish(ctx, e)
}

func (b *fakeBus) Subscribe(ctx context.Context, sub envelope.Subscription) (bus.Stream, error) {
	b.mu.Lock()
	err := b.subscribeErr
	if err == nil {
		b.subs = append(b.subs, sub)
		b.open++
	}
	b.mu.Unlock()
	if err != nil {
		return nil, err
	}
	stream, err := b.mem.Subscribe(ctx, sub)
	if err != nil {
		b.closeOne()
		return nil, err
	}
	return &fakeStream{Stream: stream, bus: b}, nil
}

func (b *fakeBus) closeOne() {
	b.mu.Lock()
	b.open--
	b.mu.Unlock()
}

func (b *fakeBus) openStreams() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.open
}

func (b *fakeBus) subscriptions() []envelope.Subscription {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]envelope.Subscription(nil), b.subs...)
}

type fakeStream struct {
	bus.Stream
	bus  *fakeBus
	once sync.Once
}

func (s *fakeStream) Close() error {
	s.once.Do(s.bus.closeOne)
	return s.Stream.Close()
}

// errStream is a subscription that fails on its first Recv, which is how a
// broker reports a consumer that fell behind. The failure path matters as much as
// the happy one: it is the only thing that tells a consumer to resubscribe.
type errStream struct {
	err error
}

func (s errStream) Recv(context.Context) (envelope.Event, error) { return envelope.Event{}, s.err }
func (s errStream) Close() error                                 { return nil }

// failingBus hands out streams that fail, without disturbing the real broker's
// bookkeeping.
type failingBus struct {
	*fakeBus
	err error
}

func (b *failingBus) Subscribe(context.Context, envelope.Subscription) (bus.Stream, error) {
	return errStream{err: b.err}, nil
}

// sequencingBus reports the sequence it assigned, which is the optional
// Sequencer extension PublishResponse.seq needs.
type sequencingBus struct {
	*fakeBus
	mu   sync.Mutex
	next uint64
}

func (b *sequencingBus) PublishSeq(ctx context.Context, e envelope.Event) (uint64, error) {
	b.mu.Lock()
	b.next++
	seq := b.next
	b.mu.Unlock()
	e.Seq = seq
	if err := b.Publish(ctx, e); err != nil {
		return 0, err
	}
	return seq, nil
}

// --- runtime ----------------------------------------------------------------

// fakeInvoker records the tags it accepted and is idempotent on Tag.ID, exactly
// as the contract requires of a real runtime.
type fakeInvoker struct {
	mu    sync.Mutex
	tags  []envelope.Tag
	runs  map[string]string
	rev   int
	err   error
	pinTo topic.Topic // when set, returned instead of letting the edge derive one
}

func newFakeInvoker() *fakeInvoker {
	return &fakeInvoker{runs: make(map[string]string), rev: 7}
}

func (i *fakeInvoker) Invoke(_ context.Context, tag envelope.Tag) (Accepted, error) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.err != nil {
		return Accepted{}, i.err
	}
	i.tags = append(i.tags, tag)
	runID, seen := i.runs[tag.ID]
	if !seen {
		runID = "run_" + tag.ID
		i.runs[tag.ID] = runID
	}
	return Accepted{RunID: runID, Rev: i.rev, Topic: i.pinTo}, nil
}

func (i *fakeInvoker) accepted() []envelope.Tag {
	i.mu.Lock()
	defer i.mu.Unlock()
	return append([]envelope.Tag(nil), i.tags...)
}

// fakeRuns is a run reader that honours the tenant argument, so the handler's
// own defence in depth is tested against a reader that is already correct as
// well as against one that is not.
type fakeRuns struct {
	mu           sync.Mutex
	runs         map[string]Run
	ignoreTenant bool
	err          error
}

func newFakeRuns() *fakeRuns { return &fakeRuns{runs: make(map[string]Run)} }

func (r *fakeRuns) Run(_ context.Context, tenant, runID string) (Run, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return Run{}, r.err
	}
	run, ok := r.runs[runID]
	if !ok {
		return Run{}, fmt.Errorf("%w: run %q", ErrNotFound, runID)
	}
	if !r.ignoreTenant && run.Tenant != tenant {
		return Run{}, fmt.Errorf("%w: run %q", ErrNotFound, runID)
	}
	return run, nil
}

func (r *fakeRuns) put(run Run) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.runs[run.ID] = run
}

// --- harness ----------------------------------------------------------------

// harness is one wired server behind an httptest server, with every fake
// reachable so a test can assert what a handler forwarded as well as what it
// answered.
type harness struct {
	t       *testing.T
	server  *Server
	http    *httptest.Server
	bus     *fakeBus
	store   *fakeStore
	invoker *fakeInvoker
	runs    *fakeRuns
	metrics *metrics.Memory
}

// newHarness builds a fully wired server. mutate adjusts the config before New,
// which is how a test removes a dependency or changes a timing.
func newHarness(t *testing.T, mutate func(*Config)) *harness {
	t.Helper()

	auth, err := NewTokens(map[string]Identity{
		tokenAcme:  {Tenant: tenantAcme, Subject: subjectAcme},
		tokenOther: {Tenant: tenantOther, Subject: "user-2"},
	})
	if err != nil {
		t.Fatalf("NewTokens: %v", err)
	}

	h := &harness{
		t:       t,
		bus:     newFakeBus(),
		store:   newFakeStore(),
		invoker: newFakeInvoker(),
		runs:    newFakeRuns(),
		metrics: metrics.NewMemory(),
	}
	cfg := Config{
		Auth:    auth,
		Store:   h.store,
		Bus:     h.bus,
		Invoker: h.invoker,
		Runs:    h.runs,
		Metrics: h.metrics,
		// Long enough that a heartbeat never interleaves with the frames a test
		// is asserting on; the heartbeat test sets its own.
		Heartbeat: 5 * time.Second,
		Now:       func() time.Time { return fixedTime },
	}
	if mutate != nil {
		mutate(&cfg)
	}
	h.server, err = New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h.http = httptest.NewServer(h.server.Handler())

	// Shutdown first, then Close: httptest waits for in-flight requests, and a
	// streaming handler only returns once the drain has reached it.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := h.server.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		h.http.Close()
	})
	return h
}

// The client constructors take the credential rather than a client, and an empty
// token means "add nothing", so a test can present a malformed credential itself.
func (h *harness) agentClient(token string) opentagv1connect.AgentServiceClient {
	return opentagv1connect.NewAgentServiceClient(h.http.Client(), h.http.URL, credential(token)...)
}

func (h *harness) busClient(token string) opentagv1connect.BusServiceClient {
	return opentagv1connect.NewBusServiceClient(h.http.Client(), h.http.URL, credential(token)...)
}

func (h *harness) invokeClient(token string) opentagv1connect.InvokeServiceClient {
	return opentagv1connect.NewInvokeServiceClient(h.http.Client(), h.http.URL, credential(token)...)
}

func credential(token string) []connect.ClientOption {
	if token == "" {
		return nil
	}
	return []connect.ClientOption{connect.WithInterceptors(bearer(token))}
}

// discardLogger is the logger a directly constructed service gets in a test.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// publish puts an event on the bus the way the runtime would.
func (h *harness) publish(e envelope.Event) {
	h.t.Helper()
	if err := h.bus.Publish(context.Background(), e); err != nil {
		h.t.Fatalf("publish: %v", err)
	}
}

// event builds a well-formed event for a run of docs-bot.
func event(tenant, runID string, seq uint64, kind envelope.Kind, payload string) envelope.Event {
	return envelope.Event{
		Seq:     seq,
		Topic:   topic.MustParse("agent:docs-bot:" + runID),
		RunID:   runID,
		Tenant:  tenant,
		Agent:   "docs-bot",
		Rev:     7,
		Origin:  "slack",
		Kind:    kind,
		Payload: []byte(payload),
		At:      fixedTime,
	}
}

// bearer adds a credential to every request a client makes, unary and streaming
// alike.
type bearer string

func (b bearer) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		req.Header().Set(HeaderAuthorization, "Bearer "+string(b))
		return next(ctx, req)
	}
}

func (b bearer) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		conn.RequestHeader().Set(HeaderAuthorization, "Bearer "+string(b))
		return conn
	}
}

func (b bearer) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return next
}

// get issues a plain HTTP GET with a bearer credential.
func (h *harness) get(ctx context.Context, path, token string) (*http.Response, error) {
	h.t.Helper()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.http.URL+path, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set(HeaderAuthorization, "Bearer "+token)
	}
	return h.http.Client().Do(req)
}

// requireCode asserts an error is a Connect error with the given code.
func requireCode(t *testing.T, err error, want connect.Code) {
	t.Helper()
	if err == nil {
		t.Fatalf("want error with code %v, got nil", want)
	}
	if got := connect.CodeOf(err); got != want {
		t.Fatalf("want code %v, got %v (%v)", want, got, err)
	}
}

// eventually polls until cond holds or the deadline passes. Streams are
// concurrent, and a test that asserts on them either polls or is flaky.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

var errInjected = errors.New("injected failure")
