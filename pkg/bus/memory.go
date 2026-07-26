package bus

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/topic"
)

// Memory is a complete single-process bus, not a stub: it is what the
// zero-infrastructure deployment runs on and what every test in this repo runs
// against, and it holds the same contract as the Redis backend down to the
// error values.
//
// One mutex guards everything. Publish takes it to append to the run's ring
// and hand the event to each subscriber's buffer with a non-blocking send, so
// the critical section is bounded by the number of subscribers and never by
// how fast any of them reads. Subscribe takes it to snapshot the retained
// history and join the fan-out in the same step, which is what closes the
// catch-up/live seam: an event is either in the snapshot or in the buffer,
// never both and never neither.
//
// Events are value-semantic. Payloads are copied in on Publish and copied out
// on Recv, so one consumer cannot see another's mutation and neither can reach
// the retained copy.
//
// The snapshot is taken eagerly and is not bounded by the backlog: subscribing
// to a broad topic from the beginning costs one slice of everything currently
// retained beneath it, at most WithRetention events per run in scope. It is the
// backlog that bounds the live tail, and the retention and run bounds that keep
// the snapshot finite.
type Memory struct {
	retention int
	backlog   int
	maxRuns   int

	mu   sync.Mutex
	runs map[string]*memRun
	// lru orders runs by last publish, most recent at the front, so that
	// evicting the coldest run is O(1) rather than a scan.
	lru  *list.List
	subs map[*memStream]struct{}
}

var _ Bus = (*Memory)(nil)

// NewMemory returns an empty in-process bus. It honours WithRetention,
// WithBacklog and WithMaxRuns.
func NewMemory(opts ...Option) *Memory {
	o := newOptions(opts)
	return &Memory{
		retention: o.retention,
		backlog:   o.backlog,
		maxRuns:   o.maxRuns,
		runs:      make(map[string]*memRun),
		lru:       list.New(),
		subs:      make(map[*memStream]struct{}),
	}
}

// memRun is one run's retention ring plus its sequence high-water mark. The
// ring is fixed-length: push overwrites the oldest slot once it is full.
type memRun struct {
	topic   topic.Topic
	buf     []envelope.Event
	head    int    // index of the oldest retained event
	n       int    // events currently retained
	highest uint64 // highest sequence ever published, retained or evicted
	elem    *list.Element
}

func (r *memRun) push(e envelope.Event) {
	r.buf[(r.head+r.n)%len(r.buf)] = e
	if r.n == len(r.buf) {
		r.head = (r.head + 1) % len(r.buf) // the ring wrapped: drop the oldest
		return
	}
	r.n++
}

// retained appends the run's retained events to dst, oldest first.
func (r *memRun) retained(dst []envelope.Event) []envelope.Event {
	for i := range r.n {
		dst = append(dst, r.buf[(r.head+i)%len(r.buf)])
	}
	return dst
}

// Publish implements Bus. A caller-supplied Seq at or below the run's
// high-water mark is a replay and returns nil without publishing anything.
func (m *Memory) Publish(ctx context.Context, e envelope.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := normalize(&e); err != nil {
		return err
	}
	e.Payload = bytes.Clone(e.Payload)

	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.runLocked(e.Topic)
	switch {
	case e.Seq == 0:
		e.Seq = r.highest + 1
	case e.Seq <= r.highest:
		return nil // already published: replay is not a duplicate
	}
	r.highest = e.Seq
	r.push(e)
	for s := range m.subs {
		s.offer(e)
	}
	return nil
}

// runLocked returns the run's ring, creating it and evicting the coldest run
// if that takes the bus over its run bound. Callers hold m.mu.
func (m *Memory) runLocked(t topic.Topic) *memRun {
	key := t.String()
	if r := m.runs[key]; r != nil {
		m.lru.MoveToFront(r.elem)
		return r
	}
	r := &memRun{topic: t, buf: make([]envelope.Event, m.retention)}
	m.runs[key] = r
	r.elem = m.lru.PushFront(r)
	for m.lru.Len() > m.maxRuns {
		cold := m.lru.Remove(m.lru.Back()).(*memRun)
		delete(m.runs, cold.topic.String())
	}
	return r
}

// Subscribe implements Bus.
func (m *Memory) Subscribe(ctx context.Context, sub envelope.Subscription) (Stream, error) {
	if err := validate(sub); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s := &memStream{
		m:      m,
		sub:    sub,
		ch:     make(chan envelope.Event, m.backlog),
		closed: make(chan struct{}),
		hup:    make(chan struct{}),
	}

	m.mu.Lock()
	// Snapshot and join in one critical section. Anything published before
	// this point is in catchup; anything after is offered to ch. There is no
	// window in between, which is the only way the seam holds under
	// concurrent publishing.
	keys := make([]string, 0, len(m.runs))
	for key, r := range m.runs {
		if sub.Topic.Covers(r.topic) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys) // deterministic interleaving across runs
	for _, key := range keys {
		for _, e := range m.runs[key].retained(nil) {
			if sub.Accepts(e) {
				s.catchup = append(s.catchup, e)
			}
		}
	}
	m.subs[s] = struct{}{}
	m.mu.Unlock()

	// One watcher per subscription, and only when the context can actually
	// be cancelled. It exits on cancel or on Close, whichever comes first.
	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = s.Close() // Memory's Close cannot fail
			case <-s.closed:
			}
		}()
	}
	return s, nil
}

// memStream is one subscriber: a snapshot to drain, then a bounded buffer fed
// by publishers.
type memStream struct {
	m   *Memory
	sub envelope.Subscription

	// catchup is owned by Recv alone and needs no lock: it is written once,
	// under m.mu, before the stream is handed to the caller.
	catchup []envelope.Event
	ch      chan envelope.Event

	once   sync.Once
	closed chan struct{}
	// hup is closed when this subscriber overflows. over guards against
	// closing it twice and stops further offers; both are held under m.mu.
	hup  chan struct{}
	over bool
}

var _ Stream = (*memStream)(nil)

// offer hands e to this subscriber without ever blocking. A full buffer
// terminates the subscriber instead of stalling the publisher. Callers hold
// m.mu.
func (s *memStream) offer(e envelope.Event) {
	if s.over || !s.sub.Accepts(e) {
		return
	}
	select {
	case s.ch <- e:
	default:
		s.over = true
		close(s.hup)
	}
}

// Recv implements Stream. It drains the catch-up snapshot, then the live
// buffer, and only reports overflow once the buffer it already holds is empty:
// those events are a contiguous prefix, and handing them over first carries
// the consumer's cursor as far forward as possible before it has to resubscribe.
func (s *memStream) Recv(ctx context.Context) (envelope.Event, error) {
	for {
		if err := ctx.Err(); err != nil {
			return envelope.Event{}, err
		}
		select {
		case <-s.closed:
			return envelope.Event{}, ErrClosed
		default:
		}
		if len(s.catchup) > 0 {
			e := s.catchup[0]
			s.catchup[0] = envelope.Event{} // drop the reference as we go
			s.catchup = s.catchup[1:]
			return copyEvent(e), nil
		}
		select {
		case e := <-s.ch:
			return copyEvent(e), nil
		default:
		}
		select {
		case <-s.hup:
			return envelope.Event{}, fmt.Errorf("bus: subscription to %s fell more than %d events behind: %w", s.sub.Topic, s.m.backlog, ErrSlowConsumer)
		default:
		}
		select {
		case e := <-s.ch:
			return copyEvent(e), nil
		case <-s.hup: // loop: report only after the buffer is drained
		case <-s.closed:
		case <-ctx.Done():
			return envelope.Event{}, ctx.Err()
		}
	}
}

// Close implements Stream. It leaves the fan-out set, so a publisher stops
// seeing this subscriber, and unblocks Recv.
func (s *memStream) Close() error {
	s.once.Do(func() {
		s.m.mu.Lock()
		delete(s.m.subs, s)
		s.m.mu.Unlock()
		close(s.closed)
	})
	return nil
}

// copyEvent detaches an event's payload from the copy the bus retains.
func copyEvent(e envelope.Event) envelope.Event {
	e.Payload = bytes.Clone(e.Payload)
	return e
}
