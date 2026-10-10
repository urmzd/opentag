package bus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/urmzd/mandatum/pkg/envelope"
	"github.com/urmzd/mandatum/pkg/topic"
)

// readBatch bounds how many entries one XRANGE or XREAD asks for. It caps the
// work of a single round trip; the reader loops until the stream is drained.
const readBatch = 128

// fieldEvent is the single stream field each entry carries.
const fieldEvent = "e"

// publishScript appends one event to a run's stream, assigning the sequence
// when the publisher left it 0.
//
// The entry ID *is* the sequence: "<seq>-0". That is what makes the whole
// backend fall out of Redis's own semantics — XADD's monotonic-ID rule becomes
// the run's monotonic-sequence rule, XRANGE "(<seq>-0" becomes catch-up after a
// cursor, and XREAD from "<seq>-0" becomes the live tail from the same cursor,
// so the catch-up/live seam is one continuous scan of one key rather than two
// reads that have to be stitched.
//
// A supplied sequence at or below the stream's top is a replay: duraturo
// re-runs a workflow from the top and re-publishes what the run already
// emitted, and within a run sequences only move forward, so anything at or
// below the top has already been published. Returning it unchanged makes
// replay idempotent even after retention has trimmed the entry away.
//
// It touches one key, so it is safe under Redis Cluster: the run's stream is
// the only thing that must be updated atomically.
//
// Sequences pass through Lua numbers, so this backend carries them up to 2^53.
// A run numbering from 1 will not reach it.
var publishScript = redis.NewScript(`
local seq = tonumber(ARGV[1])
local top = 0
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)
if #last > 0 then
  top = tonumber(string.match(last[1][1], '^(%d+)'))
end
if seq == 0 then
  seq = top + 1
elseif seq <= top then
  return seq
end
redis.call('XADD', KEYS[1], 'MAXLEN', ARGV[2], seq .. '-0', '` + fieldEvent + `', ARGV[3])
return seq
`)

// Redis is a bus backed by Redis Streams: one stream per run, keyed by the run
// topic, trimmed to the retention depth with an exact MAXLEN so that retention
// is observably the same as Memory's ring.
//
// Prefix subscriptions need to find the runs beneath a topic, and Redis has no
// key notifications worth depending on, so each publish also records the run
// topic in a sorted set per covering topic ("agent" and "agent:<name>") scored
// by publish time and trimmed to WithIndexTTL. That index is a hint, not truth:
// it is updated outside the publish script, entries are re-validated against the
// subscription before use, and a missing entry only delays a subscriber's first
// read of a new run, because catch-up from its cursor still returns everything
// the stream retains.
//
// Reading is one goroutine per subscription: it discovers keys, drains each
// stream with XRANGE from the cursor, then blocks in a single XREAD across
// every key for at most WithPollInterval. The bounded block is what lets a new
// run, a cancelled context and a Close all be noticed promptly; it also means
// each live subscription occupies one connection for most of its cycle, so size
// the client's pool for the number of concurrent subscribers.
//
// Filters are applied client-side. Redis cannot evaluate them, and evaluating
// them in Lua would put the semantics of envelope.Filter in two places.
//
// Requires Redis 6.2 or later, for exclusive XRANGE ranges.
type Redis struct {
	client    redis.UniversalClient
	retention int
	backlog   int
	prefix    string
	poll      time.Duration
	indexTTL  time.Duration

	// mu guards indexed, a cache of when each run topic was last written to
	// the discovery index. It keeps the steady-state publish path to one
	// round trip: only the first publish of a run, and a refresh every
	// quarter of the index TTL, pays for the index.
	mu      sync.Mutex
	indexed map[string]time.Time
}

// indexCacheBound caps the index-refresh cache. Overflowing it forgets
// everything, which costs a few redundant ZADDs and nothing else.
const indexCacheBound = 8192

var _ Bus = (*Redis)(nil)

// NewRedis returns a bus backed by client. It honours WithRetention,
// WithBacklog, WithKeyPrefix, WithPollInterval and WithIndexTTL. The client is
// borrowed, not owned: closing it is the caller's business.
func NewRedis(client redis.UniversalClient, opts ...Option) *Redis {
	o := newOptions(opts)
	return &Redis{
		client:    client,
		retention: o.retention,
		backlog:   o.backlog,
		prefix:    o.prefix,
		poll:      o.poll,
		indexTTL:  o.indexTTL,
		indexed:   make(map[string]time.Time),
	}
}

// streamKey is the key holding one run's events.
func (r *Redis) streamKey(t topic.Topic) string {
	return r.prefix + ":stream:" + t.String()
}

// indexKey is the key holding the run topics discoverable beneath t.
func (r *Redis) indexKey(t topic.Topic) string {
	return r.prefix + ":index:" + t.String()
}

// Publish implements Bus.
func (r *Redis) Publish(ctx context.Context, e envelope.Event) error {
	if err := normalize(&e); err != nil {
		return err
	}
	body, err := encodeEvent(e)
	if err != nil {
		return fmt.Errorf("bus: encode event on %s: %w", e.Topic, err)
	}
	if err := publishScript.Run(ctx, r.client, []string{r.streamKey(e.Topic)}, e.Seq, r.retention, body).Err(); err != nil {
		return fmt.Errorf("bus: publish to %s: %w", e.Topic, err)
	}
	if err := r.index(ctx, e.Topic); err != nil {
		return err
	}
	return nil
}

// index records the run beneath every topic that covers it, so prefix
// subscribers can find it. Throttled by the refresh cache: an active run pays
// once per quarter of the index TTL, which is what keeps it discoverable
// without turning every event into two round trips.
func (r *Redis) index(ctx context.Context, t topic.Topic) error {
	member := t.String()
	now := time.Now()

	r.mu.Lock()
	fresh := now.Sub(r.indexed[member]) < r.indexTTL/4
	r.mu.Unlock()
	if fresh {
		return nil
	}

	agent, err := topic.Agent(t.Name())
	if err != nil {
		return fmt.Errorf("bus: index %s: %w", t, err)
	}
	cutoff := strconv.FormatInt(now.Add(-r.indexTTL).UnixMilli(), 10)
	pipe := r.client.Pipeline()
	for _, covering := range []topic.Topic{topic.All(), agent} {
		key := r.indexKey(covering)
		pipe.ZAdd(ctx, key, redis.Z{Score: float64(now.UnixMilli()), Member: member})
		pipe.ZRemRangeByScore(ctx, key, "-inf", "("+cutoff)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("bus: index %s: %w", t, err)
	}

	r.mu.Lock()
	if len(r.indexed) >= indexCacheBound {
		r.indexed = make(map[string]time.Time)
	}
	r.indexed[member] = now
	r.mu.Unlock()
	return nil
}

// Subscribe implements Bus.
func (r *Redis) Subscribe(ctx context.Context, sub envelope.Subscription) (Stream, error) {
	if err := validate(sub); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	readCtx, cancel := context.WithCancel(ctx)
	s := &redisStream{
		r:      r,
		sub:    sub,
		ch:     make(chan envelope.Event, r.backlog),
		cancel: cancel,
		closed: make(chan struct{}),
		done:   make(chan struct{}),
	}
	go s.read(readCtx)

	// Cancelling the subscription context closes the stream, so a caller that
	// only cancels leaks nothing. The watcher exits either way.
	if done := ctx.Done(); done != nil {
		go func() {
			select {
			case <-done:
				_ = s.Close() // Close waits for the reader and cannot fail
			case <-s.closed:
			}
		}()
	}
	return s, nil
}

// redisStream is one subscription: a reader goroutine filling a bounded buffer.
//
// The buffer is backpressure, not a drop policy: when it is full the reader
// simply stops reading Redis, which costs nothing — Redis is already holding
// the events, and no publisher is waiting on this consumer. A consumer slow
// enough to fall behind retention loses events to MAXLEN rather than being
// terminated, which is the same bargain Memory's ring makes.
type redisStream struct {
	r   *Redis
	sub envelope.Subscription
	ch  chan envelope.Event

	once   sync.Once
	cancel context.CancelFunc
	closed chan struct{} // closed by Close, or by the subscription context
	done   chan struct{} // closed when the reader has exited
	err    error         // the reader's terminal error; written before done
}

var _ Stream = (*redisStream)(nil)

// Recv implements Stream. Buffered events are handed over before any terminal
// error, so a reader that died on a Redis fault still delivers what it had.
func (s *redisStream) Recv(ctx context.Context) (envelope.Event, error) {
	for {
		if err := ctx.Err(); err != nil {
			return envelope.Event{}, err
		}
		select {
		case <-s.closed:
			return envelope.Event{}, ErrClosed
		default:
		}
		select {
		case e := <-s.ch:
			return e, nil
		default:
		}
		select {
		case <-s.done:
			if s.err == nil {
				return envelope.Event{}, ErrClosed // the reader never exits without one
			}
			return envelope.Event{}, s.err
		default:
		}
		select {
		case e := <-s.ch:
			return e, nil
		case <-s.done: // loop: report only after the buffer is drained
		case <-s.closed:
		case <-ctx.Done():
			return envelope.Event{}, ctx.Err()
		}
	}
}

// Close implements Stream. It waits for the reader to return, so no goroutine
// and no connection outlives the call. That wait is bounded by the poll
// interval, the longest a reader can be blocked in XREAD.
func (s *redisStream) Close() error {
	s.once.Do(func() {
		close(s.closed)
		s.cancel()
	})
	<-s.done
	return nil
}

// read is the reader loop: discover the keys in scope, drain each from its
// cursor, and when everything is drained block once across all of them.
func (s *redisStream) read(ctx context.Context) {
	defer close(s.done)

	// cursors maps a stream key to the ID of the last entry the reader has
	// taken from it. It advances past filtered-out entries too, or the reader
	// would re-read them forever.
	cursors := make(map[string]string)
	var discovered time.Time
	for {
		if err := ctx.Err(); err != nil {
			s.terminate(ctx, err)
			return
		}
		if time.Since(discovered) >= s.r.poll {
			if err := s.discover(ctx, cursors); err != nil {
				s.terminate(ctx, err)
				return
			}
			discovered = time.Now()
		}
		n, err := s.catchUp(ctx, cursors)
		if err != nil {
			s.terminate(ctx, err)
			return
		}
		if n > 0 {
			continue // more history to drain before blocking
		}
		if err := s.tail(ctx, cursors); err != nil {
			s.terminate(ctx, err)
			return
		}
	}
}

// terminate records the error Recv will report once the buffer is drained.
//
// A cancelled reader context is not that error: the reader's context is a child
// of the subscription's and is cancelled by Close, so a consumer would be told
// "context canceled" about a context it never passed. The stream simply ended,
// which is ErrClosed — and that keeps the verdict the same whether Close or the
// subscription's context got there first.
func (s *redisStream) terminate(ctx context.Context, err error) {
	if ctx.Err() != nil {
		s.err = ErrClosed
		return
	}
	s.err = err
}

// discover adds the streams in scope that the reader has not seen yet, each
// starting at the subscription's resume cursor.
func (s *redisStream) discover(ctx context.Context, cursors map[string]string) error {
	// Entry IDs are "<seq>-0", so "From-0" is the exclusive cursor meaning
	// "sequences after From" for both XRANGE and XREAD.
	start := strconv.FormatUint(s.sub.From, 10) + "-0"

	if s.sub.Topic.RunID() != "" {
		key := s.r.streamKey(s.sub.Topic)
		if _, ok := cursors[key]; !ok {
			cursors[key] = start
		}
		return nil
	}

	cutoff := strconv.FormatInt(time.Now().Add(-s.r.indexTTL).UnixMilli(), 10)
	members, err := s.r.client.ZRangeByScore(ctx, s.r.indexKey(s.sub.Topic), &redis.ZRangeBy{Min: cutoff, Max: "+inf"}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("bus: discover runs under %s: %w", s.sub.Topic, err)
	}
	for _, member := range members {
		t, err := topic.Parse(member)
		if err != nil || !s.sub.Topic.Covers(t) {
			continue // the index is a hint; anything unusable is ignored
		}
		key := s.r.streamKey(t)
		if _, ok := cursors[key]; !ok {
			cursors[key] = start
		}
	}
	return nil
}

// catchUp reads one batch of retained history per stream and reports how many
// entries it consumed. Zero means every stream is drained up to its cursor,
// which is when the reader may block.
func (s *redisStream) catchUp(ctx context.Context, cursors map[string]string) (int, error) {
	keys := sortedKeys(cursors)
	if len(keys) == 0 {
		return 0, nil
	}
	pipe := s.r.client.Pipeline()
	cmds := make([]*redis.XMessageSliceCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.XRangeN(ctx, key, "("+cursors[key], "+", readBatch)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("bus: read history of %s: %w", s.sub.Topic, err)
	}
	total := 0
	for i, cmd := range cmds {
		msgs, err := cmd.Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue
			}
			return total, fmt.Errorf("bus: read history of %s: %w", keys[i], err)
		}
		n, err := s.emit(ctx, keys[i], msgs, cursors)
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

// tail blocks in one XREAD across every stream in scope, for at most the poll
// interval. With nothing in scope yet it just waits, so a subscription made
// before its agent's first run costs one sleeping goroutine.
func (s *redisStream) tail(ctx context.Context, cursors map[string]string) error {
	keys := sortedKeys(cursors)
	if len(keys) == 0 {
		return sleep(ctx, s.r.poll)
	}
	// XREAD takes every key first, then every ID, in the same order.
	args := make([]string, 0, 2*len(keys))
	args = append(args, keys...)
	for _, key := range keys {
		args = append(args, cursors[key])
	}
	streams, err := s.r.client.XRead(ctx, &redis.XReadArgs{Streams: args, Count: readBatch, Block: s.r.poll}).Result()
	switch {
	case errors.Is(err, redis.Nil):
		return nil // the block elapsed with nothing new
	case err != nil:
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("bus: tail %s: %w", s.sub.Topic, err)
	}
	for _, st := range streams {
		if _, err := s.emit(ctx, st.Stream, st.Messages, cursors); err != nil {
			return err
		}
	}
	return nil
}

// emit decodes each entry, advances the cursor past it, and hands the ones the
// subscription accepts to the consumer. The send blocks when the buffer is
// full: that is backpressure onto Redis, which no publisher is waiting on.
func (s *redisStream) emit(ctx context.Context, key string, msgs []redis.XMessage, cursors map[string]string) (int, error) {
	for i, msg := range msgs {
		e, err := decodeEvent(msg)
		if err != nil {
			return i, fmt.Errorf("bus: decode %s entry %s: %w", key, msg.ID, err)
		}
		cursors[key] = msg.ID
		if !s.sub.Accepts(e) {
			continue
		}
		select {
		case s.ch <- e:
		case <-ctx.Done():
			return i, ctx.Err()
		}
	}
	return len(msgs), nil
}

// sortedKeys keeps the order of streams within a read deterministic. Ordering
// across runs is not part of the contract, but a stable order makes a stalled
// run's effect on its peers reproducible.
func sortedKeys(cursors map[string]string) []string {
	keys := make([]string, 0, len(cursors))
	for key := range cursors {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// sleep waits for d, or returns early if ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// wireEvent is the stream encoding of an event.
//
// Two fields are handled by hand, both by shadowing: encoding/json prefers the
// shallower field when a name appears at two depths, so these win over the
// embedded event's.
//
// Topic is carried as its string form because topic.Topic keeps its segments
// unexported and has no JSON representation of its own; Parse restores it on
// the way back. Seq is shadowed by a field that is never set, so it is omitted
// from the body entirely — the entry ID is the sequence, and writing it twice
// would only create something to disagree.
type wireEvent struct {
	envelope.Event
	Topic string `json:"topic"`
	Seq   uint64 `json:"seq,omitempty"`
}

func encodeEvent(e envelope.Event) (string, error) {
	body, err := json.Marshal(wireEvent{Event: e, Topic: e.Topic.String()})
	if err != nil {
		return "", err
	}
	return string(body), nil
}

func decodeEvent(msg redis.XMessage) (envelope.Event, error) {
	raw, ok := msg.Values[fieldEvent].(string)
	if !ok {
		return envelope.Event{}, fmt.Errorf("entry has no %q field", fieldEvent)
	}
	var w wireEvent
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return envelope.Event{}, err
	}
	t, err := topic.Parse(w.Topic)
	if err != nil {
		return envelope.Event{}, err
	}
	seq, err := seqOf(msg.ID)
	if err != nil {
		return envelope.Event{}, err
	}
	e := w.Event
	e.Topic = t
	e.Seq = seq
	return e, nil
}

// seqOf reads the sequence back out of an entry ID.
func seqOf(id string) (uint64, error) {
	ms, _, ok := strings.Cut(id, "-")
	if !ok {
		return 0, fmt.Errorf("malformed entry id %q", id)
	}
	seq, err := strconv.ParseUint(ms, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("malformed entry id %q: %w", id, err)
	}
	return seq, nil
}
