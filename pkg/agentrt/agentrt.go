// Package agentrt builds a running agent from a pinned agent revision.
//
// It is the seam between two vocabularies. saige speaks providers, messages,
// tools, and a stream of typed deltas; opentag speaks revisions, kinds, and
// events. agentrt translates in exactly one direction — a Revision in, event
// bodies out — and knows nothing about how the revision was stored, which run
// it is executing, or where its events end up. That ignorance is the point: the
// same translation serves a durable run under pkg/runtime, a one-shot CLI
// invocation, and a test with a scripted provider, and none of them can leak
// their concerns into the others.
//
// # What it produces
//
// A turn's output is a sequence of Chunk values: a Kind and a marshalled body
// (see pkg/agentrt/payload). Chunks are not events, because an event needs run
// identity — topic, sequence, tenant, pinned revision — and this package cannot
// know any of it. pkg/runtime stamps that on.
//
// # Citations
//
// Citations are derived from retrieval (see Retriever and Citations), which
// makes them honest: they report the passages the turn was given, not the ones
// a model claims to have used. saige's CitationDelta, which reports what a
// model or tool cited, is not translated.
//
// # Revisions are pinned, not resolved
//
// New takes a Revision, never an agent name. Nothing in this package can look
// up "the current definition of docs-bot", because a durable run replays from
// the top and a definition that moved between attempts would make the replay
// diverge from its own ledger. Pinning is pkg/runtime's job; refusing to
// un-pin is this package's.
package agentrt

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	saige "github.com/urmzd/saige/agent"
	saigetypes "github.com/urmzd/saige/agent/types"
)

// DefaultMaxIter bounds the agent loop's tool-calling iterations per turn. It
// matches saige's own default and is exposed so a deployment can lower it: an
// agent that loops is a cost incident, and the run's retry budget will not stop
// it because a looping turn never fails.
const DefaultMaxIter = 10

// Turn is one request to an agent.
type Turn struct {
	// Text is the request, with the agent mention already stripped.
	Text string
	// Meta is trigger-specific context (a pull request number, an issue
	// title). It is rendered into the prompt in sorted key order so the same
	// tag produces the same prompt on every attempt.
	Meta map[string]string
}

// Result is what a turn produced.
type Result struct {
	// Text is the agent's final answer, the concatenation of its text
	// deltas.
	Text string
	// Chunks counts the event bodies the turn emitted, citations included.
	Chunks int
	// Citations counts the retrieved passages the turn was grounded in.
	Citations int
}

// Runner runs turns of one pinned revision.
//
// Construction validates everything a turn needs — the provider exists and is
// configured, every tool the spec names is resolvable — so a run fails at accept
// time with a clear reason instead of halfway through a Slack thread. A Runner
// is safe for concurrent use only insofar as its provider and tools are; each
// turn gets its own conversation tree.
type Runner struct {
	rev       Revision
	provider  saigetypes.Provider
	tools     *saigetypes.ToolRegistry
	retriever Retriever
	maxIter   int
	logger    *slog.Logger
}

// Option configures a Runner.
type Option func(*config)

type config struct {
	providers map[string]ProviderFunc
	script    [][]saigetypes.Delta
	tools     Tools
	retriever Retriever
	maxIter   int
	logger    *slog.Logger
}

// WithProvider registers or replaces a provider factory under name. A
// deployment adds its own backends this way; it also overrides a built-in, which
// is how a test pins "anthropic" to a fake without an API key.
func WithProvider(name string, fn ProviderFunc) Option {
	return func(c *config) {
		if name == "" || fn == nil {
			return
		}
		c.providers[name] = fn
	}
}

// WithScript sets the delta sequences the offline provider replays, one per
// model call. It is the zero-infrastructure path: no key, no daemon, no
// network, and the whole tag-to-event path still runs.
func WithScript(script ...[]saigetypes.Delta) Option {
	return func(c *config) { c.script = script }
}

// WithTools sets the catalog the spec's tool names resolve against. Without
// one, a spec that names any tool fails to build: silently running an agent
// without the tools it was defined with would answer the wrong question.
func WithTools(t Tools) Option {
	return func(c *config) { c.tools = t }
}

// WithRetriever grounds turns in a corpus. Without one, a spec's Sources are
// inert and the agent answers from the model alone.
func WithRetriever(r Retriever) Option {
	return func(c *config) { c.retriever = r }
}

// WithMaxIter bounds tool-calling iterations per turn (default DefaultMaxIter).
func WithMaxIter(n int) Option {
	return func(c *config) {
		if n > 0 {
			c.maxIter = n
		}
	}
}

// WithLogger sets the logger. Default is slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}

// New builds a runner for one pinned revision.
func New(rev Revision, opts ...Option) (*Runner, error) {
	if err := rev.Validate(); err != nil {
		return nil, err
	}
	cfg := config{
		providers: defaultProviders(),
		maxIter:   DefaultMaxIter,
		logger:    slog.Default(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	// The offline provider is resolved last because it needs the script,
	// which an option may have supplied. An explicit WithProvider for the
	// same name wins: overriding a built-in is the documented escape hatch.
	if cfg.providers[ProviderOffline] == nil {
		cfg.providers[ProviderOffline] = offlineProvider(cfg.script)
	}

	newProvider, ok := cfg.providers[rev.Spec.Provider]
	if !ok || newProvider == nil {
		return nil, fmt.Errorf("%w: agent %q revision %d names provider %q; this deployment has %v",
			ErrInvalid, rev.Spec.Name, rev.Rev, rev.Spec.Provider, providerNames(cfg.providers))
	}
	provider, err := newProvider(rev.Spec)
	if err != nil {
		return nil, err
	}

	tools := saigetypes.NewToolRegistry()
	for _, name := range rev.Spec.Tools {
		if cfg.tools == nil {
			return nil, fmt.Errorf("%w: agent %q revision %d grants tool %q but no tool catalog is configured",
				ErrInvalid, rev.Spec.Name, rev.Rev, name)
		}
		t, err := cfg.tools.Lookup(name)
		if err != nil {
			return nil, fmt.Errorf("agentrt: agent %q revision %d: %w", rev.Spec.Name, rev.Rev, err)
		}
		tools.Register(t)
	}

	return &Runner{
		rev:       rev,
		provider:  provider,
		tools:     tools,
		retriever: cfg.retriever,
		maxIter:   cfg.maxIter,
		logger:    cfg.logger,
	}, nil
}

// Revision returns the pinned revision this runner executes. Callers assert on
// it: a run that pinned revision 6 must still report 6 after the agent has been
// revised to 7.
func (r *Runner) Revision() Revision { return r.rev }

// SystemPrompt returns the prompt the pinned revision runs with. It exists so a
// caller can prove which definition executed, which a run id and a revision
// number alone cannot show.
func (r *Runner) SystemPrompt() string { return r.rev.Spec.SystemPrompt }

// Run executes one turn, handing every event body to emit as it is produced.
//
// emit is called on Run's goroutine, in order, and an error from it aborts the
// turn: the caller publishing chunks is the caller that cannot keep up or has
// gone away, and continuing to burn model tokens for a stream nobody is reading
// is worse than stopping.
//
// A failed turn returns an error and whatever text was produced before it
// failed, because a partially-answered turn is what a sink has already rendered
// and pretending otherwise would make the thread inconsistent with the run.
func (r *Runner) Run(ctx context.Context, t Turn, emit func(context.Context, Chunk) error) (Result, error) {
	var res Result
	send := func(c Chunk) error {
		if emit == nil {
			res.Chunks++
			return nil
		}
		if err := emit(ctx, c); err != nil {
			return err
		}
		res.Chunks++
		return nil
	}

	prompt := t.Text
	if r.retriever != nil && len(r.rev.Spec.Sources) > 0 {
		ac, err := r.retriever.Retrieve(ctx, t.Text)
		if err != nil {
			return res, fmt.Errorf("agentrt: agent %q revision %d: retrieve: %w", r.rev.Spec.Name, r.rev.Rev, err)
		}
		cites := Citations(ac)
		for _, c := range cites {
			if err := send(c); err != nil {
				return res, err
			}
		}
		res.Citations = len(cites)
		if ac != nil && ac.Prompt != "" {
			prompt = ac.Prompt + "\n\n" + prompt
		}
	}
	if meta := renderMeta(t.Meta); meta != "" {
		prompt = meta + "\n\n" + prompt
	}

	agent := saige.NewAgent(saige.AgentConfig{
		Name:         r.rev.Spec.Name,
		SystemPrompt: r.rev.Spec.SystemPrompt,
		Provider:     r.provider,
		Tools:        r.tools,
		MaxIter:      r.maxIter,
		Logger:       r.logger,
	})

	stream := agent.Invoke(ctx, []saigetypes.Message{saigetypes.NewUserMessage(prompt)})
	translator := NewTranslator()

	var answer strings.Builder
	var streamErr error
	for d := range stream.Deltas() {
		switch v := d.(type) {
		case saigetypes.TextContentDelta:
			answer.WriteString(v.Content)
		case saigetypes.ErrorDelta:
			// Failure is a lifecycle fact, not an event of its own (see
			// Translator.Delta). Keep the first one and let the stream
			// drain: abandoning the channel would leak the producer.
			if streamErr == nil && v.Error != nil {
				streamErr = v.Error
			}
		case saigetypes.MarkerDelta:
			// A tool asked for human approval mid-turn. Nothing in a durable
			// turn can answer that question — the caller is a queue, not a
			// person — and leaving it unresolved would block the stream
			// forever, so it is refused explicitly. The right shape for
			// approval is to park the run on an external event; see
			// pkg/runtime.
			//
			// A refusal that cannot be delivered would leave the tool
			// waiting, so it fails the turn and stops the stream instead.
			err := stream.ResolveMarkerErr(v.ToolCallID, saige.Resolution{
				Message: "opentag: interactive approval is not available inside a durable run",
			})
			if err != nil {
				streamErr = errors.Join(streamErr, fmt.Errorf("refuse approval for tool call %s: %w", v.ToolCallID, err))
				stream.Cancel()
			}
		}
		c, ok := translator.Delta(d)
		if !ok {
			continue
		}
		if err := send(c); err != nil {
			stream.Cancel()
			drain(stream)
			return res, err
		}
	}

	res.Text = answer.String()
	if err := stream.Wait(); err != nil {
		streamErr = errors.Join(streamErr, err)
	}
	if streamErr != nil {
		return res, fmt.Errorf("agentrt: agent %q revision %d: %w", r.rev.Spec.Name, r.rev.Rev, streamErr)
	}
	return res, nil
}

// drain empties a cancelled stream so its producing goroutine can exit.
func drain(s *saige.EventStream) {
	for range s.Deltas() {
	}
}

// renderMeta formats a tag's metadata as a context block, in sorted key order.
//
// Determinism matters more than prettiness here: the rendered prompt is part of
// the input a durable turn re-executes with, and Go's map iteration order is
// random, so an unsorted rendering would make two attempts of the same tag ask
// the model two different questions.
func renderMeta(meta map[string]string) string {
	if len(meta) == 0 {
		return ""
	}
	keys := make([]string, 0, len(meta))
	for k := range meta {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("Context from the trigger:")
	for _, k := range keys {
		fmt.Fprintf(&b, "\n- %s: %s", k, meta[k])
	}
	return b.String()
}

func providerNames(m map[string]ProviderFunc) []string {
	out := make([]string, 0, len(m))
	for k, v := range m {
		if v != nil {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

var _ Tools = ToolsFunc(nil)
