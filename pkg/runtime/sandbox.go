package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/urmzd/dispatch/pkg/controlplane"
	dispatchsandbox "github.com/urmzd/dispatch/pkg/sandbox"
	"github.com/urmzd/dispatch/pkg/task"
	"github.com/urmzd/dispatch/pkg/tool"
	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/agentrt"
	"github.com/urmzd/opentag/pkg/agentrt/payload"
	"github.com/urmzd/opentag/pkg/connector"
	"github.com/urmzd/opentag/pkg/envelope"
)

// Sandbox defaults.
const (
	// DefaultBuffer is the per-turn chunk buffer between the tool producing
	// events and the activity publishing them.
	DefaultBuffer = 256
	// DefaultPollInterval is how often a turn checks whether its dispatch
	// task has reported a result.
	DefaultPollInterval = 5 * time.Millisecond
	// AnswerArea is the workspace area a turn writes its answer artifact
	// under. A revision that does not grant it simply has no artifact: the
	// policy decides, and the denial is reported rather than swallowed.
	AnswerArea = "runs"
)

// Catalog builds the tool catalog for one turn.
//
// action is called for every connector action the agent invokes; the Sandbox
// turns each call into an envelope.KindActionTaken event on the run's stream, so
// "the work got done" lands on the bus next to the conversation that decided to
// do it.
type Catalog func(req Request, action func(payload.Action) error) (agentrt.Tools, error)

// Connectors returns a Catalog resolving tool names against the actions every
// registered connector contributes, bound to the tag's own surface.
func Connectors(reg *connector.Registry) Catalog {
	return func(req Request, action func(payload.Action) error) (agentrt.Tools, error) {
		target, err := req.SourceAddress()
		if err != nil {
			return nil, err
		}
		return agentrt.ConnectorTools(reg, target, action), nil
	}
}

// SourceAddress parses the request's origin surface.
func (r Request) SourceAddress() (address.Address, error) {
	if r.Source == "" {
		return address.Address{}, nil
	}
	a, err := address.Parse(r.Source)
	if err != nil {
		return address.Address{}, fmt.Errorf("runtime: run %s: source: %w", r.RunID, err)
	}
	return a, nil
}

// Sandbox executes turns as dispatch tasks, under the NGAC policy compiled from
// the pinned revision's Access grant.
//
// # What the sandbox is for
//
// A turn is arbitrary model-directed code: it calls tools, writes files, and may
// delegate to other agents. Access says what the agent may touch, and saying it
// is worth nothing unless something enforces it. dispatch is that something: the
// tool never receives the shared workspace, only a view scoped to its policy, and
// every spawn attempt is checked against the policy's allowlist before it
// reaches a queue. Default is deny, so an agent granted nothing can do nothing.
//
// # One deployment per pinned revision
//
// The policy lives on the deployment, and a run pins a revision, so the
// deployment is keyed by (agent, revision): revising an agent's Access cannot
// retroactively widen or narrow what a run already in flight is allowed to do.
// The TOOL, in contrast, is keyed by agent name alone, because Access.Spawn names
// agents and a spawn allowlist has to name something an agent can know without
// knowing which revision of it is current.
//
// # Streaming out of a sandbox
//
// A dispatch tool is a function over bytes: it has no way to stream, and the node
// running it holds neither the caller's context nor the run's replay frame. So
// the tool sends its chunks through an in-process channel registered under the
// run id, and the activity that submitted the task drains that channel on its own
// goroutine — which is what keeps every duraturo call (Emit, and the ledger
// writes behind it) on the single goroutine duraturo requires, while the agent
// loop's own parallelism stays inside the task.
//
// That handoff is in-process by construction. A remote dispatch node would have
// to publish to the bus itself; nothing here pretends otherwise.
//
// # At-most-once, on purpose
//
// dispatch's queue is at-most-once in beta. A lost task is a failed attempt, and
// the attempt is what duraturo already knows how to repeat. Durability is never
// borrowed from dispatch.
type Sandbox struct {
	plane   controlplane.ControlPlane
	tools   *tool.Registry
	specs   Specs
	catalog Catalog
	// delegator, when set, carries out a delegation for a turn that is a
	// durable run, in place of the in-turn sub-task.
	delegator Delegator

	agentOpts []agentrt.Option
	replicas  int
	buffer    int
	poll      time.Duration
	logger    *slog.Logger

	mu         sync.Mutex
	deployed   map[string]string             // "<agent>@<rev>" -> deployment name
	registered map[string]bool               // dispatch tool name -> registered
	streams    map[string]chan agentrt.Chunk // run id -> live chunk sink
}

// SandboxOption configures a Sandbox.
type SandboxOption func(*Sandbox)

// WithCatalog sets how a spec's tool names are resolved. Without one, a spec
// that names a tool fails to build, which is the safe default: an agent silently
// missing its tools answers the wrong question.
func WithCatalog(c Catalog) SandboxOption {
	return func(s *Sandbox) {
		if c != nil {
			s.catalog = c
		}
	}
}

// Delegator carries out one delegation: parent's turn asked target to do text.
// It returns the answer the parent's model reads as the tool's result.
//
// It is how a host makes a delegated turn a run in its own right, with an id, a
// stream and a record, where the default makes it a sub-task of the parent's
// turn whose only trace is the tool result. The host decides what that run is
// and how it is linked to parent; the runtime only stops answering the
// delegation itself.
//
// report puts an action on the parent's stream, so the host can say which run
// it started before that run has an answer.
//
// The spawn allowlist has already let the tool exist: a Delegator is called
// only for a target the parent's pinned revision names in Access.Spawn. A host
// with limits of its own enforces them here and returns the refusal as an
// error, which the model sees as the tool failing.
type Delegator func(ctx context.Context, parent Request, target, text string, report func(payload.Action) error) (string, error)

// WithDelegator sets who carries out a delegation made by a turn that is a
// durable run. A turn that is itself a sub-task has no run to hang another one
// from, so it keeps delegating by sub-task. Without a Delegator every
// delegation is a sub-task, which is what it always was.
func WithDelegator(d Delegator) SandboxOption {
	return func(s *Sandbox) { s.delegator = d }
}

// WithAgentOptions passes options through to every agentrt.Runner the sandbox
// builds: the provider table, an offline script, a retriever, loop bounds.
func WithAgentOptions(opts ...agentrt.Option) SandboxOption {
	return func(s *Sandbox) { s.agentOpts = append(s.agentOpts, opts...) }
}

// WithReplicas sets how many local execution nodes each deployment starts with.
func WithReplicas(n int) SandboxOption {
	return func(s *Sandbox) {
		if n > 0 {
			s.replicas = n
		}
	}
}

// WithBuffer sets the per-turn chunk buffer (default DefaultBuffer). It is
// backpressure, not a drop policy: a full buffer blocks the agent loop until the
// publisher catches up, because dropping an event from the middle of a stream
// would leave a sink rendering a conversation with a hole in it.
func WithBuffer(n int) SandboxOption {
	return func(s *Sandbox) {
		if n > 0 {
			s.buffer = n
		}
	}
}

// WithPollInterval sets how often a turn checks for its task's result.
func WithPollInterval(d time.Duration) SandboxOption {
	return func(s *Sandbox) {
		if d > 0 {
			s.poll = d
		}
	}
}

// WithSandboxLogger sets the logger.
func WithSandboxLogger(l *slog.Logger) SandboxOption {
	return func(s *Sandbox) {
		if l != nil {
			s.logger = l
		}
	}
}

// NewSandbox returns an executor over a dispatch control plane.
//
// tools is the registry the plane's nodes resolve from; the sandbox registers one
// tool per agent into it as agents are first seen. specs is used only to resolve
// a delegation target's definition; passing nil disables delegation with an error
// the model can read.
func NewSandbox(plane controlplane.ControlPlane, tools *tool.Registry, specs Specs, opts ...SandboxOption) (*Sandbox, error) {
	if plane == nil {
		return nil, fmt.Errorf("%w: no dispatch control plane", ErrInvalid)
	}
	if tools == nil {
		return nil, fmt.Errorf("%w: no dispatch tool registry", ErrInvalid)
	}
	s := &Sandbox{
		plane:      plane,
		tools:      tools,
		specs:      specs,
		replicas:   1,
		buffer:     DefaultBuffer,
		poll:       DefaultPollInterval,
		logger:     slog.Default(),
		deployed:   make(map[string]string),
		registered: make(map[string]bool),
		streams:    make(map[string]chan agentrt.Chunk),
	}
	for _, o := range opts {
		o(s)
	}
	return s, nil
}

var _ Executor = (*Sandbox)(nil)

// ToolName is the dispatch tool an agent's turns execute as. It is also the
// agent's identity in the policy graph, and the object a spawn grant names.
func ToolName(agent string) string { return "agent:" + agent }

// DeploymentName is the dispatch deployment a pinned revision runs in.
func DeploymentName(agent string, rev int) string {
	return fmt.Sprintf("opentag-%s-r%d", agent, rev)
}

// DelegateToolName is the tool an agent calls to tag another agent.
func DelegateToolName(agent string) string { return "tag_" + agent }

// AnswerKey is where a run's answer artifact is written.
func AnswerKey(agent, runID string) string {
	return path.Join(AnswerArea, agent, runID, "answer")
}

// Execute submits the turn as a dispatch task and publishes what it streams.
func (s *Sandbox) Execute(ctx context.Context, req Request, emit func(context.Context, agentrt.Chunk) error) (Outcome, error) {
	if req.RunID == "" {
		return Outcome{}, fmt.Errorf("%w: turn has no run id", ErrInvalid)
	}
	deployment, err := s.ensure(ctx, req.Revision)
	if err != nil {
		return Outcome{}, err
	}
	req.Service = deployment

	sink := make(chan agentrt.Chunk, s.buffer)
	if err := s.attach(req.RunID, sink); err != nil {
		return Outcome{}, err
	}
	defer s.detach(req.RunID)

	input, err := json.Marshal(req)
	if err != nil {
		return Outcome{}, fmt.Errorf("runtime: run %s: marshal request: %w", req.RunID, err)
	}
	id, err := s.plane.SubmitAsync(ctx, deployment, task.Task{Tool: ToolName(req.Agent()), Input: input})
	if err != nil {
		return Outcome{}, fmt.Errorf("runtime: run %s: submit to %s: %w", req.RunID, deployment, err)
	}

	return s.stream(ctx, req, deployment, id, sink, emit)
}

// stream drains the turn's chunks and returns its outcome.
//
// The loop watches the sink and polls for the task's result rather than waiting
// for the tool to close anything, because the paths that produce no chunks at all
// — an unknown tool, a node that died, a task the at-most-once queue lost — must
// end the turn too. Once a result has been reported, every chunk the tool sent is
// already in the buffer (it sent them all before returning, and a full buffer
// would have blocked it), so a non-blocking drain finishes the stream in order
// with nothing lost.
func (s *Sandbox) stream(ctx context.Context, req Request, deployment, taskID string, sink <-chan agentrt.Chunk, emit func(context.Context, agentrt.Chunk) error) (Outcome, error) {
	send := func(c agentrt.Chunk) error {
		if emit == nil {
			return nil
		}
		return emit(ctx, c)
	}
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			// The task keeps running: dispatch owns its lifecycle and its
			// queue is at-most-once. The run is what gets retried.
			return Outcome{}, fmt.Errorf("runtime: run %s: %w", req.RunID, ctx.Err())

		case c := <-sink:
			if err := send(c); err != nil {
				return Outcome{}, err
			}

		case <-ticker.C:
			res, done, err := s.plane.Result(ctx, deployment, taskID)
			if err != nil {
				return Outcome{}, fmt.Errorf("runtime: run %s: result: %w", req.RunID, err)
			}
			if !done {
				continue
			}
			for {
				select {
				case c := <-sink:
					if err := send(c); err != nil {
						return Outcome{}, err
					}
					continue
				default:
				}
				break
			}
			if res.Error != "" {
				return Outcome{}, fmt.Errorf("runtime: run %s: task %s: %s", req.RunID, taskID, res.Error)
			}
			var out Outcome
			if err := json.Unmarshal(res.Output, &out); err != nil {
				return Outcome{}, fmt.Errorf("runtime: run %s: decode outcome: %w", req.RunID, err)
			}
			return out, nil
		}
	}
}

// ensure deploys the service a pinned revision runs in, registering its tool and
// its delegation targets' tools, and returns the deployment name.
func (s *Sandbox) ensure(ctx context.Context, rev agentrt.Revision) (string, error) {
	agent := rev.Spec.Name
	key := fmt.Sprintf("%s@%d", agent, rev.Rev)

	s.mu.Lock()
	name, ok := s.deployed[key]
	s.mu.Unlock()
	if ok {
		return name, nil
	}

	name = DeploymentName(agent, rev.Rev)
	if err := s.register(ToolName(agent)); err != nil {
		return "", err
	}
	spawn := make([]string, 0, len(rev.Spec.Access.Spawn))
	for _, target := range rev.Spec.Access.Spawn {
		// A delegation target must be dispatchable before it can be spawned,
		// and it runs under its own tool name with no grants of its own: a
		// sub-task inherits nothing.
		if err := s.register(ToolName(target)); err != nil {
			return "", err
		}
		spawn = append(spawn, ToolName(target))
	}
	areas := make([]dispatchsandbox.Area, 0, len(rev.Spec.Access.WorkspaceAreas))
	for _, prefix := range rev.Spec.Access.WorkspaceAreas {
		areas = append(areas, dispatchsandbox.Area{Prefix: prefix})
	}

	err := s.plane.Deploy(ctx, controlplane.ServiceSpec{
		Name:     name,
		Replicas: s.replicas,
		Policies: []dispatchsandbox.Policy{{
			Tool:  ToolName(agent),
			Areas: areas,
			Spawn: spawn,
		}},
	})
	// Another runtime instance may have deployed the same pinned revision
	// first; the spec is a pure function of the revision, so the existing
	// deployment is the same deployment.
	if err != nil && !errors.Is(err, controlplane.ErrExists) {
		return "", fmt.Errorf("runtime: deploy %s: %w", name, err)
	}

	s.mu.Lock()
	s.deployed[key] = name
	s.mu.Unlock()
	return name, nil
}

// register adds an agent's tool to the dispatch registry, once.
func (s *Sandbox) register(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.registered[name] {
		return nil
	}
	if err := s.tools.Register(tool.Func(name, s.call)); err != nil {
		// Registered by another Sandbox over the same registry: the tool body
		// is this package's either way, so adopt it rather than failing.
		s.logger.Debug("opentag/runtime: dispatch tool already registered", "tool", name, "error", err)
	}
	s.registered[name] = true
	return nil
}

// attach registers a run's live chunk sink.
func (s *Sandbox) attach(runID string, sink chan agentrt.Chunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.streams[runID]; exists {
		// Two attempts of one run publishing into two sinks would interleave
		// on the same topic. duraturo's fencing already stops a zombie; this
		// stops the in-process case it cannot see.
		return fmt.Errorf("%w: run %s is already executing in this process", ErrInvalid, runID)
	}
	s.streams[runID] = sink
	return nil
}

func (s *Sandbox) detach(runID string) {
	s.mu.Lock()
	delete(s.streams, runID)
	s.mu.Unlock()
}

func (s *Sandbox) sink(runID string) chan agentrt.Chunk {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[runID]
}

// call is the dispatch tool: one agent turn, inside the sandbox.
//
// It runs on a node's goroutine, with a context that carries neither the run's
// replay frame nor the caller's cancellation, and with a workspace already scoped
// to the policy. Everything it needs travels in the task input.
func (s *Sandbox) call(ctx context.Context, rt tool.Runtime, input []byte) ([]byte, error) {
	var req Request
	if err := json.Unmarshal(input, &req); err != nil {
		return nil, fmt.Errorf("runtime: sandbox: decode request: %w", err)
	}
	rev := req.Revision
	if err := rev.Validate(); err != nil {
		return nil, err
	}

	// A turn with no registered sink is a delegated sub-task: its answer is the
	// tool result its parent reads, and it is not a durable run of its own, so
	// it has no stream to publish onto.
	sink := s.sink(req.RunID)
	send := func(c agentrt.Chunk) error {
		if sink == nil {
			return nil
		}
		select {
		case sink <- c:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	tools, granted, err := s.turnTools(rt, req, send)
	if err != nil {
		return nil, err
	}
	// The runner registers exactly the names the revision grants, so the
	// delegation tools have to be in that list. The revision here is a local
	// copy: the pinned definition itself is never edited.
	rev.Spec.Tools = granted

	runner, err := agentrt.New(rev, append(append([]agentrt.Option{}, s.agentOpts...), agentrt.WithTools(tools))...)
	if err != nil {
		return nil, err
	}

	res, err := runner.Run(ctx, agentrt.Turn{Text: req.Text, Meta: req.Meta}, func(_ context.Context, c agentrt.Chunk) error {
		return send(c)
	})
	if err != nil {
		return nil, err
	}

	out := Outcome{Text: res.Text, Chunks: res.Chunks, Citations: res.Citations}
	key := AnswerKey(rev.Spec.Name, req.RunID)
	if werr := rt.Workspace().Write(ctx, key, strings.NewReader(res.Text)); werr != nil {
		// The policy decided, and that is not a turn failure: an agent granted
		// no workspace area still answered. Reporting it keeps the decision
		// visible instead of pretending the artifact exists.
		out.StoreError = werr.Error()
	} else {
		out.Stored = key
	}
	return json.Marshal(out)
}

// turnTools assembles the tools one turn may call: the deployment's catalog for
// the names the spec grants, plus one delegation tool per agent in Access.Spawn.
//
// Delegation is granted by Access.Spawn alone rather than by also listing
// "tag_<agent>" in Spec.Tools. The allowlist is the grant; requiring the same
// fact in two places would let them disagree, and the failure mode of that
// disagreement is an agent that cannot delegate despite being allowed to, or the
// reverse.
// It returns the catalog and the effective list of granted tool names, in a
// stable order so that two turns of the same revision show the model the same
// tools in the same sequence.
func (s *Sandbox) turnTools(rt tool.Runtime, req Request, send func(agentrt.Chunk) error) (agentrt.Tools, []string, error) {
	report := func(a payload.Action) error {
		body, err := payload.Encode(a)
		if err != nil {
			return err
		}
		return send(agentrt.Chunk{Kind: envelope.KindActionTaken, Payload: body})
	}

	granted := make([]string, 0, len(req.Revision.Spec.Tools)+len(req.Revision.Spec.Access.Spawn))
	seen := make(map[string]bool, cap(granted))
	add := func(name string) {
		if name == "" || seen[name] {
			return
		}
		seen[name] = true
		granted = append(granted, name)
	}
	for _, name := range req.Revision.Spec.Tools {
		add(name)
	}

	delegates := make(map[string]saigetypes.Tool, len(req.Revision.Spec.Access.Spawn))
	for _, target := range req.Revision.Spec.Access.Spawn {
		name := DelegateToolName(target)
		delegates[name] = s.delegateTool(rt, req, target, report)
		add(name)
	}

	var base agentrt.Tools
	if s.catalog != nil {
		b, err := s.catalog(req, report)
		if err != nil {
			return nil, nil, err
		}
		base = b
	}

	return agentrt.ToolsFunc(func(name string) (saigetypes.Tool, error) {
		if t, ok := delegates[name]; ok {
			return t, nil
		}
		if base == nil {
			return nil, fmt.Errorf("%w: %q (no tool catalog configured)", agentrt.ErrNoSuchTool, name)
		}
		return base.Lookup(name)
	}), granted, nil
}

// delegateTool lets an agent tag another agent, gated by the spawn allowlist.
//
// The child is a sub-task of THIS turn, not a durable run: it is submitted into
// the same deployment, its answer comes back as this tool's result, and it
// therefore lands inside the parent activity's memoized record. That is why it
// resolves the target's current revision rather than pinning one — nothing
// replays a tool result, so there is nothing for an unpinned resolution to
// diverge from.
//
// The other way to have an agent tag an agent is to raise a Tag at it, which
// creates a durable run of its own with its own pinned revision and its own
// topic. That is the right shape when the delegated work should outlive this
// turn; this one is the right shape when the parent needs the answer to continue.
func (s *Sandbox) delegateTool(rt tool.Runtime, parent Request, target string, report func(payload.Action) error) saigetypes.Tool {
	return &saigetypes.ToolFunc{
		Def: saigetypes.ToolDef{
			Name:        DelegateToolName(target),
			Description: fmt.Sprintf("Tag the %q agent with a request and wait for its answer.", target),
			Parameters: saigetypes.ParameterSchema{
				Type:     "object",
				Required: []string{"text"},
				Properties: map[string]saigetypes.PropertyDef{
					"text": {Type: "string", Description: "The request to send to " + target},
				},
			},
		},
		Fn: func(ctx context.Context, args map[string]any) (string, error) {
			text, _ := args["text"].(string)
			if strings.TrimSpace(text) == "" {
				return "", fmt.Errorf("%w: delegation to %q needs a text request", ErrInvalid, target)
			}
			// A host that gives delegated turns runs of their own takes it
			// from here. A sub-task has no run id, so it cannot be a parent
			// to one and falls through to delegating by sub-task.
			if s.delegator != nil && parent.RunID != "" {
				return s.delegator(ctx, parent, target, text, report)
			}
			if s.specs == nil {
				return "", fmt.Errorf("%w: this deployment cannot resolve agent %q", ErrInvalid, target)
			}
			rev, err := s.specs.Latest(ctx, parent.Tenant, target)
			if err != nil {
				return "", fmt.Errorf("runtime: delegate to %q: %w", target, err)
			}
			child := Request{
				// No run id: a sub-task has no stream of its own, and its
				// output would be attributed to the wrong agent on the
				// parent's topic. It reaches the transcript as this tool's
				// result, which is where the model put it.
				Tenant:   parent.Tenant,
				Attempt:  parent.Attempt,
				Origin:   parent.Origin,
				Source:   parent.Source,
				Text:     text,
				Revision: rev,
				Service:  parent.Service,
			}
			input, err := json.Marshal(child)
			if err != nil {
				return "", fmt.Errorf("runtime: delegate to %q: %w", target, err)
			}
			id, err := rt.Spawn(ctx, task.Task{Tool: ToolName(target), Input: input})
			if err != nil {
				// A denial is the policy speaking. The model sees it as a
				// tool error and can say so, instead of the turn dying.
				return "", fmt.Errorf("runtime: delegate to %q: %w", target, err)
			}
			out, err := s.await(ctx, parent.Service, id)
			if err != nil {
				return "", err
			}
			return out.Text, nil
		},
	}
}

// await polls for a delegated sub-task's result.
func (s *Sandbox) await(ctx context.Context, deployment, taskID string) (Outcome, error) {
	ticker := time.NewTicker(s.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return Outcome{}, ctx.Err()
		case <-ticker.C:
		}
		res, done, err := s.plane.Result(ctx, deployment, taskID)
		if err != nil {
			return Outcome{}, fmt.Errorf("runtime: await task %s: %w", taskID, err)
		}
		if !done {
			continue
		}
		if res.Error != "" {
			return Outcome{}, fmt.Errorf("runtime: task %s: %s", taskID, res.Error)
		}
		var out Outcome
		if err := json.Unmarshal(res.Output, &out); err != nil {
			return Outcome{}, fmt.Errorf("runtime: task %s: decode outcome: %w", taskID, err)
		}
		return out, nil
	}
}
