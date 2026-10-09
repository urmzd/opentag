package agentrt

import (
	"fmt"
	"os"

	saigetypes "github.com/urmzd/saige/agent/types"

	"github.com/urmzd/saige/agent/agenttest"
	"github.com/urmzd/saige/agent/provider/anthropic"
	"github.com/urmzd/saige/agent/provider/ollama"
	"github.com/urmzd/saige/agent/provider/retry"
)

// Provider names a spec may ask for. The set is open: a deployment adds its
// own with WithProvider, and a spec naming a provider the process has not
// registered fails at build time with a message that lists what is available,
// rather than silently falling back to a model nobody asked for.
const (
	// ProviderAnthropic reads ANTHROPIC_API_KEY (see EnvAnthropicKey).
	ProviderAnthropic = "anthropic"
	// ProviderOllama talks to a local daemon (see EnvOllamaHost).
	ProviderOllama = "ollama"
	// ProviderOffline replays a canned script instead of calling a model.
	// It is what makes an opentag deployment demonstrable with no API key,
	// no daemon, and no network: the whole path from tag to delivered event
	// runs, and only the token source is fake. Tests use it with WithScript.
	ProviderOffline = "offline"
)

// Environment variables the built-in providers read. They are read at build
// time, not at init, so a process can set them after start (a config loader, a
// secret fetch) and so a test can set one per case.
const (
	EnvAnthropicKey = "ANTHROPIC_API_KEY"
	EnvOllamaHost   = "OLLAMA_HOST"
)

// DefaultOllamaHost is used when EnvOllamaHost is unset.
const DefaultOllamaHost = "http://localhost:11434"

// ProviderFunc builds the LLM provider a spec asks for.
//
// It takes the whole Spec rather than just the model name because a provider
// may need more of the definition than the model: a thinking budget derived
// from the agent's purpose, a per-agent endpoint, a fake keyed to the agent's
// name. It returns an error rather than panicking or falling back, since a run
// against the wrong model is worse than a run that does not start.
type ProviderFunc func(spec Spec) (saigetypes.Provider, error)

// defaultProviders returns the built-in provider table. It is a function, not a
// package-level map, so no two Runners can mutate each other's table and so the
// zero configuration is not process-global state.
func defaultProviders() map[string]ProviderFunc {
	return map[string]ProviderFunc{
		ProviderAnthropic: anthropicProvider,
		ProviderOllama:    ollamaProvider,
		ProviderOffline:   nil, // resolved per Runner: it needs the script
	}
}

func anthropicProvider(spec Spec) (saigetypes.Provider, error) {
	key := os.Getenv(EnvAnthropicKey)
	if key == "" {
		return nil, fmt.Errorf("%w: agent %q asks for the %s provider but %s is unset", ErrInvalid, spec.Name, ProviderAnthropic, EnvAnthropicKey)
	}
	if spec.Model == "" {
		return nil, fmt.Errorf("%w: agent %q asks for the %s provider with no model", ErrInvalid, spec.Name, ProviderAnthropic)
	}
	return withRetry(anthropic.NewAdapter(key, spec.Model)), nil
}

func ollamaProvider(spec Spec) (saigetypes.Provider, error) {
	if spec.Model == "" {
		return nil, fmt.Errorf("%w: agent %q asks for the %s provider with no model", ErrInvalid, spec.Name, ProviderOllama)
	}
	host := os.Getenv(EnvOllamaHost)
	if host == "" {
		host = DefaultOllamaHost
	}
	// The embedding model is empty: an agent turn generates, it does not
	// embed. Retrieval brings its own embedder (see Retriever).
	return withRetry(ollama.NewAdapter(ollama.NewClient(host, spec.Model, ""))), nil
}

// withRetry wraps a model adapter in saige's retry decorator. saige's adapters
// make one attempt per call (the Anthropic SDK's own retries are off), so this
// is the only retry layer: transient failures such as rate limits and
// overloads are retried with jittered backoff that honors Retry-After, and
// everything else surfaces at once.
func withRetry(p saigetypes.Provider) saigetypes.Provider {
	return retry.New(p, retry.DefaultConfig())
}

// offlineProvider replays script, one entry per model call.
//
// It is saige's own agenttest.ScriptedProvider rather than something written
// here, so the offline path exercises the same provider seam the real adapters
// implement and a test cannot accidentally be given a more forgiving fake than
// production uses.
func offlineProvider(script [][]saigetypes.Delta) ProviderFunc {
	return func(spec Spec) (saigetypes.Provider, error) {
		responses := script
		if len(responses) == 0 {
			responses = [][]saigetypes.Delta{agenttest.TextResponse(defaultOfflineAnswer(spec))}
		}
		return &agenttest.ScriptedProvider{Responses: responses}, nil
	}
}

// defaultOfflineAnswer is what the offline provider says when no script was
// given. It names itself: an operator reading a Slack thread must never mistake
// it for a model's answer.
func defaultOfflineAnswer(spec Spec) string {
	return fmt.Sprintf("%s is running with the offline provider, so no model was called. "+
		"Set the agent's provider to %q or %q for real inference.",
		spec.Name, ProviderAnthropic, ProviderOllama)
}
