package slack

import (
	"strconv"
	"strings"
	"time"

	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/connectors/mention"
	"github.com/urmzd/opentag/pkg/envelope"
)

// eventEnvelope is the outer body of an Events API request. Only the fields the
// connector acts on are decoded: an unknown field is not an error, because Slack
// adds them without notice and a strict decoder would turn that into an outage.
type eventEnvelope struct {
	Type string `json:"type"`
	// Challenge answers a url_verification request.
	Challenge string `json:"challenge"`
	// TeamID is the workspace, and becomes the address workspace.
	TeamID string `json:"team_id"`
	// EventID is Slack's own identifier for this delivery and becomes Tag.ID.
	// Slack reuses it across retries of the same event, which is exactly the
	// property an idempotency key needs.
	EventID string `json:"event_id"`
	// EventTime is when Slack observed the event, in whole seconds.
	EventTime int64 `json:"event_time"`
	// APIAppID identifies the app the event was delivered to.
	APIAppID string `json:"api_app_id"`

	Event innerEvent `json:"event"`
}

// innerEvent is the event itself.
type innerEvent struct {
	Type        string `json:"type"`
	Subtype     string `json:"subtype"`
	User        string `json:"user"`
	BotID       string `json:"bot_id"`
	Text        string `json:"text"`
	TS          string `json:"ts"`
	EventTS     string `json:"event_ts"`
	ThreadTS    string `json:"thread_ts"`
	Channel     string `json:"channel"`
	ChannelType string `json:"channel_type"`
}

// tagFrom translates a Slack event into a Tag, or reports that this event raises
// none.
//
// Everything that decides "no tag" is here rather than spread through the
// handler, because "which events do we ignore" is a security-adjacent question:
// the loop-prevention rule (never act on a bot's message) is the difference
// between a mesh and a runaway.
func (c *Connector) tagFrom(env eventEnvelope) (envelope.Tag, bool) {
	if env.Type != "event_callback" {
		return envelope.Tag{}, false
	}
	ev := env.Event

	switch ev.Type {
	case "app_mention":
		// A mention in a channel: always a candidate.
	case "message":
		// A direct message to the app is a mention by context — there is nobody
		// else in the conversation. In a channel, a plain message is not:
		// answering every message in a channel we are in is not what joining a
		// channel means.
		if ev.ChannelType != "im" {
			return envelope.Tag{}, false
		}
		if ev.Subtype != "" {
			// Joins, leaves, edits, file shares and the rest are not requests.
			return envelope.Tag{}, false
		}
	default:
		return envelope.Tag{}, false
	}

	// Loop prevention, and the reason it is unconditional: this connector's sink
	// posts into the same channels its trigger reads. A bot message that reached
	// the agent would let the mesh answer itself.
	if ev.BotID != "" || ev.User == "" {
		return envelope.Tag{}, false
	}
	if env.TeamID == "" || ev.Channel == "" {
		return envelope.Tag{}, false
	}
	if err := address.ValidWorkspace(env.TeamID); err != nil {
		return envelope.Tag{}, false
	}

	text, mentioned := decode(ev.Text, c.cfg.Bots)
	agent, text, ok := c.agentFor(text, mentioned)
	if !ok {
		return envelope.Tag{}, false
	}

	// The thread the answer belongs in: the mention's own thread if it is in one,
	// otherwise the mention itself, so the answer opens a thread under it rather
	// than adding a line to the channel.
	thread := ev.ThreadTS
	if thread == "" {
		thread = ev.TS
	}
	source := address.Address{
		Connector: Name,
		Workspace: env.TeamID,
		Path:      []string{ev.Channel},
	}
	if thread != "" {
		source = source.WithParam("thread", thread)
	}

	id := env.EventID
	if id == "" {
		// Slack always sends event_id for the Events API. If a proxy stripped
		// it, the channel and the event timestamp still identify the event
		// uniquely, which keeps redelivery idempotent.
		id = ev.Channel + ":" + firstNonEmpty(ev.EventTS, ev.TS)
	}

	return envelope.Tag{
		ID:      id,
		Agent:   agent,
		Origin:  Name,
		Source:  source,
		Text:    text,
		Actor:   envelope.Actor{ID: ev.User},
		Deliver: c.cfg.Deliver,
		Meta:    meta(env, thread),
		At:      observedAt(env),
	}, true
}

// agentFor resolves which agent was tagged. See the package doc for why the
// text-based step is opt-in.
func (c *Connector) agentFor(text string, mentioned []string) (agent, stripped string, ok bool) {
	if len(mentioned) > 0 {
		agent = mentioned[0]
		return agent, mention.Strip(text, agent), true
	}
	if c.cfg.Agents != nil {
		if agent, stripped, ok = mention.Find(text, c.cfg.Agents); ok {
			return agent, stripped, true
		}
	}
	if c.cfg.DefaultAgent != "" {
		return c.cfg.DefaultAgent, mention.Strip(text, c.cfg.DefaultAgent), true
	}
	return "", "", false
}

// meta carries the Slack context an agent's tools need. Keys are flat strings
// because Tag.Meta is a string map by design: it is context for a tool call, not
// a second payload format.
func meta(env eventEnvelope, thread string) map[string]string {
	m := map[string]string{
		"slack_team":    env.TeamID,
		"slack_channel": env.Event.Channel,
		"slack_user":    env.Event.User,
		"slack_event":   env.Event.Type,
		"slack_ts":      env.Event.TS,
	}
	if thread != "" {
		m["slack_thread_ts"] = thread
	}
	if env.Event.ChannelType != "" {
		m["slack_channel_type"] = env.Event.ChannelType
	}
	if env.APIAppID != "" {
		m["slack_app_id"] = env.APIAppID
	}
	return m
}

// observedAt is when Slack saw the event. The message timestamp is preferred
// over the envelope's whole-second event_time because it has microsecond
// resolution and is the same value Slack orders the channel by.
func observedAt(env eventEnvelope) time.Time {
	if t, ok := parseTS(firstNonEmpty(env.Event.EventTS, env.Event.TS)); ok {
		return t
	}
	if env.EventTime > 0 {
		return time.Unix(env.EventTime, 0).UTC()
	}
	return time.Time{}
}

// parseTS reads a Slack timestamp, "<seconds>.<microseconds>".
func parseTS(ts string) (time.Time, bool) {
	if ts == "" {
		return time.Time{}, false
	}
	secs, frac, _ := strings.Cut(ts, ".")
	sec, err := strconv.ParseInt(secs, 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	var nsec int64
	if frac != "" {
		micro, err := strconv.ParseInt(frac, 10, 64)
		if err != nil {
			return time.Time{}, false
		}
		// Slack's fraction is six digits; pad or truncate rather than assuming.
		for i := len(frac); i < 6; i++ {
			micro *= 10
		}
		nsec = micro * 1000
	}
	return time.Unix(sec, nsec).UTC(), true
}

// decode turns Slack's wire text into text a model can read, and reports which
// agents were mentioned, in the order they appear.
//
// Slack sends entity-escaped text with its own markup for anything that refers
// to something: <@U123> for a user, <#C123|general> for a channel,
// <https://x|label> for a link, <!here> for a broadcast. A model reading the raw
// form learns opaque ids and stray angle brackets, so each construct is reduced
// to what a human sees — except a mention of an agent, which is reduced to
// "@name" so that mention stripping can then remove it the same way it does on
// every other surface.
func decode(raw string, bots map[string]string) (text string, mentioned []string) {
	var b strings.Builder
	b.Grow(len(raw))

	for i := 0; i < len(raw); {
		if raw[i] != '<' {
			b.WriteByte(raw[i])
			i++
			continue
		}
		end := strings.IndexByte(raw[i:], '>')
		if end < 0 {
			// An unbalanced "<" is literal text, not markup.
			b.WriteString(raw[i:])
			break
		}
		token := raw[i+1 : i+end]
		i += end + 1

		body, label, hasLabel := strings.Cut(token, "|")
		switch {
		case strings.HasPrefix(body, "@"):
			id := body[1:]
			if agent, ok := bots[id]; ok {
				mentioned = append(mentioned, agent)
				b.WriteString("@" + agent)
				continue
			}
			if hasLabel && label != "" {
				b.WriteString("@" + label)
				continue
			}
			// An unresolved user id is still information: someone was named. It
			// is written as "@<id>", which no agent name set will match.
			b.WriteString("@" + id)
		case strings.HasPrefix(body, "#"):
			if hasLabel && label != "" {
				b.WriteString("#" + label)
				continue
			}
			b.WriteString("#" + body[1:])
		case strings.HasPrefix(body, "!"):
			// A broadcast: <!here>, <!channel>, <!subteam^S123|@team>.
			if hasLabel && label != "" {
				b.WriteString(label)
				continue
			}
			b.WriteString("@" + body[1:])
		default:
			// A link. The label is what the human saw; without one, the URL is.
			if hasLabel && label != "" {
				b.WriteString(label)
				continue
			}
			b.WriteString(body)
		}
	}

	return unescape(strings.TrimSpace(b.String())), mentioned
}

// unescape reverses the three entities Slack escapes on the way in. It runs
// after markup reduction so that a "&gt;" in the text cannot be mistaken for the
// end of a markup token.
func unescape(s string) string {
	if !strings.Contains(s, "&") {
		return s
	}
	s = strings.ReplaceAll(s, "&lt;", "<")
	s = strings.ReplaceAll(s, "&gt;", ">")
	// Ampersand last: doing it first would turn "&amp;lt;" into "<".
	return strings.ReplaceAll(s, "&amp;", "&")
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
