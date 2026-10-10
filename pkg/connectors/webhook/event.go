package webhook

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/urmzd/mandatum/pkg/envelope"
)

// Body is the JSON an outbound POST carries: one envelope.Event, as a program
// wants to read it.
//
// It is a hand-written struct rather than json.Marshal of the Event itself, and
// the reason is Payload. Event.Payload is []byte, so marshalling the Event
// directly would base64-encode it — including the overwhelmingly common case
// where it is already JSON, which would make every receiver decode a string to
// find an object. Splitting the two encodings into two fields means a receiver
// reads body.payload.text and never has to guess.
//
// The field names are deliberately identical to the SSE stream's data frames
// (see internal/server). A receiver that has parsed one has parsed the other,
// and there is exactly one JSON shape for an mandatum event leaving the mesh.
type Body struct {
	Seq    uint64 `json:"seq"`
	Topic  string `json:"topic"`
	RunID  string `json:"run_id"`
	Tenant string `json:"tenant"`
	Agent  string `json:"agent"`
	Rev    int    `json:"rev"`
	Origin string `json:"origin"`
	Kind   string `json:"kind"`

	// Payload carries a JSON payload inline.
	Payload json.RawMessage `json:"payload,omitempty"`
	// PayloadB64 carries a payload that is not JSON. The two fields are
	// distinct rather than one polymorphic field so a consumer never has to
	// guess which encoding it received.
	PayloadB64 []byte `json:"payload_b64,omitempty"`

	At time.Time `json:"at"`
}

// BodyOf renders an event for the wire.
func BodyOf(e envelope.Event) Body {
	b := Body{
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
		b.Payload = json.RawMessage(e.Payload)
	default:
		b.PayloadB64 = e.Payload
	}
	return b
}

// DeliveryID identifies one event exactly, and is what a receiver dedupes on.
//
// Run plus sequence number rather than a fresh uuid per attempt: the whole point
// is that a retry carries the SAME id as the attempt it repeats, so the receiver
// can recognise it. A uuid would be unique and useless.
func DeliveryID(e envelope.Event) string {
	return e.RunID + "/" + strconv.FormatUint(e.Seq, 10)
}
