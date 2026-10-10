package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strconv"
	"time"
)

// Headers this connector puts on every outbound POST.
//
// They are mandatum's own names rather than a provider's, because mandatum is the
// sender here. The signing scheme underneath them is not mandatum's own: see
// Sign.
const (
	// HeaderSignature carries "v0=" followed by the hex digest.
	HeaderSignature = "X-Mandatum-Signature"

	// HeaderTimestamp carries the unix seconds that are inside the signed base
	// string. A receiver reads it to bound the replay window and MUST use this
	// value when recomputing the digest, never its own clock.
	HeaderTimestamp = "X-Mandatum-Timestamp"

	// HeaderDelivery identifies one event exactly: "<run id>/<seq>". It is the
	// dedupe key. Delivery is at-least-once and the router retries, so a
	// receiver will see the same delivery id more than once and must treat the
	// second as a no-op.
	HeaderDelivery = "X-Mandatum-Delivery"

	// HeaderEvent carries the event kind, so a receiver can route on a header
	// rather than parsing a body it is about to discard.
	HeaderEvent = "X-Mandatum-Event"

	// HeaderTopic carries the bus topic the event was published on.
	HeaderTopic = "X-Mandatum-Topic"
)

// Version is the signing scheme version that prefixes the base string and the
// digest. It exists so that a future scheme can be introduced without a receiver
// having to guess which one it is looking at.
const Version = "v0"

// Sign returns the value for HeaderSignature: the scheme version, then the hex
// HMAC-SHA256 of the base string
//
//	"v0:" + <unix seconds> + ":" + <exact request body>
//
// under secret.
//
// # Why this base string, and why a timestamp is in it
//
// It is byte-for-byte the scheme pkg/signature's Slack verifier checks, so a
// receiver can authenticate mandatum with a verifier it already has. That
// compatibility is the reason to reuse it rather than invent one; the reason it
// is worth reusing is the timestamp.
//
// pkg/signature documents the asymmetry honestly: Slack signs a timestamp and
// gets a replay window out of it, while GitHub signs the body alone and a
// captured GitHub request stays valid for as long as the secret does. Inbound,
// we have no say — a sender's scheme is the sender's. Outbound we do, so we take
// the better one. Because the timestamp is INSIDE the digest it cannot be edited
// without invalidating the signature, which is what lets a receiver refuse
// anything older than a few minutes and turn a captured request into a captured
// request that expires.
//
// The digest covers the exact bytes of the body, so a receiver must verify
// before it parses. See the pkg/signature package doc for why that ordering is
// the security property and not a style preference.
func Sign(secret string, at time.Time, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(Version))
	_, _ = mac.Write([]byte{':'})
	_, _ = mac.Write([]byte(timestamp(at)))
	_, _ = mac.Write([]byte{':'})
	_, _ = mac.Write(body)
	return Version + "=" + hex.EncodeToString(mac.Sum(nil))
}

// timestamp renders the instant that goes into both the base string and the
// header. One function, so the two can never disagree — a signature over a
// timestamp the receiver was not sent is a signature that never verifies.
func timestamp(at time.Time) string { return strconv.FormatInt(at.Unix(), 10) }

// sign stamps the signature and timestamp headers onto a request.
//
// It is called per attempt rather than once per delivery: a retry that goes out
// four minutes after the first attempt must carry a fresh timestamp, or the
// receiver's replay window would reject exactly the deliveries that most needed
// to arrive.
func sign(h http.Header, secret string, at time.Time, body []byte) {
	h.Set(HeaderTimestamp, timestamp(at))
	h.Set(HeaderSignature, Sign(secret, at, body))
}
