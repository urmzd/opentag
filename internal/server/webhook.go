package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/urmzd/dispatch/pkg/metrics"
	"github.com/urmzd/opentag/pkg/address"
	"github.com/urmzd/opentag/pkg/envelope"
	"github.com/urmzd/opentag/pkg/signature"
)

// Webhook ingress is the untrusted edge of the system: anyone who learns the URL
// can POST to it, so the HMAC over the raw body is the only evidence that Slack
// or GitHub sent it. Everything in this file is arranged around doing that check
// first and doing it on the exact bytes received.
//
// The order below is not a style choice. Read the body, authenticate the body,
// and only then let anything interpret it. A handler that reads one field of an
// unverified payload — "it's just the event type" — has already acted on
// attacker-controlled input, whatever it does afterwards.
//
// The tenant of a webhook tag comes from the Ingress it was registered under,
// never from the payload. A webhook presents no credential of its own; its
// shared secret is its credential, and the secret is registered against exactly
// one tenant. So a payload claiming a tenant claims nothing.

// Inbound is what a verified request meant.
//
// Reply exists because some surfaces handshake before they trigger anything:
// Slack's url_verification wants its challenge echoed, GitHub's ping wants a
// 200. Both are authentic requests that produce no run, and neither is an error.
type Inbound struct {
	// Tags are the tags this request raised. Empty is normal: a handshake or an
	// event that names no agent produces none.
	Tags []envelope.Tag
	// Reply is a body the connector requires in the response. Nil answers 202
	// Accepted with the run ids.
	Reply []byte
	// ReplyType is Reply's content type. Empty means text/plain.
	ReplyType string
}

// TagFunc converts an authenticated request into tags.
//
// It is called only after the body's signature has been verified, and it is the
// connector's translation step: only the connector knows its own payload shape,
// its own mention syntax, and which of its events name an agent at all.
//
// It must not read a tenant from the body. The server assigns one.
type TagFunc func(h http.Header, body []byte) (Inbound, error)

// Ingress registers one connector's inbound webhook, served at
// POST /v1/webhooks/{connector}.
type Ingress struct {
	// Connector names the endpoint and becomes every tag's origin. It is
	// restricted to the address scheme charset, because it is the same name the
	// connector owns in the address namespace.
	Connector string

	// Tenant is the authorization scope for every tag from this endpoint. It is
	// a property of the registration, because a webhook has no credential to
	// derive one from. Empty is the single-tenant deployment.
	Tenant string

	// Verifier authenticates the raw body. Required: an ingress without one
	// would authenticate the internet.
	Verifier signature.Verifier

	// Tags converts an authenticated body into tags. Required.
	Tags TagFunc
}

func (i Ingress) validate() error {
	if i.Connector == "" {
		return fmt.Errorf("%w: ingress has no connector name", ErrInvalid)
	}
	if err := address.ValidWorkspace(i.Connector); err != nil {
		return fmt.Errorf("%w: ingress connector %q is not a usable name: %w", ErrInvalid, i.Connector, err)
	}
	if i.Verifier == nil {
		return fmt.Errorf("%w: ingress %q has no signature verifier, and an unverified webhook endpoint accepts anything", ErrInvalid, i.Connector)
	}
	if i.Tags == nil {
		return fmt.Errorf("%w: ingress %q has no tag conversion", ErrInvalid, i.Connector)
	}
	return nil
}

// accepted is the response body for a webhook that raised runs. It reports one
// entry per tag so a redelivery can be recognised by the sender: the same tag id
// comes back with the same run id.
type accepted struct {
	Accepted []acceptedRun `json:"accepted"`
}

type acceptedRun struct {
	TagID string `json:"tag_id"`
	RunID string `json:"run_id"`
	Rev   int    `json:"rev"`
	Topic string `json:"topic"`
}

// handleWebhook serves POST /v1/webhooks/{connector}.
func (s *Server) handleWebhook(invoker Invoker) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		connector := r.PathValue("connector")
		in, ok := s.ingress[connector]
		if !ok {
			s.webhookResult(connector, "unknown")
			http.Error(w, "no webhook is registered for this connector", http.StatusNotFound)
			return
		}
		if invoker == nil {
			s.webhookResult(connector, "unimplemented")
			s.httpFail(w, "webhook", fmt.Errorf("%w: no runtime is configured", ErrUnimplemented))
			return
		}

		// The body must be buffered whole before it can be authenticated, which
		// is exactly why it is capped.
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.maxBody))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				s.webhookResult(connector, "too_large")
				http.Error(w, "webhook body exceeds the configured limit", http.StatusRequestEntityTooLarge)
				return
			}
			s.webhookResult(connector, "unreadable")
			http.Error(w, "webhook body could not be read", http.StatusBadRequest)
			return
		}

		// Authenticate before anything parses, reads, or logs the body.
		if err := in.Verifier.Verify(r.Header, body); err != nil {
			status := verificationStatus(err)
			s.log.Warn("server: webhook verification failed",
				"connector", connector, "status", status, "error", err)
			s.webhookResult(connector, "unverified")
			http.Error(w, verificationMessage(status), status)
			return
		}

		inbound, err := in.Tags(r.Header, body)
		if err != nil {
			s.webhookResult(connector, "unconvertible")
			s.httpFail(w, "webhook", fmt.Errorf("%w: %w", ErrInvalid, err))
			return
		}

		out := accepted{Accepted: make([]acceptedRun, 0, len(inbound.Tags))}
		for _, tag := range inbound.Tags {
			// Tenant and origin are the server's, not the payload's. Origin is
			// overwritten rather than defaulted: provenance is which endpoint
			// the bytes arrived on, which is something we know and the body
			// only claims.
			tag.Tenant = in.Tenant
			tag.Origin = in.Connector
			if tag.At.IsZero() {
				tag.At = s.now()
			}
			if err := tag.Validate(); err != nil {
				s.webhookResult(connector, "invalid_tag")
				s.httpFail(w, "webhook", err)
				return
			}
			// The same accept path as the Invoke RPCs, so a webhook and an RPC
			// acknowledge a tag with the same run id, revision and topic.
			run, err := s.accept(r.Context(), invoker, tag)
			if err != nil {
				s.webhookResult(connector, "rejected")
				s.httpFail(w, "webhook", err)
				return
			}
			out.Accepted = append(out.Accepted, acceptedRun{
				TagID: tag.ID,
				RunID: run.RunID,
				Rev:   run.Rev,
				Topic: run.Topic.String(),
			})
		}

		// A handshake answers exactly what the connector asked for; anything
		// else answers 202, because the runs are durable but nothing has run
		// yet.
		if inbound.Reply != nil {
			s.webhookResult(connector, "handshake")
			contentType := inbound.ReplyType
			if contentType == "" {
				contentType = "text/plain; charset=utf-8"
			}
			w.Header().Set("Content-Type", contentType)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(inbound.Reply)
			return
		}
		s.webhookResult(connector, "accepted")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		if err := json.NewEncoder(w).Encode(out); err != nil {
			s.log.Warn("server: webhook response could not be written", "connector", connector, "error", err)
		}
	}
}

func (s *Server) webhookResult(connector, result string) {
	s.metrics.Count(MetricWebhooks, 1,
		metrics.Label{Key: "connector", Value: connector},
		metrics.Label{Key: "result", Value: result},
	)
}

// verificationStatus maps a verification failure onto a status.
//
// The default is 401, not 500: a Verifier that returned an error this package
// does not recognise has still refused the request, and treating an unrecognised
// refusal as our own fault would be the one interpretation that lets the request
// through on a later retry.
func verificationStatus(err error) int {
	switch {
	case errors.Is(err, signature.ErrSignatureMismatch), errors.Is(err, signature.ErrTimestampSkew):
		return http.StatusUnauthorized
	case errors.Is(err, signature.ErrMalformed):
		return http.StatusBadRequest
	case errors.Is(err, signature.ErrNoSecret):
		return http.StatusInternalServerError
	default:
		return http.StatusUnauthorized
	}
}

// verificationMessage answers without saying which check failed. The detail is
// logged; returning it would tell a caller probing the endpoint whether it got
// the digest, the timestamp, or the header format wrong.
func verificationMessage(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "webhook signature headers are malformed"
	case http.StatusInternalServerError:
		return "webhook verification is misconfigured"
	default:
		return "webhook signature could not be verified"
	}
}
