package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/jedi-knights/go-platform/audit"
)

// instrumentationName identifies this package's OTel instrumentation
// scope. The global MeterProvider resolves it; no provider installed
// (tests) yields a no-op meter so the Add calls below are
// side-effect-free.
const instrumentationName = "github.com/jedi-knights/jk-metering/internal/ingest"

// MaxBodyBytes caps the request body. 64 KiB is generous for a single
// metering event and tight enough that a malicious client cannot exhaust
// memory by streaming a huge attrs payload.
const MaxBodyBytes = 64 * 1024

// Request is the shape callers POST to /metering/events. The handler
// trusts only event_type, resource_kind, resource_id, resource_parent,
// resource_path, and attrs from this body — every other audit field is
// derived from the authenticated principal or the server clock, so a
// compromised client cannot impersonate another subject or backdate
// events.
type Request struct {
	EventType      string         `json:"event_type"`
	ResourceKind   string         `json:"resource_kind"`
	ResourceID     string         `json:"resource_id"`
	ResourceParent string         `json:"resource_parent"`
	ResourcePath   string         `json:"resource_path"`
	Action         string         `json:"action,omitempty"`
	Attrs          map[string]any `json:"attrs,omitempty"`
}

// Handler emits one ADR-0018 audit event per accepted request through
// the supplied [audit.Emitter]. The same durable sink the backend
// identity services write to is shared here, so a metering event posted
// via HTTP is indistinguishable from one written in-process.
type Handler struct {
	emitter audit.Emitter
	service string
	logger  *slog.Logger

	// eventsAccepted counts every request that reaches the 202 branch,
	// labeled by event_code (the Request.EventType the client sent).
	// Lets dashboards graph ingest volume per billable-event-type
	// without reading the audit table.
	eventsAccepted metric.Int64Counter
}

// NewHandler constructs the HTTP handler. A nil emitter panics —
// composition errors are loud at startup. service is stamped on
// Event.Service for every emitted event so downstream consumers can
// recognize the ingest path. A nil logger falls back to the default
// slog.Default().
func NewHandler(emitter audit.Emitter, service string, logger *slog.Logger) *Handler {
	if emitter == nil {
		panic("ingest: NewHandler called with nil emitter")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if service == "" {
		service = "jk-metering-ingest"
	}
	return &Handler{
		emitter:        emitter,
		service:        service,
		logger:         logger,
		eventsAccepted: newEventsAcceptedCounter(),
	}
}

// newEventsAcceptedCounter builds the ingest-accepted counter from
// the global MeterProvider. OTel's returned counter is always
// usable (no-op in the error path), so the error is intentionally
// dropped — a scrape silently returns no series until the provider
// is installed at startup.
func newEventsAcceptedCounter() metric.Int64Counter {
	meter := otel.Meter(instrumentationName)
	counter, _ := meter.Int64Counter(
		"metering.events_accepted",
		metric.WithDescription("Ingest requests that reached the 202 accepted branch, by event_code."),
		metric.WithUnit("{event}"),
	)
	return counter
}

// ServeHTTP implements POST /metering/events.
//
//   - 401 on missing / invalid auth (handled by the middleware before
//     this handler runs).
//   - 400 on malformed JSON, missing required fields, or a
//     resource_path the principal is not authorized to emit for.
//   - 500 on durable-sink failure — per ADR-0019's paid-event policy a
//     metering event whose audit row could not be persisted must not
//     be silently dropped.
//   - 202 on success. The handler returns immediately after the audit
//     emit so the client is not blocked on downstream metering shim
//     and Lago processing.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	principal := h.requirePrincipal(w, r)
	if principal == nil {
		return
	}
	req, ok := h.readAndValidateRequest(w, r, principal)
	if !ok {
		return
	}
	if err := h.emitter.Emit(r.Context(), buildAuditEvent(req, principal, h.service)); err != nil {
		h.logger.Error("emit failed", "error", err)
		http.Error(w, "audit emit failed", http.StatusInternalServerError)
		return
	}
	h.recordAccepted(r.Context(), req.EventType)
	w.WriteHeader(http.StatusAccepted)
}

// recordAccepted increments the metering.events_accepted counter
// with the event_code label. Nil-safe: the counter is a no-op when
// no MeterProvider is installed (tests).
func (h *Handler) recordAccepted(ctx context.Context, eventType string) {
	if h.eventsAccepted == nil {
		return
	}
	h.eventsAccepted.Add(ctx, 1, metric.WithAttributes(attribute.String("event_code", eventType)))
}

// requirePrincipal pulls the auth-middleware-populated principal from
// the context. A missing principal is a wiring bug (auth middleware
// not installed on the route); log and 500 rather than exposing the
// endpoint unauthenticated.
func (h *Handler) requirePrincipal(w http.ResponseWriter, r *http.Request) *Principal {
	principal := PrincipalFromContext(r.Context())
	if principal == nil {
		h.logger.Error("missing principal on context — auth middleware not wired")
		http.Error(w, "internal error", http.StatusInternalServerError)
		return nil
	}
	return principal
}

// readAndValidateRequest parses the JSON body, runs contract validation,
// and enforces the emit-scope check. Writes the appropriate 4xx status
// and returns ok=false on any failure so the caller can return.
func (h *Handler) readAndValidateRequest(w http.ResponseWriter, r *http.Request, principal *Principal) (Request, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	var req Request
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeBadRequest(w, decodeErrorMessage(err))
		return Request{}, false
	}
	if err := req.Validate(); err != nil {
		writeBadRequest(w, err.Error())
		return Request{}, false
	}
	if !principalCanEmit(principal, req.ResourceParent) {
		http.Error(w, "forbidden", http.StatusForbidden)
		return Request{}, false
	}
	return req, true
}

// buildAuditEvent projects the ingest Request + auth Principal into the
// go-platform/audit envelope. Extracted so ServeHTTP stays under the
// gocyclo cap; the field mapping here is stable and best read as one
// dense block.
func buildAuditEvent(req Request, principal *Principal, service string) audit.Event {
	event := audit.Event{
		EventType:      req.EventType,
		Service:        service,
		ActorType:      audit.ActorType(principal.ActorType),
		ActorID:        principal.ActorID,
		SubjectID:      principal.SubjectID,
		ClientID:       principal.ClientID,
		Resource:       composeResource(req),
		ResourceKind:   audit.ResourceKind(req.ResourceKind),
		ResourceID:     req.ResourceID,
		ResourceParent: req.ResourceParent,
		ResourcePath:   req.ResourcePath,
		Action:         req.Action,
		Decision:       audit.DecisionAllow,
		Attrs:          req.Attrs,
	}
	if event.Action == "" {
		event.Action = "use"
	}
	return event
}

// Validate returns the first contract violation on the request body, or
// nil if the request is well-formed. Required fields are the ones a
// downstream Lago filter cannot reconstruct (event_type, resource_kind,
// resource_path); optional fields default at the handler.
func (r Request) Validate() error {
	if r.EventType == "" {
		return errors.New("event_type is required")
	}
	if r.ResourceKind == "" {
		return errors.New("resource_kind is required")
	}
	if r.ResourceParent == "" {
		return errors.New("resource_parent is required")
	}
	if r.ResourcePath == "" {
		return errors.New("resource_path is required")
	}
	// resource_path must begin with resource_parent so a single Lago
	// prefix filter can address an entire surface — otherwise the
	// taxonomy guarantees in ADR-0019 break silently.
	if !strings.HasPrefix(r.ResourcePath, r.ResourceParent+"/") &&
		r.ResourcePath != r.ResourceParent {
		return fmt.Errorf("resource_path %q must begin with resource_parent %q",
			r.ResourcePath, r.ResourceParent)
	}
	return nil
}

// principalCanEmit gates which resource_parent values a token may emit
// for. Scopes carry the parent name in the form metering:emit:<parent>;
// the bare metering:emit scope authorizes emission for any parent and
// is intended for service-to-service callers.
func principalCanEmit(p *Principal, parent string) bool {
	want := "metering:emit:" + parent
	for _, s := range p.Scopes {
		if s == want || s == "metering:emit" {
			return true
		}
	}
	return false
}

// composeResource builds the human-readable resource string from the
// taxonomy fields. Mirrors the convention backend services use:
// "<kind>:<id>" when an id is present; "<kind>" otherwise.
func composeResource(r Request) string {
	if r.ResourceID == "" {
		return r.ResourceKind
	}
	return r.ResourceKind + ":" + r.ResourceID
}

// writeBadRequest is a tiny convenience that always sets the cache
// header for error responses — RFC 6749 §5.1 conventions are not a
// requirement here but consistency with the rest of the platform keeps
// log noise down.
func writeBadRequest(w http.ResponseWriter, msg string) {
	w.Header().Set("Cache-Control", "no-store")
	http.Error(w, msg, http.StatusBadRequest)
}

// decodeErrorMessage returns a human-friendly description of a JSON
// decode error without leaking internal type information.
func decodeErrorMessage(err error) string {
	if errors.Is(err, io.EOF) {
		return "request body is empty"
	}
	var maxBytes *http.MaxBytesError
	if errors.As(err, &maxBytes) {
		return "request body exceeds maximum size"
	}
	return "malformed JSON body"
}
