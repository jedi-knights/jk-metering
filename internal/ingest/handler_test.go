package ingest_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/jedi-knights/go-platform/audit"

	"github.com/jedi-knights/jk-metering/internal/ingest"
)

type captureSink struct {
	mu     sync.Mutex
	events []audit.Event
	err    error
}

func (c *captureSink) Sink(_ context.Context, e audit.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, e)
	return c.err
}

func (c *captureSink) snapshot() []audit.Event {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]audit.Event, len(c.events))
	copy(out, c.events)
	return out
}

// newTestHandler wires a Handler around a no-op audit.Emitter sink and
// the given Principal so handler-only tests don't have to stand up a
// real JWKS / Middleware pair. The principal is injected via
// [ingest.HandlerWithPrincipal] which uses the same unexported context
// key the production Middleware writes to.
func newTestHandler(t *testing.T, sink audit.Sink, principal *ingest.Principal) http.Handler {
	t.Helper()
	emitter := audit.New(sink)
	handler := ingest.NewHandler(emitter, "jk-metering-ingest", nil)
	return ingest.HandlerWithPrincipal(handler, principal)
}

func TestServeHTTP_HappyPath_Returns202(t *testing.T) {
	sink := &captureSink{}
	h := newTestHandler(t, sink, &ingest.Principal{
		ActorType: "user",
		ActorID:   "user-omar",
		SubjectID: "user-omar",
		Scopes:    []string{"metering:emit:billpayer"},
	})

	body := `{
		"event_type": "feature_used",
		"resource_kind": "feature",
		"resource_id": "pdf_export",
		"resource_parent": "billpayer",
		"resource_path": "billpayer/feature/pdf_export",
		"attrs": {"size_kb": 47}
	}`
	req := httptest.NewRequest(http.MethodPost, "/metering/events",
		strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body = %s", w.Code, w.Body.String())
	}
	events := sink.snapshot()
	if len(events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(events))
	}
	e := events[0]
	if e.EventType != "feature_used" {
		t.Errorf("event_type = %q", e.EventType)
	}
	if e.ActorID != "user-omar" {
		t.Errorf("actor_id = %q, want user-omar (server-injected)", e.ActorID)
	}
	if e.ResourceParent != "billpayer" {
		t.Errorf("resource_parent = %q", e.ResourceParent)
	}
	if e.Service != "jk-metering-ingest" {
		t.Errorf("service = %q", e.Service)
	}
	if e.Attrs["size_kb"] != float64(47) {
		t.Errorf("attrs.size_kb = %v, want 47", e.Attrs["size_kb"])
	}
}

func TestServeHTTP_RejectsForbiddenParent(t *testing.T) {
	sink := &captureSink{}
	h := newTestHandler(t, sink, &ingest.Principal{
		ActorType: "user",
		ActorID:   "user-omar",
		SubjectID: "user-omar",
		Scopes:    []string{"metering:emit:other_app"},
	})
	body := `{"event_type":"feature_used","resource_kind":"feature","resource_id":"x","resource_parent":"billpayer","resource_path":"billpayer/feature/x"}`
	req := httptest.NewRequest(http.MethodPost, "/metering/events",
		strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Errorf("status = %d, want 403", w.Code)
	}
	if len(sink.snapshot()) != 0 {
		t.Error("expected no emission when forbidden")
	}
}

func TestServeHTTP_RejectsBadResourcePathPrefix(t *testing.T) {
	sink := &captureSink{}
	h := newTestHandler(t, sink, &ingest.Principal{
		ActorType: "user", ActorID: "user-omar", SubjectID: "user-omar",
		Scopes: []string{"metering:emit:billpayer"},
	})
	body := `{"event_type":"e","resource_kind":"feature","resource_parent":"billpayer","resource_path":"not_billpayer/feature/x"}`
	req := httptest.NewRequest(http.MethodPost, "/metering/events",
		strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestServeHTTP_RejectsMissingFields(t *testing.T) {
	sink := &captureSink{}
	h := newTestHandler(t, sink, &ingest.Principal{
		ActorType: "user", ActorID: "u", SubjectID: "u",
		Scopes: []string{"metering:emit"},
	})
	tests := []struct {
		name string
		body string
	}{
		{"missing event_type", `{"resource_kind":"feature","resource_parent":"p","resource_path":"p/x"}`},
		{"missing resource_kind", `{"event_type":"e","resource_parent":"p","resource_path":"p/x"}`},
		{"missing resource_parent", `{"event_type":"e","resource_kind":"feature","resource_path":"p/x"}`},
		{"missing resource_path", `{"event_type":"e","resource_kind":"feature","resource_parent":"p"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/metering/events",
				strings.NewReader(tt.body))
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", w.Code)
			}
		})
	}
}

func TestServeHTTP_RejectsNonPOST(t *testing.T) {
	h := newTestHandler(t, &captureSink{}, &ingest.Principal{
		ActorID: "u", SubjectID: "u", Scopes: []string{"metering:emit"},
	})
	req := httptest.NewRequest(http.MethodGet, "/metering/events", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestServeHTTP_BareScopeAllowsAnyParent(t *testing.T) {
	sink := &captureSink{}
	h := newTestHandler(t, sink, &ingest.Principal{
		ActorType: "service", ActorID: "svc", SubjectID: "svc",
		Scopes: []string{"metering:emit"},
	})
	body := `{"event_type":"e","resource_kind":"feature","resource_id":"x","resource_parent":"anything","resource_path":"anything/feature/x"}`
	req := httptest.NewRequest(http.MethodPost, "/metering/events",
		strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("status = %d, want 202", w.Code)
	}
}

func TestServeHTTP_AuditEmitFailureReturns500(t *testing.T) {
	sink := &captureSink{err: errSinkBroken}
	h := newTestHandler(t, sink, &ingest.Principal{
		ActorID: "u", SubjectID: "u",
		Scopes: []string{"metering:emit"},
	})
	body := `{"event_type":"e","resource_kind":"feature","resource_id":"x","resource_parent":"p","resource_path":"p/feature/x"}`
	req := httptest.NewRequest(http.MethodPost, "/metering/events",
		strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("status = %d, want 500", w.Code)
	}
}

func TestNewHandler_NilEmitterPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = ingest.NewHandler(nil, "jk-metering-ingest", nil)
}

// errSinkBroken is shared with the auth/middleware test file.
var errSinkBroken = sinkError("broken")

type sinkError string

func (s sinkError) Error() string { return string(s) }

// _ keeps the json import live for future tests that compare bodies.
var _ = json.Marshal
var _ = bytes.NewReader
