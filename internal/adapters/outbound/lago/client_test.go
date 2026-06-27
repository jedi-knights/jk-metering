package lago_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	lagoadapter "github.com/jedi-knights/jk-metering/internal/adapters/outbound/lago"
	"github.com/jedi-knights/jk-metering/internal/domain"
)

func sampleEvent() domain.LagoEvent {
	return domain.LagoEvent{
		TransactionID:          "01J7M3X9TEST0000000000000",
		ExternalSubscriptionID: "user-omar",
		Code:                   "usage",
		Timestamp:              1750000000,
		Properties: map[string]any{
			"event_type": "tool_invoked",
		},
	}
}

func TestPushEvent_PostsEnvelopedWrapper(t *testing.T) {
	var gotBody []byte
	var gotAuth, gotContentType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		gotBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := lagoadapter.New(srv.URL, "test-key", srv.Client())
	if err := client.PushEvent(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotAuth != "Bearer test-key" {
		t.Errorf("Authorization = %q, want Bearer test-key", gotAuth)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	var wrapper domain.LagoEventWrapper
	if err := json.Unmarshal(gotBody, &wrapper); err != nil {
		t.Fatalf("body not JSON-decodable: %v", err)
	}
	if wrapper.Event.TransactionID != sampleEvent().TransactionID {
		t.Errorf("body event transaction_id = %q, want %q",
			wrapper.Event.TransactionID, sampleEvent().TransactionID)
	}
}

func TestPushEvent_Non2xxReturnsAPIError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"error":"invalid_event"}`))
	}))
	defer srv.Close()

	client := lagoadapter.New(srv.URL, "test-key", srv.Client())
	err := client.PushEvent(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected error on 422")
	}
	var apiErr *lagoadapter.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected *APIError, got %T: %v", err, err)
	}
	if apiErr.Status != http.StatusUnprocessableEntity {
		t.Errorf("status = %d, want 422", apiErr.Status)
	}
	if !strings.Contains(apiErr.Body, "invalid_event") {
		t.Errorf("expected body to mention invalid_event, got %q", apiErr.Body)
	}
}

func TestNew_EmptyBaseURLPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = lagoadapter.New("", "test-key", nil)
}

func TestNew_EmptyAPIKeyPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = lagoadapter.New("https://example.com", "", nil)
}
