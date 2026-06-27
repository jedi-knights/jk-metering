package ingest_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jedi-knights/go-platform/jwtutil"

	"github.com/jedi-knights/jk-metering/internal/ingest"
)

func newTestRSAKey(t *testing.T) (*rsa.PrivateKey, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa.GenerateKey: %v", err)
	}
	return key, "test-kid-1"
}

func signClaims(t *testing.T, priv *rsa.PrivateKey, kid string, cfg jwtutil.ClaimsConfig) string {
	t.Helper()
	claims := jwtutil.NewClaims(cfg)
	raw, err := jwtutil.SignRS256(claims, priv, kid)
	if err != nil {
		t.Fatalf("SignRS256: %v", err)
	}
	return raw
}

func captureHandler() (http.Handler, *struct{ called bool }) {
	state := &struct{ called bool }{}
	return http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		state.called = true
	}), state
}

func TestMiddleware_AcceptsValidToken(t *testing.T) {
	priv, kid := newTestRSAKey(t)
	keys := func(_ context.Context, k string) (*rsa.PublicKey, error) {
		if k != kid {
			t.Fatalf("unexpected kid %q", k)
		}
		return &priv.PublicKey, nil
	}
	next, state := captureHandler()
	mw := ingest.Middleware(keys, "https://auth-server.test", next)

	now := time.Now()
	raw := signClaims(t, priv, kid, jwtutil.ClaimsConfig{
		Issuer:    "https://auth-server.test",
		Subject:   "user-omar",
		TokenID:   "tok-1",
		ClientID:  "client-1",
		Scope:     "metering:emit:billpayer",
		IssuedAt:  now,
		ExpiresAt: now.Add(time.Hour),
		ActorType: "user",
	})
	req := httptest.NewRequest(http.MethodPost, "/metering/events", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	w := httptest.NewRecorder()
	mw.ServeHTTP(w, req)

	if !state.called {
		t.Errorf("next handler not called")
	}
}

func TestMiddleware_RejectsMissingToken(t *testing.T) {
	keys := func(_ context.Context, _ string) (*rsa.PublicKey, error) {
		t.Fatal("KeySource should not be called when token is missing")
		return nil, nil
	}
	next, state := captureHandler()
	mw := ingest.Middleware(keys, "", next)

	req := httptest.NewRequest(http.MethodPost, "/metering/events", nil)
	w := httptest.NewRecorder()
	mw.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
	if !contains(w.Header().Get("WWW-Authenticate"), "Bearer") {
		t.Errorf("missing WWW-Authenticate challenge")
	}
	if state.called {
		t.Errorf("next handler should not be called")
	}
}

func TestMiddleware_RejectsWrongIssuer(t *testing.T) {
	priv, kid := newTestRSAKey(t)
	keys := func(_ context.Context, _ string) (*rsa.PublicKey, error) {
		return &priv.PublicKey, nil
	}
	next, _ := captureHandler()
	mw := ingest.Middleware(keys, "https://other-issuer.test", next)

	now := time.Now()
	raw := signClaims(t, priv, kid, jwtutil.ClaimsConfig{
		Issuer:    "https://auth-server.test",
		Subject:   "user-omar",
		TokenID:   "tok-1",
		ClientID:  "client-1",
		Scope:     "metering:emit",
		IssuedAt:  now,
		ExpiresAt: now.Add(time.Hour),
	})
	req := httptest.NewRequest(http.MethodPost, "/metering/events", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	w := httptest.NewRecorder()
	mw.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 on issuer mismatch", w.Code)
	}
}

func TestMiddleware_RejectsTamperedToken(t *testing.T) {
	priv, kid := newTestRSAKey(t)
	keys := func(_ context.Context, _ string) (*rsa.PublicKey, error) {
		return &priv.PublicKey, nil
	}
	next, _ := captureHandler()
	mw := ingest.Middleware(keys, "", next)

	now := time.Now()
	raw := signClaims(t, priv, kid, jwtutil.ClaimsConfig{
		Issuer:    "any",
		Subject:   "user-omar",
		TokenID:   "tok-1",
		ClientID:  "client-1",
		Scope:     "metering:emit",
		IssuedAt:  now,
		ExpiresAt: now.Add(time.Hour),
	})
	// Tamper the signature.
	tampered := raw + "garbage"
	req := httptest.NewRequest(http.MethodPost, "/metering/events", nil)
	req.Header.Set("Authorization", "Bearer "+tampered)
	w := httptest.NewRecorder()
	mw.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 on tampered token", w.Code)
	}
}

func TestMiddleware_PopulatesPrincipalForHandler(t *testing.T) {
	priv, kid := newTestRSAKey(t)
	keys := func(_ context.Context, _ string) (*rsa.PublicKey, error) {
		return &priv.PublicKey, nil
	}

	var captured *ingest.Principal
	final := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = ingest.PrincipalFromContext(r.Context())
	})
	mw := ingest.Middleware(keys, "", final)

	now := time.Now()
	raw := signClaims(t, priv, kid, jwtutil.ClaimsConfig{
		Issuer:    "any",
		Subject:   "user-omar",
		TokenID:   "tok-1",
		ClientID:  "client-1",
		Scope:     "metering:emit:billpayer feature:exporter",
		IssuedAt:  now,
		ExpiresAt: now.Add(time.Hour),
		ActorType: "user",
	})
	req := httptest.NewRequest(http.MethodPost, "/metering/events", nil)
	req.Header.Set("Authorization", "Bearer "+raw)
	mw.ServeHTTP(httptest.NewRecorder(), req)

	if captured == nil {
		t.Fatal("expected principal on context")
	}
	if captured.SubjectID != "user-omar" {
		t.Errorf("subject_id = %q", captured.SubjectID)
	}
	if captured.ClientID != "client-1" {
		t.Errorf("client_id = %q", captured.ClientID)
	}
	if captured.ActorType != "user" {
		t.Errorf("actor_type = %q", captured.ActorType)
	}
	if len(captured.Scopes) != 2 {
		t.Errorf("scopes = %v", captured.Scopes)
	}
}

func TestMiddleware_NilKeySourcePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = ingest.Middleware(nil, "", http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
