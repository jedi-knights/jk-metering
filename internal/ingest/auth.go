package ingest

import (
	"context"
	"crypto/rsa"
	"errors"
	"net/http"
	"strings"

	"github.com/jedi-knights/go-platform/jwtutil"
)

// principalContextKey scopes the parsed principal on the request context
// so the handler can read it after the middleware authenticates.
type principalContextKey struct{}

// Principal carries the fields the handler needs from a validated bearer
// token: actor classification (ADR-0015), the subject the token
// represents, and the client_id that obtained the token. These become
// the trusted server-injected fields on the audit envelope; the client
// cannot override them via the request body.
type Principal struct {
	ActorType string
	ActorID   string
	SubjectID string
	ClientID  string
	Scopes    []string
}

// PrincipalFromContext extracts the [Principal] populated by [Middleware].
// Returns nil when the middleware was bypassed (e.g. on an unauthenticated
// route); the handler should treat this as a programming error.
func PrincipalFromContext(ctx context.Context) *Principal {
	p, _ := ctx.Value(principalContextKey{}).(*Principal)
	return p
}

// WithPrincipal returns ctx with p stamped on it under the same key
// [Middleware] uses. Exported so tests can drive [Handler] without
// constructing a real JWT, and so callers that have already
// authenticated upstream (e.g. behind an authenticated proxy) can hand
// the [Handler] a pre-resolved principal.
func WithPrincipal(ctx context.Context, p *Principal) context.Context {
	return context.WithValue(ctx, principalContextKey{}, p)
}

// HandlerWithPrincipal wraps next in a small middleware that stamps p
// onto every request context. Intended for tests that want to drive
// the [Handler] without standing up [Middleware] and a fake JWKS.
func HandlerWithPrincipal(next http.Handler, p *Principal) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(WithPrincipal(r.Context(), p)))
	})
}

// KeySource is the key-resolution interface the middleware depends on.
// In production this is [*JWKSFetcher.KeyByID]; tests inject a static
// resolver.
type KeySource func(ctx context.Context, kid string) (*rsa.PublicKey, error)

// Middleware returns an http.Handler that validates the Authorization
// header as an RS256 bearer token (RFC 6750), parses the claims via
// [jwtutil.ParseRS256] so type-confusion attacks are rejected at the
// JWT layer, and propagates the resulting [Principal] on the request
// context.
//
// A missing or invalid token returns 401 with a WWW-Authenticate
// challenge per RFC 6750 §3. The expected issuer is enforced when
// non-empty so cross-issuer token reuse is impossible.
func Middleware(keys KeySource, expectedIssuer string, next http.Handler) http.Handler {
	if keys == nil {
		panic("ingest: Middleware called with nil KeySource")
	}
	if next == nil {
		panic("ingest: Middleware called with nil next handler")
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := extractBearer(r.Header.Get("Authorization"))
		if !ok {
			writeUnauthorized(w, "missing bearer token")
			return
		}
		claims, err := jwtutil.ParseRS256(r.Context(), raw, jwtutil.KeySource(keys))
		if err != nil {
			writeUnauthorized(w, "invalid bearer token")
			return
		}
		if expectedIssuer != "" && claims.Issuer != expectedIssuer {
			writeUnauthorized(w, "issuer mismatch")
			return
		}
		p := &Principal{
			ActorType: claims.ActorType,
			ActorID:   firstNonEmpty(claims.AgentID, claims.ClientID, claims.Subject),
			SubjectID: claims.Subject,
			ClientID:  claims.ClientID,
			Scopes:    strings.Fields(claims.Scope),
		}
		ctx := context.WithValue(r.Context(), principalContextKey{}, p)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// extractBearer returns the raw token from an Authorization header.
// Authorization: Bearer <token> is the only accepted shape; any other
// scheme returns ok=false so the middleware writes a 401.
func extractBearer(h string) (string, bool) {
	const prefix = "Bearer "
	if !strings.HasPrefix(h, prefix) {
		return "", false
	}
	raw := strings.TrimSpace(strings.TrimPrefix(h, prefix))
	return raw, raw != ""
}

// writeUnauthorized writes the RFC 6750 §3 401 response. The error
// description is generic on purpose — leaking parse-failure detail to
// callers helps attackers narrow what's missing.
func writeUnauthorized(w http.ResponseWriter, _ string) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="jk-metering-ingest"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// ErrMissingPrincipal is returned by helpers that expect the
// authentication middleware to have populated the context. Surfacing
// this as an error rather than panicking lets tests construct a
// handler-only environment without faking the middleware.
var ErrMissingPrincipal = errors.New("ingest: missing principal on context")
