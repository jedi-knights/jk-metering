// Package ingest hosts the HTTP entry point that lets web apps and
// SPAs emit billable events without importing go-platform/audit
// directly. It validates bearer tokens against the identity-platform-go
// JWKS, derives the audit envelope's trusted fields from the token, and
// emits through the same go-platform/audit pipeline that backend
// services use — so an event posted via HTTP is indistinguishable from
// one written in-process to the metering shim downstream.
package ingest

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// jwkSet is the JSON shape of a JWKS document (RFC 7517 §5). Fields
// other than the ones we need are ignored — pgx is forgiving and so is
// json.Unmarshal here.
type jwkSet struct {
	Keys []jwk `json:"keys"`
}

type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	N   string `json:"n"`
	E   string `json:"e"`
	Alg string `json:"alg,omitempty"`
	Use string `json:"use,omitempty"`
}

// JWKSFetcher resolves a kid to an RSA public key by fetching the JWKS
// document at the configured URL. A successful fetch is cached for
// [JWKSFetcher.cacheTTL]; subsequent lookups within that window do not
// hit the network. On a cache miss the cached set is refreshed and the
// kid is looked up again — handles auth-server key rotation without
// requiring a process restart.
type JWKSFetcher struct {
	url      string
	client   *http.Client
	cacheTTL time.Duration

	mu        sync.Mutex
	cached    map[string]*rsa.PublicKey
	expiresAt time.Time
}

// NewJWKSFetcher constructs a fetcher pointing at the JWKS URL.
//
// A nil http.Client falls back to [http.DefaultClient]; a non-positive
// cacheTTL falls back to one hour.
func NewJWKSFetcher(jwksURL string, client *http.Client, cacheTTL time.Duration) *JWKSFetcher {
	if jwksURL == "" {
		panic("ingest: NewJWKSFetcher called with empty URL")
	}
	if client == nil {
		client = http.DefaultClient
	}
	if cacheTTL <= 0 {
		cacheTTL = time.Hour
	}
	return &JWKSFetcher{url: jwksURL, client: client, cacheTTL: cacheTTL}
}

// KeyByID returns the RSA public key for the given kid, refreshing the
// JWKS cache when the key is absent or the cache has expired. Returns
// an error when the kid is not present even in a freshly fetched set —
// the caller treats this as token-validation failure.
func (f *JWKSFetcher) KeyByID(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	f.mu.Lock()
	key, ok := f.cached[kid]
	expired := time.Now().After(f.expiresAt)
	f.mu.Unlock()
	if ok && !expired {
		return key, nil
	}
	// Either the key is missing or the cache is stale. Re-fetch.
	if err := f.refresh(ctx); err != nil {
		return nil, err
	}
	f.mu.Lock()
	key, ok = f.cached[kid]
	f.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("ingest/jwks: key id %q not found in JWKS", kid)
	}
	return key, nil
}

// refresh fetches the JWKS document and replaces the cache. Other
// goroutines see the new keys atomically — the swap happens under the
// mutex after a successful parse.
func (f *JWKSFetcher) refresh(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, nil)
	if err != nil {
		return fmt.Errorf("ingest/jwks: building request: %w", err)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return fmt.Errorf("ingest/jwks: fetching %s: %w", f.url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("ingest/jwks: %s returned %d", f.url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("ingest/jwks: reading body: %w", err)
	}
	var set jwkSet
	if err := json.Unmarshal(body, &set); err != nil {
		return fmt.Errorf("ingest/jwks: decoding JWKS: %w", err)
	}
	cached := make(map[string]*rsa.PublicKey, len(set.Keys))
	for _, k := range set.Keys {
		if k.Kty != "RSA" || k.N == "" || k.E == "" {
			continue
		}
		pub, err := rsaPublicKeyFromJWK(k)
		if err != nil {
			return fmt.Errorf("ingest/jwks: decoding key %q: %w", k.Kid, err)
		}
		cached[k.Kid] = pub
	}
	if len(cached) == 0 {
		return errors.New("ingest/jwks: no usable RSA keys in JWKS document")
	}
	f.mu.Lock()
	f.cached = cached
	f.expiresAt = time.Now().Add(f.cacheTTL)
	f.mu.Unlock()
	return nil
}

// rsaPublicKeyFromJWK decodes the modulus + exponent of a JWK into a
// crypto/rsa.PublicKey per RFC 7518 §6.3.1. base64url, no padding.
func rsaPublicKeyFromJWK(k jwk) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("decoding n: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("decoding e: %w", err)
	}
	e := new(big.Int).SetBytes(eBytes)
	if !e.IsInt64() {
		return nil, fmt.Errorf("exponent out of range")
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: int(e.Int64()),
	}, nil
}
