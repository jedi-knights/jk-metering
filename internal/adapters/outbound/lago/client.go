// Package lago provides a [ports.MeterSink] implementation that posts
// events to Lago's Event API. See
// https://docs.getlago.com/api-reference/events/create-an-event.
//
// The client is deliberately small — a single HTTP call per event with
// JSON encoding and bearer-token auth. Bulk ingestion, retry policy,
// and circuit-breaking are deferred to focused follow-ups once the
// shim has real workload pressure to design against.
package lago

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jedi-knights/jk-metering/internal/domain"
	"github.com/jedi-knights/jk-metering/internal/ports"
)

// DefaultPath is Lago's events endpoint relative to the API root.
const DefaultPath = "/api/v1/events"

// DefaultTimeout caps each Lago API call. Lago is typically sub-100ms
// on a healthy region; the 5s ceiling forgives a cold start without
// letting a stuck connection pin the worker.
const DefaultTimeout = 5 * time.Second

// Client is the Lago Event API client.
type Client struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// Compile-time assertion.
var _ ports.MeterSink = (*Client)(nil)

// New constructs a Lago client. baseURL is the API root (e.g.
// https://api.getlago.com or http://lago-api.internal); apiKey is the
// Lago API key. Empty values panic — composition errors are loud at
// startup.
//
// The http.Client is shared across calls; pass [http.DefaultClient]
// when no specific transport tuning is required, or build one with a
// custom timeout and connection pool for production deployments.
func New(baseURL, apiKey string, httpClient *http.Client) *Client {
	if baseURL == "" {
		panic("metering/lago: New called with empty baseURL")
	}
	if apiKey == "" {
		panic("metering/lago: New called with empty apiKey")
	}
	if _, err := url.Parse(baseURL); err != nil {
		panic("metering/lago: New called with invalid baseURL: " + err.Error())
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: DefaultTimeout}
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		apiKey:     apiKey,
		httpClient: httpClient,
	}
}

// PushEvent posts a single event to Lago. Returns an error on any
// non-2xx response; the caller (metering service) treats it as a
// retryable failure and leaves the audit row unconsumed for the next
// tick.
func (c *Client) PushEvent(ctx context.Context, event domain.LagoEvent) error {
	body, err := json.Marshal(domain.LagoEventWrapper{Event: event})
	if err != nil {
		return fmt.Errorf("marshalling event %s: %w", event.TransactionID, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+DefaultPath, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("constructing request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("posting event %s: %w", event.TransactionID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode/100 != 2 {
		// Read up to 1 KiB of the response body for the error message —
		// enough to surface Lago's validation reason without unbounded
		// memory growth on a misbehaving server.
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return &APIError{
			Status:    resp.StatusCode,
			Body:      strings.TrimSpace(string(snippet)),
			EventID:   event.TransactionID,
		}
	}
	return nil
}

// APIError is returned by [Client.PushEvent] when Lago responds with a
// non-2xx status. Status is the HTTP status code; Body is up to 1 KiB
// of the response body. Use [errors.As] to inspect.
type APIError struct {
	Status  int
	Body    string
	EventID string
}

// Error formats the error for logs.
func (e *APIError) Error() string {
	return fmt.Sprintf("lago: event %s push failed: status %d: %s",
		e.EventID, e.Status, e.Body)
}

// Is lets callers test for APIError via errors.Is(err, &APIError{}).
func (e *APIError) Is(target error) bool {
	var other *APIError
	return errors.As(target, &other)
}
