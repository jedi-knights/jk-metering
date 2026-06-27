// Package domain holds the metering shim's pure data model. It mirrors the
// ADR-0018 audit envelope so the shim can deserialize a Postgres row from
// the audit_events table and transform it into a Lago event without any
// cross-package coupling to the producing services.
package domain

import "time"

// AuditEvent is a deserialized row from the audit_events table. The shape
// matches go-platform/audit.Event with one extension: CreatedAt and
// ConsumedAt come from the audit_events table itself, not from the JSON
// payload, so the shim can track its own progress without modifying the
// schema-version-stable Event envelope.
//
// Attrs is intentionally typed as map[string]any — the shim does not
// validate or interpret event-type-specific properties; it forwards them
// verbatim to Lago as event properties.
type AuditEvent struct {
	// Postgres bookkeeping
	CreatedAt  time.Time
	ConsumedAt *time.Time

	// ADR-0018 envelope (deserialized from audit_events.payload)
	SchemaVersion  string         `json:"schema_version"`
	EventID        string         `json:"event_id"`
	EventType      string         `json:"event_type"`
	Timestamp      time.Time      `json:"timestamp"`
	Service        string         `json:"service"`
	TraceID        string         `json:"trace_id,omitempty"`
	CorrelationID  string         `json:"correlation_id,omitempty"`
	ActorType      string         `json:"actor_type"`
	ActorID        string         `json:"actor_id"`
	SubjectID      string         `json:"subject_id,omitempty"`
	ClientID       string         `json:"client_id,omitempty"`
	Resource       string         `json:"resource"`
	ResourceKind   string         `json:"resource_kind,omitempty"`
	ResourceID     string         `json:"resource_id,omitempty"`
	ResourceParent string         `json:"resource_parent,omitempty"`
	ResourcePath   string         `json:"resource_path,omitempty"`
	Action         string         `json:"action"`
	Decision       string         `json:"decision"`
	Reason         string         `json:"reason,omitempty"`
	Attrs          map[string]any `json:"attrs,omitempty"`
}

// LagoEvent is the request body for Lago's POST /api/v1/events endpoint.
// See https://docs.getlago.com/api-reference/events/create-an-event.
//
// TransactionID is intentionally typed as string so the metering shim can
// map it 1:1 to the audit EventID (ULID) and let Lago's dedupe protect
// against double-billing on retry.
type LagoEvent struct {
	TransactionID          string         `json:"transaction_id"`
	ExternalSubscriptionID string         `json:"external_subscription_id"`
	Code                   string         `json:"code"`
	Timestamp              int64          `json:"timestamp"` // RFC 3339 in Lago docs; Unix seconds also accepted
	Properties             map[string]any `json:"properties,omitempty"`
}

// LagoEventWrapper is the outermost JSON shape Lago's Event API expects.
// Lago wraps the event under an "event" key — this struct exists so the
// transport adapter can serialise the right shape without sprinkling
// JSON-key knowledge through the application layer.
type LagoEventWrapper struct {
	Event LagoEvent `json:"event"`
}
