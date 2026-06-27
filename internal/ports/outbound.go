// Package ports declares the hexagonal port interfaces the metering shim
// depends on. Adapters in internal/adapters/outbound/* implement them;
// the application layer programs against these interfaces only.
package ports

import (
	"context"

	"github.com/jedi-knights/jk-metering/internal/domain"
)

// EventSource is the inbound port for unconsumed audit events. The
// Postgres adapter polls the audit_events table; a future NATS JetStream
// adapter could implement the same contract.
type EventSource interface {
	// FetchUnconsumed returns up to limit events that have not yet been
	// pushed to Lago, ordered by creation time so older events surface
	// first. Returns an empty slice (not nil) when nothing is pending.
	FetchUnconsumed(ctx context.Context, limit int) ([]domain.AuditEvent, error)

	// MarkConsumed records that the event with the given ID was
	// successfully delivered to the metering sink. Idempotent — calling
	// twice with the same ID is a no-op.
	MarkConsumed(ctx context.Context, eventID string) error
}

// MeterSink is the outbound port the application uses to push events to
// the metering backend. The Lago adapter implements it; tests use a
// recording double.
type MeterSink interface {
	// PushEvent transports a single Lago event. Implementations must be
	// idempotent against [domain.LagoEvent.TransactionID] so retries do
	// not double-bill.
	PushEvent(ctx context.Context, event domain.LagoEvent) error
}
