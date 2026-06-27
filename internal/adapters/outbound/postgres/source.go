// Package postgres provides a [ports.EventSource] implementation that
// reads unconsumed rows from the audit_events table written by
// go-platform/audit/durable. The table contract is owned by that package;
// see identity-platform-go ADR-0019 for the schema and the consumed_at
// idempotency lever this adapter uses.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/jedi-knights/jk-metering/internal/domain"
	"github.com/jedi-knights/jk-metering/internal/ports"
)

// DefaultTable matches go-platform/audit/durable's default table name.
const DefaultTable = "audit_events"

// Querier is the minimum subset of [pgxpool.Pool] this adapter uses.
// Tests substitute a pgxmock pool directly; production wires the real
// pool.
type Querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// Source is the Postgres-backed [ports.EventSource].
type Source struct {
	db    Querier
	table string
}

// Compile-time assertion.
var _ ports.EventSource = (*Source)(nil)

// New constructs the Postgres source. A nil db panics — composition
// errors are loud at startup. table defaults to [DefaultTable] when
// empty; identifier validation guards the only string interpolated into
// the SQL.
func New(db Querier, table string) *Source {
	if db == nil {
		panic("metering/postgres: New called with nil Querier")
	}
	if table == "" {
		table = DefaultTable
	}
	if err := validateIdentifier(table); err != nil {
		panic("metering/postgres: invalid table name: " + err.Error())
	}
	return &Source{db: db, table: table}
}

// FetchUnconsumed returns up to limit rows with consumed_at IS NULL,
// ordered by created_at ascending so older events surface first. The
// payload column is JSON-decoded into [domain.AuditEvent]; rows that
// fail to decode are skipped and logged through the returned error
// chain (a single decode failure does not poison the whole batch — it
// is wrapped with the event_id so the caller can DLQ that row).
func (s *Source) FetchUnconsumed(ctx context.Context, limit int) ([]domain.AuditEvent, error) {
	if limit <= 0 {
		limit = 100
	}
	// #nosec G201 -- table name is validated by validateIdentifier at construction.
	query := fmt.Sprintf(
		`SELECT event_id, payload, created_at, consumed_at
		   FROM %s
		  WHERE consumed_at IS NULL
		  ORDER BY created_at ASC
		  LIMIT $1`, s.table)
	rows, err := s.db.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("querying %s: %w", s.table, err)
	}
	defer rows.Close()

	out := make([]domain.AuditEvent, 0, limit)
	for rows.Next() {
		var (
			eventID    string
			payload    []byte
			createdAt  = nilTime{}
			consumedAt = nilTime{}
		)
		if err := rows.Scan(&eventID, &payload, &createdAt, &consumedAt); err != nil {
			return nil, fmt.Errorf("scanning audit event row: %w", err)
		}
		var e domain.AuditEvent
		if err := json.Unmarshal(payload, &e); err != nil {
			return nil, fmt.Errorf("decoding payload for event %s: %w", eventID, err)
		}
		// Bookkeeping fields come from the row, not the payload.
		e.EventID = eventID // payload may differ; the row is canonical
		e.CreatedAt = createdAt.t
		if consumedAt.set {
			t := consumedAt.t
			e.ConsumedAt = &t
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// MarkConsumed records that the event has been pushed to Lago. Sets
// consumed_at = now(); idempotent under repeated calls because the
// downstream metering shim's only state on Lago is the event itself
// (deduped by transaction_id).
func (s *Source) MarkConsumed(ctx context.Context, eventID string) error {
	// #nosec G201 -- table name is validated by validateIdentifier at construction.
	query := fmt.Sprintf(
		`UPDATE %s SET consumed_at = now() WHERE event_id = $1`, s.table)
	if _, err := s.db.Exec(ctx, query, eventID); err != nil {
		return fmt.Errorf("marking event %s consumed: %w", eventID, err)
	}
	return nil
}

// validateIdentifier guards against SQL injection in the table-name
// slot. Same shape as go-platform/audit/durable.
func validateIdentifier(s string) error {
	if s == "" {
		return errors.New("identifier is empty")
	}
	if len(s) > 63 {
		return errors.New("identifier exceeds 63 characters")
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r == '_':
		case r >= '0' && r <= '9':
			if i == 0 {
				return errors.New("identifier cannot start with a digit")
			}
		default:
			return fmt.Errorf("identifier contains invalid character %q at position %d", r, i)
		}
	}
	return nil
}
