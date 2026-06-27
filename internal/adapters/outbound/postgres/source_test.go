package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"

	pgadapter "github.com/jedi-knights/jk-metering/internal/adapters/outbound/postgres"
	"github.com/jedi-knights/jk-metering/internal/domain"
)

func newMock(t *testing.T) pgxmock.PgxPoolIface {
	t.Helper()
	m, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool: %v", err)
	}
	t.Cleanup(m.Close)
	return m
}

func samplePayload(t *testing.T) []byte {
	t.Helper()
	payload, err := json.Marshal(domain.AuditEvent{
		SchemaVersion: "1.0",
		EventID:       "01J7M3X9TEST0000000000000",
		EventType:     "tool_invoked",
		Service:       "jk-mcp-nwsl",
		ActorType:     "agent",
		ActorID:       "agent-claude",
		SubjectID:     "user-omar",
		Resource:      "tool:get_standings",
		Action:        "invoke",
		Decision:      "allow",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return payload
}

func TestFetchUnconsumed_DecodesPayload(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "")

	payload := samplePayload(t)
	created := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	mock.ExpectQuery(regexp.QuoteMeta("SELECT event_id, payload, created_at, consumed_at")).
		WithArgs(100).
		WillReturnRows(pgxmock.NewRows([]string{"event_id", "payload", "created_at", "consumed_at"}).
			AddRow("01J7M3X9TEST0000000000000", payload, created, nil))

	events, err := src.FetchUnconsumed(context.Background(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	e := events[0]
	if e.EventType != "tool_invoked" {
		t.Errorf("event_type = %q, want tool_invoked", e.EventType)
	}
	if e.SubjectID != "user-omar" {
		t.Errorf("subject_id = %q, want user-omar", e.SubjectID)
	}
	if !e.CreatedAt.Equal(created) {
		t.Errorf("created_at = %v, want %v", e.CreatedAt, created)
	}
	if e.ConsumedAt != nil {
		t.Errorf("consumed_at = %v, want nil", e.ConsumedAt)
	}
}

func TestFetchUnconsumed_DefaultsLimit(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT event_id, payload")).
		WithArgs(100).
		WillReturnRows(pgxmock.NewRows([]string{"event_id", "payload", "created_at", "consumed_at"}))

	if _, err := src.FetchUnconsumed(context.Background(), 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

func TestFetchUnconsumed_BadPayloadReturnsErrorWithEventID(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "")

	mock.ExpectQuery(regexp.QuoteMeta("SELECT event_id, payload")).
		WithArgs(100).
		WillReturnRows(pgxmock.NewRows([]string{"event_id", "payload", "created_at", "consumed_at"}).
			AddRow("01J7M3X9BAD0000000000000", []byte("not json"), time.Now(), nil))

	_, err := src.FetchUnconsumed(context.Background(), 100)
	if err == nil {
		t.Fatal("expected error for malformed payload")
	}
	if !regexp.MustCompile(`01J7M3X9BAD0000000000000`).MatchString(err.Error()) {
		t.Errorf("error did not include event id: %v", err)
	}
}

func TestMarkConsumed_UpdatesRow(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "")

	mock.ExpectExec(regexp.QuoteMeta("UPDATE audit_events SET consumed_at = now() WHERE event_id = $1")).
		WithArgs("01J7M3X9TEST0000000000000").
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	if err := src.MarkConsumed(context.Background(), "01J7M3X9TEST0000000000000"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMarkConsumed_PropagatesError(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "")

	dbErr := errors.New("connection refused")
	mock.ExpectExec(regexp.QuoteMeta("UPDATE audit_events")).
		WithArgs("01J7M3X9TEST0000000000000").
		WillReturnError(dbErr)

	err := src.MarkConsumed(context.Background(), "01J7M3X9TEST0000000000000")
	if !errors.Is(err, dbErr) {
		t.Errorf("expected wrapped db error, got %v", err)
	}
}

func TestNew_NilDBPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = pgadapter.New(nil, "")
}

func TestNew_InvalidTableNamePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	_ = pgadapter.New(newMock(t), "events; DROP TABLE x")
}

func TestNew_RespectsCustomTable(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "audit_events_v2")

	mock.ExpectQuery(regexp.QuoteMeta("FROM audit_events_v2")).
		WithArgs(100).
		WillReturnRows(pgxmock.NewRows([]string{"event_id", "payload", "created_at", "consumed_at"}))

	if _, err := src.FetchUnconsumed(context.Background(), 100); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
