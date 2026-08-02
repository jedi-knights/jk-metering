package postgres_test

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v4"

	pgadapter "github.com/jedi-knights/jk-metering/internal/adapters/outbound/postgres"
)

func TestOldestUnconsumedAge_ReturnsQueriedDuration(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "")

	mock.ExpectQuery(regexp.QuoteMeta("EXTRACT(EPOCH FROM (now() - MIN(created_at)))")).
		WillReturnRows(pgxmock.NewRows([]string{"seconds"}).AddRow(float64(420)))

	got, err := src.OldestUnconsumedAge(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 7*time.Minute {
		t.Errorf("age = %v, want 7m", got)
	}
}

func TestOldestUnconsumedAge_EmptyBacklogReturnsZero(t *testing.T) {
	mock := newMock(t)
	src := pgadapter.New(mock, "")

	// COALESCE turns MIN(NULL) into 0 seconds.
	mock.ExpectQuery(regexp.QuoteMeta("EXTRACT(EPOCH FROM (now() - MIN(created_at)))")).
		WillReturnRows(pgxmock.NewRows([]string{"seconds"}).AddRow(float64(0)))

	got, err := src.OldestUnconsumedAge(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 0 {
		t.Errorf("age = %v, want 0", got)
	}
}
