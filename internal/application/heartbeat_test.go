package application_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jedi-knights/jk-metering/internal/application"
)

// captureLogger returns a logger writing JSON to the buffer, plus the
// buffer itself so tests can inspect emitted lines.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	buf := &bytes.Buffer{}
	return slog.New(slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: slog.LevelInfo})), buf
}

// fakeClock advances by the accumulated delta each call so tests can
// pin the wall-clock the heartbeat's rate calculation reads.
type fakeClock struct {
	now time.Time
}

func (c *fakeClock) fn() func() time.Time    { return func() time.Time { return c.now } }
func (c *fakeClock) advance(d time.Duration) { c.now = c.now.Add(d) }

func TestEmitHeartbeat_UnderThresholdLogsInfo(t *testing.T) {
	logger, buf := captureLogger()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	src := &fakeSource{oldestAge: 30 * time.Second}
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger:            logger,
		LagAlertThreshold: 5 * time.Minute,
		Now:               clock.fn(),
	})
	clock.advance(60 * time.Second)

	svc.TestOnlyEmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"level":"INFO"`) {
		t.Errorf("expected INFO level, got %q", line)
	}
	if !strings.Contains(line, `"lag_seconds":30`) {
		t.Errorf("expected lag_seconds=30, got %q", line)
	}
}

func TestEmitHeartbeat_OverThresholdEscalatesToError(t *testing.T) {
	logger, buf := captureLogger()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	src := &fakeSource{oldestAge: 10 * time.Minute}
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger:            logger,
		LagAlertThreshold: 5 * time.Minute,
		Now:               clock.fn(),
	})
	clock.advance(60 * time.Second)

	svc.TestOnlyEmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"level":"ERROR"`) {
		t.Errorf("expected ERROR level (Fly log-alert signal), got %q", line)
	}
	if !strings.Contains(line, "lag exceeds alert threshold") {
		t.Errorf("expected alert message, got %q", line)
	}
}

// TestEmitHeartbeat_LagQueryFailureFiresAlert guards the Must Fix from
// the PR review: a lag-query error must escalate to ERROR-with-alert-
// marker, otherwise a broken lag query would silently disable the
// entire E6-S1 alert. Fail-closed on this path.
func TestEmitHeartbeat_LagQueryFailureFiresAlert(t *testing.T) {
	logger, buf := captureLogger()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	src := &fakeSource{oldestErr: errors.New("lag query timeout")}
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger:            logger,
		LagAlertThreshold: 5 * time.Minute,
		Now:               clock.fn(),
	})
	clock.advance(60 * time.Second)

	svc.TestOnlyEmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"level":"ERROR"`) {
		t.Errorf("expected ERROR (fail-closed alert) on lag query failure, got %q", line)
	}
	if !strings.Contains(line, "lag exceeds alert threshold") {
		t.Errorf("expected alert marker so Fly log-based filter still fires, got %q", line)
	}
	if !strings.Contains(line, `"lag_query_error":"lag query timeout"`) {
		t.Errorf("expected lag_query_error attribute carrying cause, got %q", line)
	}
}

// TestEmitHeartbeat_LagAtThresholdStaysInfo pins the strict-greater
// comparison so a mutation flipping `>` to `>=` at line ~168 of
// service.go dies. Boundary-flip tests are the highest-signal
// mutations we can pre-empt cheaply.
func TestEmitHeartbeat_LagAtThresholdStaysInfo(t *testing.T) {
	logger, buf := captureLogger()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	src := &fakeSource{oldestAge: 5 * time.Minute}
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger:            logger,
		LagAlertThreshold: 5 * time.Minute,
		Now:               clock.fn(),
	})
	clock.advance(60 * time.Second)

	svc.TestOnlyEmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"level":"INFO"`) {
		t.Errorf("lag == threshold should stay INFO (uses `>` not `>=`); got %q", line)
	}
}

// TestEmitHeartbeat_EmptyBacklogStaysInfo covers the lag=0 happy path
// end-to-end so the postgres COALESCE branch has a heartbeat-layer
// counterpart.
func TestEmitHeartbeat_EmptyBacklogStaysInfo(t *testing.T) {
	logger, buf := captureLogger()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	src := &fakeSource{} // oldestAge zero, no err
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger: logger,
		Now:    clock.fn(),
	})
	clock.advance(60 * time.Second)

	svc.TestOnlyEmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"level":"INFO"`) {
		t.Errorf("empty backlog (lag=0) should log INFO, got %q", line)
	}
	if !strings.Contains(line, `"lag_seconds":0`) {
		t.Errorf("expected lag_seconds=0, got %q", line)
	}
}

func TestEmitHeartbeat_ReportsRatePerSecond(t *testing.T) {
	logger, buf := captureLogger()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	src := &fakeSource{}
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger: logger,
		Now:    clock.fn(),
	})

	// Drive 20 events through the service, then advance 10 seconds
	// before heartbeating — expected rate = 2 events/sec.
	for range 20 {
		src.pending = append(src.pending, sampleEvent())
	}
	_ = svc.Tick(context.Background()) // processed_total should now be 20

	clock.advance(10 * time.Second)
	svc.TestOnlyEmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"rate_per_sec":2`) {
		t.Errorf("expected rate_per_sec=2, got %q", line)
	}
	if !strings.Contains(line, `"processed_delta":20`) {
		t.Errorf("expected processed_delta=20, got %q", line)
	}
}

// TestRun_SchedulesHeartbeat catches a regression where the heartbeat
// select case is dropped from Run. Uses real time (short intervals) so
// the ticker + select actually fire, then cancels the context and
// asserts the heartbeat marker landed in the log.
func TestRun_SchedulesHeartbeat(t *testing.T) {
	logger, buf := captureLogger()
	src := &fakeSource{}
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger:            logger,
		HeartbeatInterval: 20 * time.Millisecond,
		LagAlertThreshold: 5 * time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	// Run returns ctx.Err() on cancellation; that's the expected shape.
	_ = svc.Run(ctx, 50*time.Millisecond)

	if !strings.Contains(buf.String(), "metering heartbeat") {
		t.Errorf("expected Run to schedule at least one heartbeat, log had %q", buf.String())
	}
}
