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

	svc.EmitHeartbeat(context.Background())

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

	svc.EmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"level":"ERROR"`) {
		t.Errorf("expected ERROR level (Fly log-alert signal), got %q", line)
	}
	if !strings.Contains(line, "lag exceeds alert threshold") {
		t.Errorf("expected alert message, got %q", line)
	}
}

func TestEmitHeartbeat_LagQueryFailureLogsWarn(t *testing.T) {
	logger, buf := captureLogger()
	clock := &fakeClock{now: time.Unix(1_000_000, 0)}
	src := &fakeSource{oldestErr: errors.New("lag query timeout")}
	svc := application.NewMeteringService(src, &fakeSink{}, application.Config{
		Logger:            logger,
		LagAlertThreshold: 5 * time.Minute,
		Now:               clock.fn(),
	})
	clock.advance(60 * time.Second)

	svc.EmitHeartbeat(context.Background())

	if !strings.Contains(buf.String(), `"level":"WARN"`) {
		t.Errorf("expected WARN for lag query failure, got %q", buf.String())
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
	for i := 0; i < 20; i++ {
		src.pending = append(src.pending, sampleEvent())
	}
	_ = svc.Tick(context.Background()) // processed_total should now be 20

	clock.advance(10 * time.Second)
	svc.EmitHeartbeat(context.Background())

	line := buf.String()
	if !strings.Contains(line, `"rate_per_sec":2`) {
		t.Errorf("expected rate_per_sec=2, got %q", line)
	}
	if !strings.Contains(line, `"processed_delta":20`) {
		t.Errorf("expected processed_delta=20, got %q", line)
	}
}
