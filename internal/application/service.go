package application

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jedi-knights/jk-metering/internal/ports"
)

// MeteringService is the core orchestration loop. It pulls unconsumed
// audit events from the [ports.EventSource], transforms each into a Lago
// event, pushes via the [ports.MeterSink], and marks each consumed on
// success.
//
// Failures are deliberately non-fatal at the per-event level so a
// poison event cannot stall the whole consumer. Errors are logged and
// metric counters incremented; the event remains unconsumed and will be
// retried on the next tick. A follow-up will add a DLQ for events that
// fail repeatedly.
type MeteringService struct {
	source          ports.EventSource
	sink            ports.MeterSink
	logger          *slog.Logger
	billingIdentity BillingIdentityField
	batchSize       int

	// heartbeatInterval controls how often Run emits a heartbeat log
	// line with the rolling counters + lag. Zero disables the
	// heartbeat entirely (used by tests that drive Tick directly).
	heartbeatInterval time.Duration
	// lagAlertThreshold is the age above which a heartbeat is emitted
	// at ERROR level — Fly log-based alerts fire on this line per the
	// E6-S1 AC on issue #166.
	lagAlertThreshold time.Duration
	// now sources the wall clock for rate calculation; tests inject a
	// fixed clock to pin rate/lag arithmetic without racing time.Now.
	now func() time.Time
	// heartbeat-loop-owned state: touched only from emitHeartbeat and
	// its constructor path, both of which run on the Run goroutine.
	lastProcessed   uint64
	lastHeartbeatAt time.Time

	processed atomic.Uint64
	failed    atomic.Uint64
	skipped   atomic.Uint64
}

// Config holds the service-level knobs that are passed in at the
// composition root.
type Config struct {
	Logger          *slog.Logger
	BillingIdentity BillingIdentityField
	BatchSize       int

	// HeartbeatInterval controls how often Run emits a heartbeat log
	// line with the rolling counters + lag. Zero disables the
	// heartbeat — set explicitly in tests so a rapid Tick loop is not
	// polluted with heartbeat noise.
	HeartbeatInterval time.Duration
	// LagAlertThreshold is the oldest-unconsumed-age above which a
	// heartbeat escalates to ERROR level so a Fly log-based alert
	// fires. Zero defaults to 5 min per the E6-S1 AC.
	LagAlertThreshold time.Duration
	// Now overrides the wall clock used by the heartbeat rate
	// calculation. Zero-value (nil) defaults to time.Now.
	Now func() time.Time
}

// DefaultLagAlertThreshold is the E6-S1 AC-mandated ceiling on the age
// of the oldest unconsumed row. Above this, the heartbeat log line is
// emitted at ERROR level so a Fly log-based alert fires.
const DefaultLagAlertThreshold = 5 * time.Minute

// NewMeteringService constructs the service. nil dependencies panic at
// the composition root so wiring errors surface loudly. BatchSize <= 0
// defaults to 100.
func NewMeteringService(source ports.EventSource, sink ports.MeterSink, cfg Config) *MeteringService {
	if source == nil {
		panic("application: NewMeteringService called with nil EventSource")
	}
	if sink == nil {
		panic("application: NewMeteringService called with nil MeterSink")
	}
	cfg = applyServiceDefaults(cfg)
	return &MeteringService{
		source:            source,
		sink:              sink,
		logger:            cfg.Logger,
		billingIdentity:   cfg.BillingIdentity,
		batchSize:         cfg.BatchSize,
		heartbeatInterval: cfg.HeartbeatInterval,
		lagAlertThreshold: cfg.LagAlertThreshold,
		now:               cfg.Now,
		lastHeartbeatAt:   cfg.Now(),
	}
}

// applyServiceDefaults fills in the zero-value config fields with their
// documented defaults. Extracted so NewMeteringService stays under the
// gocyclo cap; every branch here maps to a Config field's docstring.
func applyServiceDefaults(cfg Config) Config {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.BillingIdentity == "" {
		cfg.BillingIdentity = BillingIdentitySubject
	}
	if cfg.LagAlertThreshold <= 0 {
		cfg.LagAlertThreshold = DefaultLagAlertThreshold
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return cfg
}

// EmitHeartbeat writes one INFO (or ERROR when lag exceeds
// lagAlertThreshold) log line carrying the rolling counters, the
// consumption rate since the previous heartbeat, and the age of the
// oldest unconsumed row.
//
// Exported so tests can drive the heartbeat directly; Run schedules
// this on the configured interval. Callers outside Run must not
// invoke this concurrently — the lastProcessed / lastHeartbeatAt
// bookkeeping is single-writer.
func (s *MeteringService) EmitHeartbeat(ctx context.Context) {
	stats := s.Stats()
	processedNow := stats.Processed
	delta := processedNow - s.lastProcessed
	elapsed := s.now().Sub(s.lastHeartbeatAt)
	rate := 0.0
	if elapsed > 0 {
		rate = float64(delta) / elapsed.Seconds()
	}
	lag, lagErr := s.source.OldestUnconsumedAge(ctx)
	if lagErr != nil {
		// A lag query failure is informational — the tick loop is what
		// actually drains events. Log at WARN so the operator sees it
		// without pretending the whole worker is unhealthy.
		s.logger.Warn("heartbeat: lag query failed", "error", lagErr)
	}
	args := []any{
		"processed_total", processedNow,
		"failed_total", stats.Failed,
		"skipped_total", stats.Skipped,
		"processed_delta", delta,
		"rate_per_sec", rate,
		"lag_seconds", lag.Seconds(),
		"lag_alert_threshold_seconds", s.lagAlertThreshold.Seconds(),
	}
	if lag > s.lagAlertThreshold {
		s.logger.Error("metering heartbeat: lag exceeds alert threshold", args...)
	} else {
		s.logger.Info("metering heartbeat", args...)
	}
	s.lastProcessed = processedNow
	s.lastHeartbeatAt = s.now()
}

// Stats is a snapshot of the service's counters. Useful for exporting as
// Prometheus / OTel metrics.
type Stats struct {
	Processed uint64
	Failed    uint64
	// Skipped is incremented when an event cannot resolve a billing
	// identity (every candidate field empty). The event is marked
	// consumed to avoid retry-storming the unattributable record.
	Skipped uint64
}

// Stats returns a snapshot of the current counters.
func (s *MeteringService) Stats() Stats {
	return Stats{
		Processed: s.processed.Load(),
		Failed:    s.failed.Load(),
		Skipped:   s.skipped.Load(),
	}
}

// Run blocks until ctx is canceled, fetching and forwarding events on
// the configured interval. Use [MeteringService.Tick] when an external
// scheduler (test harness, lambda) drives the loop instead.
func (s *MeteringService) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("metering: Run requires a positive interval, got %v", interval)
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	// Run an initial tick immediately rather than waiting for the first
	// timer fire — keeps the steady-state behavior the same whether the
	// service is freshly started or has been polling for hours.
	s.tickAndLog(ctx)
	hbCh := s.heartbeatChan()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s.tickAndLog(ctx)
		case <-hbCh:
			s.EmitHeartbeat(ctx)
		}
	}
}

// heartbeatChan returns the heartbeat timer channel. A zero
// HeartbeatInterval yields a nil channel — receiving from nil blocks
// forever, which cleanly disables the heartbeat case of Run's select
// without special-casing the config.
func (s *MeteringService) heartbeatChan() <-chan time.Time {
	if s.heartbeatInterval <= 0 {
		return nil
	}
	// The Ticker leaks on process exit; acceptable since Run runs
	// until ctx cancellation and the process terminates immediately
	// after. A follow-up can plumb Stop through if Run is ever hosted
	// inside a longer-lived container.
	return time.NewTicker(s.heartbeatInterval).C
}

// tickAndLog runs one Tick and logs any error at ERROR level, except
// when ctx has already been canceled — the tick's failure is expected
// on shutdown and would otherwise generate a noise line at every
// deploy.
func (s *MeteringService) tickAndLog(ctx context.Context) {
	if err := s.Tick(ctx); err != nil && !ctxCanceled(ctx) {
		s.logger.Error("metering tick failed", "error", err)
	}
}

// Tick performs one fetch-transform-push-mark cycle. Returned errors
// only cover infrastructure failures (source unreachable, etc.) — per-
// event transform / push failures are logged and counted internally so
// one bad event cannot fail the whole tick.
func (s *MeteringService) Tick(ctx context.Context) error {
	events, err := s.source.FetchUnconsumed(ctx, s.batchSize)
	if err != nil {
		return fmt.Errorf("fetching unconsumed events: %w", err)
	}
	for _, e := range events {
		lagoEvent := Transform(e, s.billingIdentity)
		if lagoEvent.ExternalSubscriptionID == "" {
			// No billing identity at all — every candidate field is
			// empty. Mark consumed and increment skipped so we do not
			// retry-loop on an unattributable record.
			if err := s.source.MarkConsumed(ctx, e.EventID); err != nil {
				s.logger.Error("marking skipped event consumed",
					"event_id", e.EventID, "error", err)
				s.failed.Add(1)
				continue
			}
			s.skipped.Add(1)
			s.logger.Warn("skipping event with no billing identity",
				"event_id", e.EventID, "event_type", e.EventType)
			continue
		}
		if err := s.sink.PushEvent(ctx, lagoEvent); err != nil {
			s.logger.Error("pushing event to Lago",
				"event_id", e.EventID, "error", err)
			s.failed.Add(1)
			continue
		}
		if err := s.source.MarkConsumed(ctx, e.EventID); err != nil {
			s.logger.Error("marking event consumed",
				"event_id", e.EventID, "error", err)
			s.failed.Add(1)
			continue
		}
		s.processed.Add(1)
	}
	return nil
}

// ctxCanceled reports whether ctx is done — used to suppress noise
// logging when Run exits in the normal shutdown path.
func ctxCanceled(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true
	default:
		return false
	}
}
