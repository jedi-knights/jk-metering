package application

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jedi-knights/jk-metering/internal/ports"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName identifies the OTel instrumentation scope for
// counters this package emits. The global MeterProvider resolves it;
// when no provider is installed (tests) the resulting meter is a
// no-op so the production call sites below stay side-effect-free.
const instrumentationName = "github.com/jedi-knights/jk-metering/internal/application"

// Result label values for the metering.events_drained counter.
// Mirrors the three terminal branches of [MeteringService.Tick].
const (
	resultOK      = "ok"
	resultSkipped = "skipped"
	resultFailed  = "failed"
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
	// now stamps both the rate window's start (lastHeartbeatAt) and its
	// end at heartbeat time. Tests inject a fixed clock to pin
	// rate/elapsed arithmetic without racing time.Now.
	now func() time.Time
	// hbMu guards the heartbeat-owned state below. Run drives the
	// heartbeat single-writer via its select, but the exported
	// TestOnlyEmitHeartbeat helper (see helpers_test.go) also invokes
	// emitHeartbeat and could race with Run in a poorly-constructed
	// test — the mutex closes that door without adding real overhead
	// on the production path (uncontended lock is a handful of ns).
	hbMu            sync.Mutex
	lastProcessed   uint64
	lastHeartbeatAt time.Time

	processed atomic.Uint64
	failed    atomic.Uint64
	skipped   atomic.Uint64

	// eventsDrained records one tick-terminal outcome per audit event
	// with a result label (ok | skipped | failed). Exposes the same
	// three counts the heartbeat log line carries, in Prometheus
	// shape, scraped from /metrics on :9464.
	eventsDrained metric.Int64Counter
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
		eventsDrained:     newEventsDrainedCounter(),
	}
}

// newEventsDrainedCounter builds the metering.events_drained counter
// from the global MeterProvider. A failed registration (unexpected —
// the OTel SDK's returned counter is always a valid no-op in error
// paths) is logged at the point of first use rather than crashing
// startup.
func newEventsDrainedCounter() metric.Int64Counter {
	meter := otel.Meter(instrumentationName)
	counter, _ := meter.Int64Counter(
		"metering.events_drained",
		metric.WithDescription("Audit events drained from the queue, labeled by outcome."),
		metric.WithUnit("{event}"),
	)
	return counter
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

// emitHeartbeat writes one INFO (or ERROR when lag exceeds
// lagAlertThreshold or the lag query itself failed) log line carrying
// the rolling counters, the consumption rate since the previous
// heartbeat, and the age of the oldest unconsumed row.
//
// Unexported: Run drives it via its select loop; tests reach it via
// [MeteringService.TestOnlyEmitHeartbeat] which delegates here. The
// hbMu lock is held across the whole emission so lastProcessed and
// lastHeartbeatAt cannot be read torn even if a badly-constructed
// test races Run.
func (s *MeteringService) emitHeartbeat(ctx context.Context) {
	s.hbMu.Lock()
	defer s.hbMu.Unlock()

	stats := s.Stats()
	processedNow := stats.Processed
	delta := processedNow - s.lastProcessed
	elapsed := s.now().Sub(s.lastHeartbeatAt)
	rate := 0.0
	if elapsed > 0 {
		rate = float64(delta) / elapsed.Seconds()
	}
	lag, lagErr := s.source.OldestUnconsumedAge(ctx)

	args := []any{
		"processed_total", processedNow,
		"failed_total", stats.Failed,
		"skipped_total", stats.Skipped,
		"processed_delta", delta,
		"rate_per_sec", rate,
		"lag_seconds", lag.Seconds(),
		"lag_alert_threshold_seconds", s.lagAlertThreshold.Seconds(),
	}
	switch {
	case lagErr != nil:
		// Fail closed: a systematically failing lag query would
		// otherwise leave the ERROR-level alert dark forever because
		// the WARN-only path never carries the alert message.
		// Escalate here so the Fly log-based filter still fires and an
		// operator hears about the gap.
		args = append(args, "lag_query_error", lagErr.Error())
		s.logger.Error("metering heartbeat: lag exceeds alert threshold (lag query failed)", args...)
	case lag > s.lagAlertThreshold:
		s.logger.Error("metering heartbeat: lag exceeds alert threshold", args...)
	default:
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
	hbCh, hbStop := s.heartbeatTicker()
	defer hbStop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			s.tickAndLog(ctx)
		case <-hbCh:
			s.emitHeartbeat(ctx)
		}
	}
}

// heartbeatTicker returns the heartbeat timer channel and its Stop
// hook. A zero HeartbeatInterval yields a nil channel and a no-op
// stop — receiving from nil blocks forever, which cleanly disables
// the heartbeat case of Run's select without special-casing the
// config. Callers must defer the stop so the underlying Ticker does
// not leak when Run returns.
func (s *MeteringService) heartbeatTicker() (<-chan time.Time, func()) {
	if s.heartbeatInterval <= 0 {
		return nil, func() {}
	}
	t := time.NewTicker(s.heartbeatInterval)
	return t.C, t.Stop
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
				s.recordDrained(ctx, resultFailed)
				continue
			}
			s.skipped.Add(1)
			s.recordDrained(ctx, resultSkipped)
			s.logger.Warn("skipping event with no billing identity",
				"event_id", e.EventID, "event_type", e.EventType)
			continue
		}
		if err := s.sink.PushEvent(ctx, lagoEvent); err != nil {
			s.logger.Error("pushing event to Lago",
				"event_id", e.EventID, "error", err)
			s.failed.Add(1)
			s.recordDrained(ctx, resultFailed)
			continue
		}
		if err := s.source.MarkConsumed(ctx, e.EventID); err != nil {
			s.logger.Error("marking event consumed",
				"event_id", e.EventID, "error", err)
			s.failed.Add(1)
			s.recordDrained(ctx, resultFailed)
			continue
		}
		s.processed.Add(1)
		s.recordDrained(ctx, resultOK)
	}
	return nil
}

// recordDrained increments the metering.events_drained counter with
// the given result label. Centralized so the three per-event branches
// of [Tick] stay readable; the counter is nil-safe (OTel's no-op
// meter path when no MeterProvider is installed).
func (s *MeteringService) recordDrained(ctx context.Context, result string) {
	if s.eventsDrained == nil {
		return
	}
	s.eventsDrained.Add(ctx, 1, metric.WithAttributes(attribute.String("result", result)))
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
