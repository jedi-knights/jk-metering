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
}

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
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	billing := cfg.BillingIdentity
	if billing == "" {
		billing = BillingIdentitySubject
	}
	return &MeteringService{
		source:          source,
		sink:            sink,
		logger:          logger,
		billingIdentity: billing,
		batchSize:       batchSize,
	}
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

// Run blocks until ctx is cancelled, fetching and forwarding events on
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
	if err := s.Tick(ctx); err != nil && !ctxCanceled(ctx) {
		s.logger.Error("metering tick failed", "error", err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := s.Tick(ctx); err != nil && !ctxCanceled(ctx) {
				s.logger.Error("metering tick failed", "error", err)
			}
		}
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
