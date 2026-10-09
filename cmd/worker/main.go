// Package main is the entry point for the jk-metering shim. Its only
// job is to wire the composition root and start the polling loop; all
// behavior lives behind hexagonal ports in internal/.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/jedi-knights/go-platform/httpserver"
	platformotel "github.com/jedi-knights/go-platform/otel"

	lagoadapter "github.com/jedi-knights/jk-metering/internal/adapters/outbound/lago"
	pgadapter "github.com/jedi-knights/jk-metering/internal/adapters/outbound/postgres"
	"github.com/jedi-knights/jk-metering/internal/application"
	"github.com/jedi-knights/jk-metering/internal/config"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		slog.Default().Error("metering shim exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	logger := newLogger(cfg.Log)
	slog.SetDefault(logger)

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	obs, err := platformotel.New(ctx, platformotel.Config{
		ServiceName: "jk-metering-worker",
	})
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()
		if sErr := obs.Shutdown(shutdownCtx); sErr != nil {
			logger.Error("observability shutdown error", "error", sErr)
		}
	}()

	// Even though the worker has no main HTTP listener, it exposes
	// /metrics on :9464 so Fly's hosted Prometheus can scrape the
	// Go runtime + OTel runtime instrumentation metrics (and any
	// counters this service registers in follow-ups —
	// metering.events_drained, metering.lago_push_duration).
	metricsSrv, err := httpserver.StartMetricsServer("", "", obs.PromHandler)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if sErr := metricsSrv.Shutdown(shutdownCtx); sErr != nil {
			logger.Error("metrics shutdown error", "error", sErr)
		}
	}()
	logger.Info("metrics endpoint ready", "addr", httpserver.DefaultMetricsAddr, "path", httpserver.DefaultMetricsPath)

	pool, err := pgxpool.New(ctx, cfg.Audit.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	source := pgadapter.New(pool, cfg.Audit.Table)
	sink := lagoadapter.New(cfg.Lago.BaseURL, cfg.Lago.APIKey,
		&http.Client{Timeout: lagoadapter.DefaultTimeout})

	svc := application.NewMeteringService(source, sink, application.Config{
		Logger:            logger,
		BillingIdentity:   application.BillingIdentityField(cfg.Metering.BillingIdentity),
		BatchSize:         cfg.Metering.BatchSize,
		HeartbeatInterval: time.Duration(cfg.Metering.HeartbeatIntervalSeconds) * time.Second,
		LagAlertThreshold: time.Duration(cfg.Metering.LagAlertSeconds) * time.Second,
	})

	logger.Info("metering shim starting",
		"poll_interval_seconds", cfg.Metering.PollIntervalSeconds,
		"batch_size", cfg.Metering.BatchSize,
		"billing_identity", cfg.Metering.BillingIdentity,
		"heartbeat_interval_seconds", cfg.Metering.HeartbeatIntervalSeconds,
		"lag_alert_seconds", cfg.Metering.LagAlertSeconds,
		"audit_table", cfg.Audit.Table)

	interval := time.Duration(cfg.Metering.PollIntervalSeconds) * time.Second
	return svc.Run(ctx, interval)
}

// newLogger builds the service's structured logger. The base handler
// is wrapped by [platformotel.SpanContextHandler] so every record
// carries trace_id and span_id from any OTel span on the context —
// matching the fleet-wide log↔trace correlation story.
func newLogger(cfg config.LogConfig) *slog.Logger {
	level := slog.LevelInfo
	switch cfg.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	var base slog.Handler
	switch cfg.Format {
	case "text":
		base = slog.NewTextHandler(os.Stderr, opts)
	default:
		base = slog.NewJSONHandler(os.Stderr, opts)
	}
	return slog.New(platformotel.NewSpanContextHandler(base))
}
