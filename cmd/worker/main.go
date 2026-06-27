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

	"github.com/jedi-knights/jk-metering/internal/application"
	"github.com/jedi-knights/jk-metering/internal/config"
	lagoadapter "github.com/jedi-knights/jk-metering/internal/adapters/outbound/lago"
	pgadapter "github.com/jedi-knights/jk-metering/internal/adapters/outbound/postgres"
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

	pool, err := pgxpool.New(ctx, cfg.Audit.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	source := pgadapter.New(pool, cfg.Audit.Table)
	sink := lagoadapter.New(cfg.Lago.BaseURL, cfg.Lago.APIKey,
		&http.Client{Timeout: lagoadapter.DefaultTimeout})

	svc := application.NewMeteringService(source, sink, application.Config{
		Logger:          logger,
		BillingIdentity: application.BillingIdentityField(cfg.Metering.BillingIdentity),
		BatchSize:       cfg.Metering.BatchSize,
	})

	logger.Info("metering shim starting",
		"poll_interval_seconds", cfg.Metering.PollIntervalSeconds,
		"batch_size", cfg.Metering.BatchSize,
		"billing_identity", cfg.Metering.BillingIdentity,
		"audit_table", cfg.Audit.Table)

	interval := time.Duration(cfg.Metering.PollIntervalSeconds) * time.Second
	return svc.Run(ctx, interval)
}

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
	switch cfg.Format {
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	default:
		return slog.New(slog.NewJSONHandler(os.Stderr, opts))
	}
}
