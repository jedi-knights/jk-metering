// Command jk-metering-ingest is the HTTP entry point that lets web
// applications, SPAs, and any client that cannot import go-platform/audit
// emit billable events through a bearer-token-authenticated endpoint.
// The emitted events flow into the same audit_events table the
// jk-metering worker consumes, so there is one canonical path from
// emission to Lago billing regardless of the producer.
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

	"github.com/jedi-knights/go-platform/audit"
	"github.com/jedi-knights/go-platform/audit/durable"

	"github.com/jedi-knights/jk-metering/internal/config"
	"github.com/jedi-knights/jk-metering/internal/ingest"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Default().Error("ingest exited with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.LoadIngest()
	if err != nil {
		return err
	}

	logger := newLogger(cfg.Log)
	slog.SetDefault(logger)

	// Emit boot-visible config before any I/O so a misconfigured deploy
	// surfaces in `fly logs` even when a downstream dependency fails.
	// Secrets (DSN, API keys) are deliberately excluded.
	logger.Info("ingest starting",
		"listen_addr", cfg.Ingest.ListenAddr,
		"jwks_url", cfg.Ingest.JWKSURL,
		"expected_issuer", cfg.Ingest.ExpectedIssuer,
		"service_name", cfg.Ingest.ServiceName,
	)
	if cfg.Ingest.ExpectedIssuer == "" {
		logger.Warn("expected_issuer not set — iss claim will not be enforced (development only)")
	}

	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := pgxpool.New(ctx, cfg.Audit.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	durableSink := durable.New(pool)
	if err := durableSink.Migrate(ctx); err != nil {
		return err
	}

	stderrSink := audit.NewStderrJSONSink()
	asyncStderr := audit.NewAsyncSink(stderrSink, 1024)
	emitter := audit.New(audit.NewMultiSink(asyncStderr, durableSink))

	jwks := ingest.NewJWKSFetcher(cfg.Ingest.JWKSURL,
		&http.Client{Timeout: 5 * time.Second}, time.Hour)

	handler := ingest.NewHandler(emitter, cfg.Ingest.ServiceName, logger)
	authed := ingest.Middleware(jwks.KeyByID, cfg.Ingest.ExpectedIssuer, handler)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("/metering/events", authed)

	srv := &http.Server{
		Addr:              cfg.Ingest.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(),
			15*time.Second)
		defer shutdownCancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	logger.Info("ingest listening", "addr", cfg.Ingest.ListenAddr)
	return srv.ListenAndServe()
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
