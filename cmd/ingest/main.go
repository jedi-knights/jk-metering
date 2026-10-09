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
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/jedi-knights/go-platform/audit"
	"github.com/jedi-knights/go-platform/audit/durable"
	"github.com/jedi-knights/go-platform/httpserver"
	platformotel "github.com/jedi-knights/go-platform/otel"

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

	stopObs, err := setupObservability(ctx, logger)
	if err != nil {
		return err
	}
	defer stopObs()

	pool, err := pgxpool.New(ctx, cfg.Audit.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()

	emitter, err := buildEmitter(ctx, pool)
	if err != nil {
		return err
	}

	srv := buildServer(cfg, logger, emitter)
	go waitForShutdown(ctx, srv)

	logger.Info("ingest listening", "addr", cfg.Ingest.ListenAddr)
	return srv.ListenAndServe()
}

// setupObservability wires the OTel tracer + meter and starts the
// Prometheus scrape listener on :9464. The returned function runs
// both shutdowns in LIFO order so the caller defers one function.
// Extracted from [run] to keep its cyclomatic complexity within the
// project limit.
func setupObservability(ctx context.Context, logger *slog.Logger) (func(), error) {
	obs, err := platformotel.New(ctx, platformotel.Config{
		ServiceName: "jk-metering-ingest",
	})
	if err != nil {
		return nil, err
	}
	metricsSrv, err := httpserver.StartMetricsServer("", "", obs.PromHandler)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = obs.Shutdown(shutdownCtx)
		return nil, err
	}
	logger.Info("metrics endpoint ready", "addr", httpserver.DefaultMetricsAddr, "path", httpserver.DefaultMetricsPath)
	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if sErr := metricsSrv.Shutdown(shutdownCtx); sErr != nil {
			logger.Error("metrics shutdown error", "error", sErr)
		}
		if sErr := obs.Shutdown(shutdownCtx); sErr != nil {
			logger.Error("observability shutdown error", "error", sErr)
		}
	}, nil
}

// buildEmitter composes the stderr + durable Postgres audit sinks
// behind a single audit.Emitter. Extracted from [run] so the entry
// point stays under the gocyclo budget.
func buildEmitter(ctx context.Context, pool *pgxpool.Pool) (audit.Emitter, error) {
	durableSink := durable.New(pool)
	if err := durableSink.Migrate(ctx); err != nil {
		return nil, err
	}
	stderrSink := audit.NewStderrJSONSink()
	asyncStderr := audit.NewAsyncSink(stderrSink, 1024)
	return audit.New(audit.NewMultiSink(asyncStderr, durableSink)), nil
}

// buildServer assembles the HTTP router: health route, authenticated
// /metering/events route, and the otelhttp wrapper that produces a
// server span per inbound request.
func buildServer(cfg *config.IngestConfig, logger *slog.Logger, emitter audit.Emitter) *http.Server {
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

	// otelhttp wraps the mux so every inbound request becomes a server
	// span; traceparent headers from the emitter are honored by the
	// W3C TraceContext propagator that go-platform/otel registers.
	tracedMux := otelhttp.NewHandler(mux, "jk-metering-ingest",
		otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
			return r.Method + " " + r.URL.Path
		}),
	)

	return &http.Server{
		Addr:              cfg.Ingest.ListenAddr,
		Handler:           tracedMux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// waitForShutdown blocks until ctx is canceled, then gracefully
// shuts the server down with a bounded timeout.
func waitForShutdown(ctx context.Context, srv *http.Server) {
	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(),
		15*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

// newLogger builds the service's structured logger. The base handler
// is wrapped by [platformotel.SpanContextHandler] so every record
// carries trace_id and span_id from any OTel span on the context.
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
