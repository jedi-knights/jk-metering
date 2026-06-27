# jk-metering

The metering shim for the Jedi Knights portfolio. Polls the
`audit_events` table written by
[`go-platform/audit/durable`](https://github.com/jedi-knights/go-platform/tree/main/audit/durable),
transforms each event into a Lago event per
[identity-platform-go ADR-0019](https://github.com/jedi-knights/identity-platform-go/blob/main/docs/adr/0019-usage-accounting-and-billing.md),
and posts to [Lago's Event API](https://docs.getlago.com/api-reference/events/create-an-event).

- **Language:** Go
- **Deploy:** Fly.io (worker, no HTTP)
- **Source:** Postgres `audit_events` table (idempotent via `consumed_at`)
- **Sink:** self-hosted Lago Event API (idempotent via `transaction_id`)

## Why it exists

ADR-0019 establishes the audit pipeline as the single event stream for
both compliance audit and usage billing. The `durable` sink in
`go-platform/audit/durable` persists every emission so the bill cannot
have gaps; this service is the consumer that transforms those rows
into Lago events. End-to-end exactly-once is delivered by two
dedupes: this service marks `consumed_at` on success (no re-fetch),
and Lago dedupes by `transaction_id` (no re-bill on retry).

## Architecture

Strict hexagonal, mirroring the rest of the portfolio.

```
cmd/main.go          composition root, signal handling
internal/
├── config/          viper-based env config (METERING_*)
├── domain/          AuditEvent + LagoEvent value types
├── ports/           EventSource + MeterSink interfaces
├── application/
│   ├── transformer  audit event → Lago event (pure, no I/O)
│   └── service      poll → transform → push → mark loop
└── adapters/
    └── outbound/
        ├── postgres/  EventSource backed by audit_events
        └── lago/      MeterSink posting to Lago Event API
```

The transformer is a pure property pump — every audit event becomes
one Lago event with `code = "usage"` and every envelope field flattened
into Lago `properties`. All SKU discrimination happens via Lago billable-
metric filters on `event_type`, `resource_kind`, `resource_parent`,
`resource_path`, `actor_type`, etc. Adding new SKUs requires zero
metering-shim changes — only Lago admin configuration.

## Configuration

All variables under the `METERING_` prefix.

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `METERING_AUDIT_DSN` | yes | — | Postgres DSN for the `audit_events` table |
| `METERING_AUDIT_TABLE` | no | `audit_events` | Override the audit table name |
| `METERING_LAGO_BASE_URL` | yes | — | Lago API root (e.g. `http://lago-api.internal`) |
| `METERING_LAGO_API_KEY` | yes | — | Lago API key (never logged) |
| `METERING_METERING_POLL_INTERVAL_SECONDS` | no | `5` | Polling interval |
| `METERING_METERING_BATCH_SIZE` | no | `100` | Max events per tick |
| `METERING_METERING_BILLING_IDENTITY` | no | `subject` | Field that becomes `external_subscription_id`: `subject` / `actor` / `client` |
| `METERING_LOG_LEVEL` | no | `info` | `debug` / `info` / `warn` / `error` |
| `METERING_LOG_FORMAT` | no | `json` | `json` / `text` |

## Quickstart

```bash
# Run tests with race detector
go test ./... -race -count=1

# Vet
go vet ./...

# Build
go build ./...

# Run locally against a Postgres + Lago that you've already stood up
export METERING_AUDIT_DSN=postgres://localhost:5432/identity?sslmode=disable
export METERING_LAGO_BASE_URL=http://localhost:3000
export METERING_LAGO_API_KEY=test-key
go run ./cmd
```

## Deploy

Fly.io. The included `fly.toml` runs one always-on shared-cpu-1x worker
with no public HTTP listener.

```bash
fly secrets set METERING_AUDIT_DSN=... METERING_LAGO_BASE_URL=... METERING_LAGO_API_KEY=...
fly deploy --remote-only
```

## Idempotency contract

- **Postgres side:** `consumed_at` is set after a successful Lago push.
  A row with `consumed_at NOT NULL` is never re-fetched.
- **Lago side:** `transaction_id` equals the audit `event_id` (ULID).
  Lago dedupes on this, so a retry after a partial failure does not
  produce duplicate Lago events.

Combined, the shim is exactly-once at-rest from emission through
billing. The single edge case is a row marked consumed but never
delivered to Lago — this requires a process crash *between* the Lago
2xx and the UPDATE. The corresponding Lago event is, however,
already accepted (Lago is the source of truth on the billing side); no
duplicate, no gap.

## What's deliberately deferred

- **LISTEN/NOTIFY** for sub-second latency. The polling cap on the
  current shape is `METERING_METERING_POLL_INTERVAL_SECONDS` (default 5s);
  a future PR swaps polling for `LISTEN audit_events_new` once latency
  matters more than implementation simplicity.
- **Bulk ingestion.** Lago supports batch events; the shim sends one
  event per call today. Batching lands when the workload pressure
  justifies it.
- **Retry policy / circuit breaker.** Per-event failures log + count
  but stay on the queue (`consumed_at` is not set), so the next tick
  retries. No exponential backoff or DLQ — both wait until production
  workload calibrates the right thresholds.
- **Customer / subscription provisioning.** The shim assumes the Lago
  customer + subscription already exist when an event arrives. The
  login-ui plan-selection flow (separate work) creates them up front.

## Reference

- [identity-platform-go ADR-0019](https://github.com/jedi-knights/identity-platform-go/blob/main/docs/adr/0019-usage-accounting-and-billing.md) — full design including the
  flexibility-commitment table for adding new billable surfaces.
- [`go-platform/audit/durable`](https://github.com/jedi-knights/go-platform/tree/main/audit/durable) — the source side of this pipeline.
- [Lago Event API docs](https://docs.getlago.com/api-reference/events/create-an-event).

## License

[MIT](LICENSE).
