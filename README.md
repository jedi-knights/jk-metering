# jk-metering

The metering plane for the Jedi Knights portfolio. Two binaries from one
repo per
[identity-platform-go ADR-0019](https://github.com/jedi-knights/identity-platform-go/blob/main/docs/adr/0019-usage-accounting-and-billing.md):

| Binary | Built from | Role |
|---|---|---|
| `jk-metering` | `cmd/worker` | Polls the `audit_events` table written by [`go-platform/audit/durable`](https://github.com/jedi-knights/go-platform/tree/main/audit/durable), transforms each event into a Lago event, posts to [Lago's Event API](https://docs.getlago.com/api-reference/events/create-an-event). |
| `jk-metering-ingest` | `cmd/ingest` | HTTP entry point so web apps / SPAs can emit billable events through `POST /metering/events` without importing `go-platform/audit`. Validates RS256 bearer tokens against the identity-platform-go JWKS; derives `actor_type` / `actor_id` / `subject_id` from the token; emits through the same durable sink the worker drains. |

- **Language:** Go
- **Deploy:** Fly.io — `fly.toml` for the worker, `fly.ingest.toml` for ingest
- **Source / sink:** Postgres `audit_events` (idempotent via `consumed_at`); Lago Event API (idempotent via `transaction_id`)

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
cmd/
├── worker/main.go     composition root for jk-metering (worker)
└── ingest/main.go     composition root for jk-metering-ingest (HTTP)
internal/
├── config/            viper-based env config (METERING_*)
├── domain/            AuditEvent + LagoEvent value types
├── ports/             EventSource + MeterSink interfaces
├── application/       (worker only)
│   ├── transformer    audit event → Lago event (pure, no I/O)
│   └── service        poll → transform → push → mark loop
├── ingest/            (ingest only)
│   ├── handler        POST /metering/events
│   ├── auth           RS256 bearer-token middleware
│   └── jwks           cached JWKS fetcher
└── adapters/outbound/
    ├── postgres/      EventSource backed by audit_events
    └── lago/          MeterSink posting to Lago Event API
```

The transformer is a pure property pump — every audit event becomes
one Lago event with `code = "usage"` and every envelope field flattened
into Lago `properties`. All SKU discrimination happens via Lago billable-
metric filters on `event_type`, `resource_kind`, `resource_parent`,
`resource_path`, `actor_type`, etc. Adding new SKUs requires zero
metering-shim changes — only Lago admin configuration.

## Configuration

All variables under the `METERING_` prefix. Both binaries share the
audit DSN and logging configuration; each has its own additional set.

### Worker (`jk-metering`)

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

### Ingest (`jk-metering-ingest`)

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `METERING_AUDIT_DSN` | yes | — | Postgres DSN for the `audit_events` table (shared with worker) |
| `METERING_INGEST_LISTEN_ADDR` | no | `:8090` | HTTP bind address |
| `METERING_INGEST_JWKS_URL` | yes | — | Identity-platform-go JWKS URL |
| `METERING_INGEST_EXPECTED_ISSUER` | no | — | Required `iss` claim; unset disables issuer enforcement (dev only) |
| `METERING_INGEST_SERVICE_NAME` | no | `jk-metering-ingest` | Stamped on `Event.Service` |
| `METERING_LOG_LEVEL` | no | `info` | shared |
| `METERING_LOG_FORMAT` | no | `json` | shared |

## HTTP API (ingest)

### `POST /metering/events`

Emit a billable event. The request body carries the surface
identifiers; the server derives every audit-envelope field that needs
trust (actor identity, timestamp, event ID) from the bearer token and
its own clock.

**Auth.** RS256 bearer token in `Authorization: Bearer <token>`. The
token must:

- Sign with a key present in the configured JWKS.
- Carry `iss` matching `METERING_INGEST_EXPECTED_ISSUER` when that env
  var is set.
- Include either `metering:emit` (any parent) or
  `metering:emit:<resource_parent>` in `scope`.

**Request body:**

```json
{
  "event_type": "feature_used",
  "resource_kind": "feature",
  "resource_id": "pdf_export",
  "resource_parent": "billpayer",
  "resource_path": "billpayer/feature/pdf_export",
  "action": "use",
  "attrs": { "size_kb": 47 }
}
```

| Field | Required | Notes |
|---|---|---|
| `event_type` | yes | Free-form, e.g. `feature_used`, `application_started` |
| `resource_kind` | yes | One of the ADR-0019 enum values (`feature`, `application`, …) |
| `resource_parent` | yes | The surface the principal is authorised to emit for |
| `resource_path` | yes | Must begin with `resource_parent + "/"` (or equal `resource_parent`) |
| `resource_id` | no | Leaf identifier |
| `action` | no | Defaults to `use` |
| `attrs` | no | Free-form property bag forwarded verbatim to Lago |

**Response codes:**

| Status | Meaning |
|---|---|
| 202 Accepted | Event persisted to `audit_events`; worker will push to Lago on next tick |
| 400 Bad Request | Malformed JSON, missing required field, or path/parent mismatch |
| 401 Unauthorized | Missing / invalid / wrong-issuer bearer token |
| 403 Forbidden | Token lacks `metering:emit:<parent>` (or `metering:emit`) scope |
| 500 Internal Server Error | Audit durable sink could not persist the event |
| 405 Method Not Allowed | Anything other than `POST` |

### `GET /health`

Unauthenticated liveness probe. Returns `{"status":"ok"}`.

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

Two Fly apps from one repo:

```bash
# Worker (no public HTTP)
fly secrets -a jk-metering set \
  METERING_AUDIT_DSN=... \
  METERING_LAGO_BASE_URL=... \
  METERING_LAGO_API_KEY=...
fly deploy --remote-only -c fly.toml

# Ingest (HTTP, behind the gateway)
fly secrets -a jk-metering-ingest set \
  METERING_AUDIT_DSN=... \
  METERING_INGEST_JWKS_URL=https://auth-server.internal/.well-known/jwks.json \
  METERING_INGEST_EXPECTED_ISSUER=https://auth-server.internal
fly deploy --remote-only -c fly.ingest.toml
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
