#!/usr/bin/env bash
# verify-metering.sh — E6-S3 end-to-end metering pipeline verification.
#
# Emits a synthetic metering event through jk-metering-ingest, waits for
# the worker to drain it (audit_events.consumed_at goes non-null), and
# asserts Lago has a matching event keyed by the ingest-generated
# transaction_id.
#
# Design notes
# ------------
# The ingest handler generates event_id server-side (ULID). The script
# cannot predict it, so correlation flows through a probe_id we set in
# attrs and echo into resource_id. Step 2 looks up the row by probe_id
# to learn event_id, then step 3 polls that row for consumed_at, and
# step 4 queries Lago by transaction_id (== event_id).
#
# Required env
# ------------
#   METERING_INGEST_URL   Public URL for POST /metering/events
#                         (e.g. https://jk-api-gateway.fly.dev)
#   METERING_TEST_TOKEN   Bearer token with the metering:emit scope (or
#                         the resource_parent-scoped metering:emit:verify)
#   METERING_AUDIT_DSN    Postgres DSN for the audit_events table
#   LAGO_BASE_URL         Lago API root (e.g. https://jk-lago-api.fly.dev)
#   LAGO_API_KEY          Lago API key (Bearer)
#
# Optional env
# ------------
#   POLL_TIMEOUT_SECONDS   Total wait per polling step. Default 30.
#   POLL_INTERVAL_SECONDS  Poll cadence. Default 2.
#
# Exit codes
# ----------
#   0  verified
#   1  verification failed (step-specific detail on stderr)
#   2  configuration error (missing env, missing binary)

set -euo pipefail

# ---- helpers ---------------------------------------------------------------

die() { printf 'verify-metering: %s\n' "$*" >&2; exit "${2:-1}"; }
step() { printf '\n== %s ==\n' "$*" >&2; }
require_env() {
  for var in "$@"; do
    if [[ -z "${!var:-}" ]]; then die "missing required env var: $var" 2; fi
  done
}
require_bin() {
  for bin in "$@"; do
    if ! command -v "$bin" >/dev/null 2>&1; then die "missing required binary: $bin" 2; fi
  done
}

# ---- preflight -------------------------------------------------------------

require_bin curl jq psql openssl
require_env METERING_INGEST_URL METERING_TEST_TOKEN METERING_AUDIT_DSN \
            LAGO_BASE_URL LAGO_API_KEY

POLL_TIMEOUT_SECONDS="${POLL_TIMEOUT_SECONDS:-30}"
POLL_INTERVAL_SECONDS="${POLL_INTERVAL_SECONDS:-2}"

# Sanity-cap the loop iterations. Every polling loop below runs at most
# MAX_POLL_ITERATIONS times so a misconfigured timeout cannot hang CI.
MAX_POLL_ITERATIONS=$(( POLL_TIMEOUT_SECONDS / POLL_INTERVAL_SECONDS + 1 ))

PROBE_ID="probe-$(date +%s)-$(openssl rand -hex 4)"

# ---- step 1: emit --------------------------------------------------------

step "1/4 emit probe event via jk-metering-ingest ($PROBE_ID)"

PAYLOAD=$(jq -nc \
  --arg pid "$PROBE_ID" \
  '{
     event_type: "verify.probe",
     resource_kind: "probe",
     resource_id: $pid,
     resource_parent: "verify",
     resource_path: "verify/probe",
     action: "verify",
     attrs: { probe_id: $pid }
   }')

HTTP_CODE=$(curl -sS -o /tmp/verify-metering.emit.body -w '%{http_code}' \
  -X POST "${METERING_INGEST_URL%/}/metering/events" \
  -H "Authorization: Bearer $METERING_TEST_TOKEN" \
  -H 'Content-Type: application/json' \
  --data-raw "$PAYLOAD")

if [[ "$HTTP_CODE" != "202" ]]; then
  cat /tmp/verify-metering.emit.body >&2 || true
  die "ingest returned $HTTP_CODE, expected 202 (see body above)"
fi
printf 'ingest accepted probe (HTTP 202)\n' >&2

# ---- step 2: locate audit row --------------------------------------------

step "2/4 locate audit_events row by probe_id"

EVENT_ID=""
for (( i=1; i<=MAX_POLL_ITERATIONS; i++ )); do
  EVENT_ID=$(psql "$METERING_AUDIT_DSN" -tAX -c \
    "SELECT event_id FROM audit_events
      WHERE payload::jsonb #>> '{attrs,probe_id}' = '$PROBE_ID'
      LIMIT 1" 2>/dev/null || true)
  if [[ -n "$EVENT_ID" ]]; then break; fi
  sleep "$POLL_INTERVAL_SECONDS"
done

if [[ -z "$EVENT_ID" ]]; then
  die "no audit_events row found for probe_id=$PROBE_ID after ${POLL_TIMEOUT_SECONDS}s"
fi
printf 'audit_events row found: event_id=%s\n' "$EVENT_ID" >&2

# ---- step 3: wait for consumed_at ----------------------------------------

step "3/4 wait for worker to mark consumed_at"

CONSUMED=""
for (( i=1; i<=MAX_POLL_ITERATIONS; i++ )); do
  CONSUMED=$(psql "$METERING_AUDIT_DSN" -tAX -c \
    "SELECT consumed_at FROM audit_events
      WHERE event_id = '$EVENT_ID'" 2>/dev/null || true)
  if [[ -n "$CONSUMED" && "$CONSUMED" != "" ]]; then break; fi
  sleep "$POLL_INTERVAL_SECONDS"
done

if [[ -z "$CONSUMED" ]]; then
  die "worker did not set consumed_at within ${POLL_TIMEOUT_SECONDS}s (event_id=$EVENT_ID)"
fi
printf 'worker drained event: consumed_at=%s\n' "$CONSUMED" >&2

# ---- step 4: assert Lago has the event -----------------------------------

step "4/4 assert Lago has a matching event (transaction_id=$EVENT_ID)"

LAGO_URL="${LAGO_BASE_URL%/}/api/v1/events?transaction_id=$EVENT_ID"
LAGO_HTTP=$(curl -sS -o /tmp/verify-metering.lago.body -w '%{http_code}' \
  -H "Authorization: Bearer $LAGO_API_KEY" \
  "$LAGO_URL")

if [[ "$LAGO_HTTP" != "200" ]]; then
  cat /tmp/verify-metering.lago.body >&2 || true
  die "Lago returned $LAGO_HTTP querying transaction_id=$EVENT_ID (see body above)"
fi

MATCHES=$(jq -r '.events | length' /tmp/verify-metering.lago.body)
if [[ "$MATCHES" != "1" ]]; then
  cat /tmp/verify-metering.lago.body >&2
  die "expected 1 Lago event for transaction_id=$EVENT_ID, got $MATCHES"
fi

printf '\nverify-metering: OK (probe_id=%s event_id=%s)\n' "$PROBE_ID" "$EVENT_ID" >&2
