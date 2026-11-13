#!/usr/bin/env bash
# End-to-end smoke test against a running docker compose stack.
# Usage: ./scripts/smoke.sh   (BASE_URL defaults to http://localhost:8000)
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
COMPOSE="${COMPOSE:-docker compose}"

fail() { echo "SMOKE FAIL: $*" >&2; exit 1; }
json() { python3 -c "import sys,json; d=json.load(sys.stdin); print(d$1)"; }

echo "1. health"
curl -fsS "$BASE_URL/healthz" >/dev/null || fail "healthz"
curl -fsS "$BASE_URL/readyz" >/dev/null || fail "readyz"

echo "2. create API key"
KEY=$($COMPOSE run --rm --no-deps -T api python -m jobplatform.cli create-api-key --owner smoke 2>/dev/null | tail -n1)
[[ "$KEY" == jp_* ]] || fail "could not create key"
AUTH="Authorization: Bearer $KEY"

echo "3. submit, poll to success"
JOB=$(curl -fsS -X POST "$BASE_URL/v1/jobs" -H "$AUTH" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: smoke-1' -d '{"type":"io_task","payload":{"duration_ms":200}}' | json "['id']")
for _ in $(seq 1 60); do
  STATUS=$(curl -fsS "$BASE_URL/v1/jobs/$JOB" -H "$AUTH" | json "['status']")
  [[ "$STATUS" == "succeeded" ]] && break
  sleep 0.5
done
[[ "$STATUS" == "succeeded" ]] || fail "job ended as $STATUS"

echo "4. idempotent replay returns the same job"
JOB2=$(curl -fsS -X POST "$BASE_URL/v1/jobs" -H "$AUTH" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: smoke-1' -d '{"type":"io_task","payload":{"duration_ms":200}}' | json "['id']")
[[ "$JOB" == "$JOB2" ]] || fail "idempotency replay created a second job"

echo "5. cancel a delayed job"
DELAYED=$(curl -fsS -X POST "$BASE_URL/v1/jobs" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"type":"io_task","delay_seconds":300}' | json "['id']")
CANCELLED=$(curl -fsS -X POST "$BASE_URL/v1/jobs/$DELAYED/cancel" -H "$AUTH" | json "['status']")
[[ "$CANCELLED" == "cancelled" ]] || fail "cancel returned $CANCELLED"

echo "6. exhaust retries, find it in the DLQ, requeue it"
DEAD=$(curl -fsS -X POST "$BASE_URL/v1/jobs" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"type":"flaky","payload":{"fail_times":50,"error":"fatal"}}' | json "['id']")
for _ in $(seq 1 60); do
  STATUS=$(curl -fsS "$BASE_URL/v1/jobs/$DEAD" -H "$AUTH" | json "['status']")
  [[ "$STATUS" == "dead" ]] && break
  sleep 0.5
done
[[ "$STATUS" == "dead" ]] || fail "flaky job ended as $STATUS"
curl -fsS "$BASE_URL/v1/dlq" -H "$AUTH" | grep -q "$DEAD" || fail "job missing from DLQ"
REQ=$(curl -fsS -X POST "$BASE_URL/v1/dlq/$DEAD/requeue" -H "$AUTH" | json "['status']")
[[ "$REQ" == "queued" ]] || fail "requeue returned $REQ"

echo "7. CPU-bound job"
CPU=$(curl -fsS -X POST "$BASE_URL/v1/jobs" -H "$AUTH" -H 'Content-Type: application/json' \
  -d '{"type":"cpu_report","payload":{"rows":300000,"seed":42}}' | json "['id']")
for _ in $(seq 1 120); do
  STATUS=$(curl -fsS "$BASE_URL/v1/jobs/$CPU" -H "$AUTH" | json "['status']")
  [[ "$STATUS" == "succeeded" ]] && break
  sleep 0.5
done
[[ "$STATUS" == "succeeded" ]] || fail "cpu job ended as $STATUS"

echo "8. rate limit headers and metrics"
curl -fsSi "$BASE_URL/v1/jobs" -H "$AUTH" | grep -qi '^x-ratelimit-remaining' || fail "rate limit headers missing"
curl -fsS "$BASE_URL/metrics" | grep -q '^http_requests_total' || fail "metrics missing"

echo "SMOKE OK"
