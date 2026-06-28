#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "$ROOT_DIR/test-harness/lib/common.sh"
source "$ROOT_DIR/test-harness/lib/redpanda.sh"

PIDS=()
SERVICE_NAMES=()
SERVICE_PIDS=()

source "$ROOT_DIR/test-harness/lib/exchange-services.sh"

print_failure_context() {
  local lines="${E2E_LOG_TAIL_LINES:-120}"
  local log_file
  local found_logs=0

  echo >&2
  echo "e2e smoke failed; collecting diagnostics" >&2

  if command -v docker >/dev/null 2>&1; then
    echo >&2
    echo "docker compose services:" >&2
    compose ps >&2 || true
  fi

  echo >&2
  echo "service log tails (last ${lines} lines):" >&2
  for log_file in "$LOG_DIR"/*.log; do
    [[ -e "$log_file" ]] || continue
    found_logs=1
    echo "----- $(basename "$log_file") -----" >&2
    tail -n "$lines" "$log_file" >&2 || true
  done

  if ((found_logs == 0)); then
    echo "no service logs found in $LOG_DIR" >&2
  fi

  if command -v docker >/dev/null 2>&1; then
    echo >&2
    echo "infra log tails (last ${lines} lines):" >&2
    compose logs --no-color --tail "$lines" >&2 || true
  fi
}

cleanup() {
  if ((${#PIDS[@]} > 0)); then
    kill "${PIDS[@]}" >/dev/null 2>&1 || true
    wait "${PIDS[@]}" >/dev/null 2>&1 || true
  fi
}

on_exit() {
  local status=$?

  trap - EXIT
  if ((status != 0)); then
    print_failure_context
  fi

  cleanup
  exit "$status"
}

trap on_exit EXIT

publish_mark_ingress_input() {
  local now_ms
  local valid_until_ms

  now_ms="$(($(date +%s) * 1000))"
  valid_until_ms="$((now_ms + 60000))"

  echo "queuing mark price ingress input $E2E_MARK_INPUT_ID"
  (
    cd "$ROOT_DIR"
    env \
      DATABASE_URL="$DATABASE_URL" \
      ENGINE_INPUT_TOPIC="engine.input" \
      "$ROOT_DIR/target/debug/engine-ingress" mark-price \
        --input-id "$E2E_MARK_INPUT_ID" \
        --market-id 1 \
        --mark-price 100 \
        --index-price 100 \
        --source-timestamp-ms "$now_ms" \
        --published-at-ms "$now_ms" \
        --valid-until-ms "$valid_until_ms" \
        --source-sequence "$now_ms" \
        --source-status VALID
  )
}

wait_for_migrations() {
  wait_until database-migrations main_database_migrations_ready
}

main_database_migrations_ready() {
  compose exec -T postgres psql -U postgres -d exchange -tAc \
    "SELECT to_regclass('public.markets') IS NOT NULL" | grep -q t
}

seed_smoke_markets_sql() {
  cat <<'EOF_SQL'
INSERT INTO markets(
    market_id,
    market_name,
    base_asset,
    quote_asset,
    decimal_base,
    decimal_quote,
    last_traded_price
)
VALUES
    (1,'SOL-PERP','SOL'::asset_type,'USDC'::asset_type,9,6,0),
    (2,'ETH-PERP','ETH'::asset_type,'USDC'::asset_type,9,6,0)
ON CONFLICT(market_id)
DO UPDATE
SET market_name=EXCLUDED.market_name,
    base_asset=EXCLUDED.base_asset,
    quote_asset=EXCLUDED.quote_asset,
    decimal_base=EXCLUDED.decimal_base,
    decimal_quote=EXCLUDED.decimal_quote;
EOF_SQL
}

seed_smoke_markets_in_db() {
  local service="$1"
  local user="$2"
  local database="$3"

  seed_smoke_markets_sql | compose exec -T "$service" \
    psql -U "$user" -d "$database" >/dev/null
}

seed_smoke_markets() {
  echo "seeding smoke markets before ingress inputs"
  seed_smoke_markets_in_db postgres postgres exchange
}

seed_timeseries_smoke_markets() {
  if ! timeseries_markets_table_ready; then
    echo "timeseries database has no markets table; skipping market seed"
    return
  fi

  echo "seeding timeseries smoke markets"
  seed_smoke_markets_in_db timescaledb "$TIMESCALE_USER" "$TIMESCALE_DB"
}

run_exchange_tests() {
  echo "applying Postgres migrations for Rust tests"
  (
    cd "$ROOT_DIR"
    env DATABASE_URL="$DATABASE_URL" sqlx migrate run --source crates/db/migrations
  )

  echo "running Rust exchange tests"
  (
    cd "$ROOT_DIR"
    env \
      DATABASE_URL="$DATABASE_URL" \
      TIMESERIES_DATABASE_URL="$TIMESERIES_DATABASE_URL" \
      S3_ENDPOINT="$S3_ENDPOINT" \
      S3_REGION="$S3_REGION" \
      S3_BUCKET="$S3_BUCKET" \
      S3_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID" \
      S3_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY" \
      cargo test --workspace
  )
}

need_command docker
need_command cargo
need_command curl
need_command sqlx

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log

echo "checking prepared e2e infra"
echo "expected Redpanda: 127.0.0.1:$REDPANDA_PORT"
echo "expected TimescaleDB: $TIMESERIES_DATABASE_URL"
echo "expected MinIO: $S3_ENDPOINT bucket $S3_BUCKET"
wait_for_compose_infra
wait_for_storage_setup
assert_e2e_topics_ready

run_exchange_tests

echo "building e2e binaries"
(
  cd "$ROOT_DIR"
  cargo build -p wallet -p projector -p timeseries -p ws -p ledger -p server -p engine-ingress -p e2e-smoke
)

echo "starting exchange services"
start_service wallet
start_service projector
start_service timeseries
start_service ledger
start_service ws
start_service server

sleep "${E2E_SERVICE_STARTUP_GRACE_SECONDS:-5}"
if ! check_services_alive "startup"; then
  exit 1
fi
wait_for_migrations
wait_for_timeseries_migrations
seed_smoke_markets
seed_timeseries_smoke_markets
publish_mark_ingress_input

echo "running e2e smoke driver"
driver_status=0
(
  cd "$ROOT_DIR"
  env \
    DATABASE_URL="$DATABASE_URL" \
    TIMESERIES_DATABASE_URL="$TIMESERIES_DATABASE_URL" \
    E2E_SERVER_URL="$API_URL" \
    E2E_WS_URL="$WS_URL" \
    E2E_REDPANDA_BROKERS="127.0.0.1:${REDPANDA_PORT}" \
    E2E_MARK_INPUT_ID="$E2E_MARK_INPUT_ID" \
    cargo run -p e2e-smoke
) || driver_status=$?

service_status=0
check_services_alive "the smoke driver" || service_status=$?

log_status=0
scan_service_logs || log_status=$?

if ((driver_status != 0)); then
  echo "e2e smoke driver failed with status $driver_status" >&2
  exit "$driver_status"
fi

if ((service_status != 0)); then
  echo "one or more exchange services exited during the smoke run" >&2
  exit 1
fi

if ((log_status != 0)); then
  echo "serious error patterns were found in service logs" >&2
  exit 1
fi

echo "e2e smoke complete"
