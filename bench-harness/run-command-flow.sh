#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "$ROOT_DIR/test-harness/lib/common.sh"
source "$ROOT_DIR/test-harness/lib/redpanda.sh"

PIDS=()
SERVICE_NAMES=()
SERVICE_PIDS=()

source "$ROOT_DIR/test-harness/lib/exchange-services.sh"

COMMANDS="${EXCHANGE_BENCH_COMMANDS:-10000}"
WARMUP="${EXCHANGE_BENCH_WARMUP:-1000}"
CONCURRENCY="${EXCHANGE_BENCH_CONCURRENCY:-1}"
RESULT_ROOT="${EXCHANGE_BENCH_RESULT_DIR:-$ROOT_DIR/target/exchange-bench}"
RUN_ID="${EXCHANGE_BENCH_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RESULT_DIR="$RESULT_ROOT/$RUN_ID"
LOG_DIR="${EXCHANGE_BENCH_LOG_DIR:-$RESULT_DIR/logs}"
BUILD_PROFILE="${EXCHANGE_BENCH_PROFILE:-release}"

if [[ "$BUILD_PROFILE" == "release" ]]; then
  CARGO_PROFILE_ARGS=(--release)
  SERVICE_BIN_DIR="$ROOT_DIR/target/release"
elif [[ "$BUILD_PROFILE" == "debug" ]]; then
  CARGO_PROFILE_ARGS=()
  SERVICE_BIN_DIR="$ROOT_DIR/target/debug"
else
  echo "unknown EXCHANGE_BENCH_PROFILE: $BUILD_PROFILE" >&2
  exit 2
fi
export SERVICE_BIN_DIR

print_failure_context() {
  local lines="${EXCHANGE_BENCH_LOG_TAIL_LINES:-120}"
  local log_file

  echo >&2
  echo "exchange benchmark failed; collecting diagnostics" >&2

  if command -v docker >/dev/null 2>&1; then
    echo >&2
    echo "docker compose services:" >&2
    compose ps >&2 || true
  fi

  echo >&2
  echo "service log tails (last ${lines} lines):" >&2
  for log_file in "$LOG_DIR"/*.log; do
    [[ -e "$log_file" ]] || continue
    echo "----- $(basename "$log_file") -----" >&2
    tail -n "$lines" "$log_file" >&2 || true
  done
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

main_database_migrations_ready() {
  compose exec -T postgres psql -U postgres -d exchange -tAc \
    "SELECT to_regclass('public.markets') IS NOT NULL" | grep -q t
}

wait_for_migrations() {
  wait_until database-migrations main_database_migrations_ready
}

seed_benchmark_markets_sql() {
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
    (1,'SOL-PERP','SOL'::asset_type,'USDC'::asset_type,9,6,0)
ON CONFLICT(market_id)
DO UPDATE
SET market_name=EXCLUDED.market_name,
    base_asset=EXCLUDED.base_asset,
    quote_asset=EXCLUDED.quote_asset,
    decimal_base=EXCLUDED.decimal_base,
    decimal_quote=EXCLUDED.decimal_quote;
EOF_SQL
}

seed_benchmark_markets_in_db() {
  local service="$1"
  local user="$2"
  local database="$3"

  seed_benchmark_markets_sql | compose exec -T "$service" \
    psql -U "$user" -d "$database" >/dev/null
}

seed_benchmark_markets() {
  echo "seeding benchmark market"
  seed_benchmark_markets_in_db postgres postgres exchange

  if timeseries_markets_table_ready; then
    seed_benchmark_markets_in_db timescaledb "$TIMESCALE_USER" "$TIMESCALE_DB"
  fi
}

wait_for_server() {
  wait_until exchange-server-listening \
    curl -sS -o /dev/null "$API_URL/markets/1/orderbook?depth=1"
}

start_benchmark_engine_peer() {
  local log_file="$LOG_DIR/exchange-bench-engine-peer.log"
  local pid

  (
    cd "$ROOT_DIR"
    env \
      REDPANDA_BROKERS="127.0.0.1:${REDPANDA_PORT}" \
      ENGINE_INPUT_TOPIC="engine.input" \
      ENGINE_REPLIES_TOPIC="engine.replies" \
      ENGINE_EVENTS_TOPIC="engine.events" \
      "$SERVICE_BIN_DIR/exchange-bench-engine-peer"
  ) >"$log_file" 2>&1 &

  pid="$!"
  register_service_pid "exchange-bench-engine-peer" "$pid" "$log_file"
}

need_command cargo
need_command curl
need_command sqlx

mkdir -p "$RESULT_DIR" "$LOG_DIR"
rm -f "$LOG_DIR"/*.log

if [[ "${EXCHANGE_BENCH_SKIP_INFRA_UP:-0}" != "1" ]]; then
  "$ROOT_DIR/test-harness/infra.sh" up
else
  wait_for_compose_infra
  wait_for_storage_setup
  assert_e2e_topics_ready
fi

echo "applying Postgres migrations for benchmark build"
(
  cd "$ROOT_DIR"
  env DATABASE_URL="$DATABASE_URL" sqlx migrate run --source crates/db/migrations
)

echo "building exchange benchmark binaries and services"
(
  cd "$ROOT_DIR"
  env \
    DATABASE_URL="$DATABASE_URL" \
    TIMESERIES_DATABASE_URL="$TIMESERIES_DATABASE_URL" \
    cargo build \
      -p wallet \
      -p projector \
      -p timeseries \
      -p ws \
      -p ledger \
      -p server \
      -p exchange-bench-engine-peer \
      -p exchange-bench-driver \
      "${CARGO_PROFILE_ARGS[@]}"
)

echo "starting exchange benchmark services"
start_service wallet
start_service projector
start_service timeseries
start_service ledger
start_service ws
start_service server
start_benchmark_engine_peer

sleep "${EXCHANGE_BENCH_SERVICE_STARTUP_GRACE_SECONDS:-5}"
check_services_alive "benchmark startup"
wait_for_migrations
wait_for_timeseries_migrations
seed_benchmark_markets
wait_for_server

echo "running exchange command-flow benchmark"
(
  cd "$ROOT_DIR"
  env \
    EXCHANGE_BENCH_SERVER_URL="$API_URL" \
    EXCHANGE_BENCH_RUN_ID="$RUN_ID" \
    "$SERVICE_BIN_DIR/exchange-bench-driver" \
      --commands "$COMMANDS" \
      --warmup "$WARMUP" \
      --concurrency "$CONCURRENCY" \
      > "$RESULT_DIR/command-flow.json"
)

check_services_alive "the benchmark driver"
scan_service_logs

cat > "$RESULT_DIR/manifest.json" <<EOF_JSON
{
  "run_id": "$RUN_ID",
  "commands": $COMMANDS,
  "warmup": $WARMUP,
  "concurrency": $CONCURRENCY,
  "server_url": "$API_URL",
  "redpanda_brokers": "127.0.0.1:$REDPANDA_PORT",
  "result_dir": "$RESULT_DIR"
}
EOF_JSON

echo "exchange benchmark results: $RESULT_DIR"
