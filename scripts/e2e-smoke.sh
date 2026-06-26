#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MONOREPO_ROOT="$(cd "$ROOT_DIR/.." && pwd)"
ENGINE_DIR="${E2E_CPP_ENGINE_DIR:-$MONOREPO_ROOT/engine}"
COMPOSE_FILE="$ROOT_DIR/scripts/e2e-compose.yml"
PROJECT_NAME="${E2E_COMPOSE_PROJECT:-exchange-e2e}"
POSTGRES_PORT="${E2E_POSTGRES_PORT:-55432}"
REDPANDA_PORT="${E2E_REDPANDA_PORT:-19092}"
SERVER_PORT="${E2E_SERVER_PORT:-18080}"
WS_PORT="${E2E_WS_PORT:-18081}"
DATABASE_URL="postgres://postgres:postgres@127.0.0.1:${POSTGRES_PORT}/exchange"
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"
API_URL="${SERVER_URL}/api"
WS_URL="ws://127.0.0.1:${WS_PORT}/ws"
E2E_MARK_INPUT_ID="${E2E_MARK_INPUT_ID:-${PROJECT_NAME}-mark-price-smoke}"
LOG_DIR="$ROOT_DIR/target/e2e-smoke"
CPP_ENGINE_BUILD_DIR="${E2E_CPP_ENGINE_BUILD_DIR:-$LOG_DIR/cpp-engine-build}"
CPP_ENGINE_BROKERS="${E2E_CPP_ENGINE_BROKERS:-${CEX_ENGINE_BOOTSTRAP_SERVERS:-127.0.0.1:${REDPANDA_PORT}}}"
CPP_ENGINE_GROUP_ID="${E2E_CPP_ENGINE_GROUP_ID:-${CEX_ENGINE_GROUP_ID:-${PROJECT_NAME}-cpp-engine}}"
CPP_ENGINE_POLL_LIMIT="${E2E_CPP_ENGINE_POLL_LIMIT:-${CEX_ENGINE_POLL_LIMIT:-}}"
CPP_ENGINE_CHECKPOINT_DIR_MANAGED=0
if [[ -n "${E2E_CPP_ENGINE_CHECKPOINT_DIR:-}" ]]; then
  CPP_ENGINE_CHECKPOINT_DIR="$E2E_CPP_ENGINE_CHECKPOINT_DIR"
elif [[ -n "${CEX_ENGINE_CHECKPOINT_DIR:-}" ]]; then
  CPP_ENGINE_CHECKPOINT_DIR="$CEX_ENGINE_CHECKPOINT_DIR"
else
  CPP_ENGINE_CHECKPOINT_DIR="$LOG_DIR/cpp-engine-checkpoints"
  CPP_ENGINE_CHECKPOINT_DIR_MANAGED=1
fi
CPP_ENGINE_MARKETS_CONFIG_MANAGED=0
if [[ -n "${E2E_CPP_ENGINE_MARKETS_CONFIG:-}" ]]; then
  CPP_ENGINE_MARKETS_CONFIG="$E2E_CPP_ENGINE_MARKETS_CONFIG"
elif [[ -n "${CEX_ENGINE_MARKETS_CONFIG:-}" ]]; then
  CPP_ENGINE_MARKETS_CONFIG="$CEX_ENGINE_MARKETS_CONFIG"
else
  CPP_ENGINE_MARKETS_CONFIG="$LOG_DIR/cpp-engine-markets.conf"
  CPP_ENGINE_MARKETS_CONFIG_MANAGED=1
fi
PIDS=()
SERVICE_NAMES=()
SERVICE_PIDS=()

print_failure_context() {
  local lines="${E2E_LOG_TAIL_LINES:-120}"
  local log_file
  local found_logs=0

  echo >&2
  echo "e2e smoke failed; collecting diagnostics" >&2

  if command -v docker >/dev/null 2>&1; then
    echo >&2
    echo "docker compose services:" >&2
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" ps >&2 || true
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
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" logs --no-color --tail "$lines" >&2 || true
  fi
}

cleanup() {
  if ((${#PIDS[@]} > 0)); then
    kill "${PIDS[@]}" >/dev/null 2>&1 || true
    wait "${PIDS[@]}" >/dev/null 2>&1 || true
  fi

  if [[ "${E2E_KEEP_INFRA:-0}" != "1" ]]; then
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
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

need_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_until() {
  local label="$1"
  shift
  local attempts=60

  for _ in $(seq 1 "$attempts"); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "timed out waiting for $label" >&2
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" ps >&2 || true
  exit 1
}

create_topic() {
  local topic="$1"
  local partitions="$2"
  local attempt
  shift 2

  for attempt in $(seq 1 10); do
    if docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda \
      rpk topic create --if-not-exists --partitions "$partitions" --brokers localhost:9092 "$@" "$topic" >/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "failed to create topic $topic after retries" >&2
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda \
    rpk topic create --if-not-exists --partitions "$partitions" --brokers localhost:9092 "$@" "$topic"
}

set_topic_config() {
  local topic="$1"
  local config="$2"
  local attempt

  for attempt in $(seq 1 10); do
    if docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda \
      rpk topic alter-config "$topic" --set "$config" --brokers localhost:9092 >/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "failed to set topic config $topic $config after retries" >&2
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda \
    rpk topic alter-config "$topic" --set "$config" --brokers localhost:9092
}

topic_partitions() {
  local topic="$1"

  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda \
    rpk topic describe "$topic" --brokers localhost:9092 | awk '$1 == "PARTITIONS" { print $2; exit }'
}

assert_topic_partitions() {
  local topic="$1"
  local expected="$2"
  local actual
  local attempt

  for attempt in $(seq 1 10); do
    actual="$(topic_partitions "$topic" || true)"
    if [[ "$actual" == "$expected" ]]; then
      return 0
    fi
    sleep 1
  done

  echo "topic $topic must have $expected partition(s), found ${actual:-unknown}" >&2
  exit 1
}

create_engine_input_topic() {
  create_topic engine.input 1 -c retention.ms=1800000
  set_topic_config engine.input retention.ms=1800000
  assert_topic_partitions engine.input 1
}

register_service_pid() {
  local name="$1"
  local pid="$2"
  local log_file="$3"

  PIDS+=("$pid")
  SERVICE_NAMES+=("$name")
  SERVICE_PIDS+=("$pid")
  echo "started $name (pid $pid), log: $log_file"
}

start_service() {
  local binary="$1"
  local log_file="$LOG_DIR/${binary}.log"
  local pid

  (
    cd "$ROOT_DIR"
    env \
      DATABASE_URL="$DATABASE_URL" \
      SERVER_URL="$SERVER_URL" \
      SERVER_HOST="127.0.0.1" \
      SERVER_PORT="$SERVER_PORT" \
      WS_HOST="127.0.0.1" \
      WS_PORT="$WS_PORT" \
      JWT_SECRET="e2e-secret" \
      REDPANDA_BROKERS="127.0.0.1:${REDPANDA_PORT}" \
      SERVER_REPLY_PARTITION="0" \
      REQUEST_WAIT_TIMEOUT_MS="${E2E_REQUEST_WAIT_TIMEOUT_MS:-8000}" \
      "$ROOT_DIR/target/debug/$binary"
  ) >"$log_file" 2>&1 &

  pid="$!"
  register_service_pid "$binary" "$pid" "$log_file"
}

find_cpp_engine_app() {
  local candidate

  for candidate in \
    "$CPP_ENGINE_BUILD_DIR/engine_app" \
    "$CPP_ENGINE_BUILD_DIR/Debug/engine_app" \
    "$CPP_ENGINE_BUILD_DIR/Release/engine_app" \
    "$CPP_ENGINE_BUILD_DIR/RelWithDebInfo/engine_app" \
    "$CPP_ENGINE_BUILD_DIR/MinSizeRel/engine_app"; do
    if [[ -x "$candidate" ]]; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  if [[ -d "$CPP_ENGINE_BUILD_DIR" ]]; then
    find "$CPP_ENGINE_BUILD_DIR" -maxdepth 3 -type f -name engine_app -perm -111 -print \
      2>/dev/null | head -n 1
  fi
}

prepare_cpp_engine_markets_config() {
  if [[ "$CPP_ENGINE_MARKETS_CONFIG_MANAGED" != "1" ]]; then
    if [[ ! -f "$CPP_ENGINE_MARKETS_CONFIG" ]]; then
      echo "C++ engine markets config not found: $CPP_ENGINE_MARKETS_CONFIG" >&2
      exit 1
    fi
    return
  fi

  mkdir -p "$(dirname "$CPP_ENGINE_MARKETS_CONFIG")"
  cat >"$CPP_ENGINE_MARKETS_CONFIG" <<'EOF_MARKETS'
[[market]]
market_id = 1
market_name = SOL-PERP
tick_size = 1
lot_size = 1
min_quantity = 1
max_quantity = 1000000
min_price = 1
max_price = 1000000
ring_capacity_ticks = 1000
threshold_percentage = 10
initial_base_tick = 0
price_scale = 0
quantity_scale = 0
maker_fee_rate = 0
taker_fee_rate = 0
trading_enabled = true

[[market]]
market_id = 2
market_name = ETH-PERP
tick_size = 1
lot_size = 1
min_quantity = 1
max_quantity = 1000000
min_price = 1
max_price = 1000000
ring_capacity_ticks = 1000
threshold_percentage = 10
initial_base_tick = 0
price_scale = 0
quantity_scale = 0
maker_fee_rate = 0
taker_fee_rate = 0
trading_enabled = true
EOF_MARKETS
}

build_cpp_engine_app() {
  if [[ ! -d "$ENGINE_DIR" ]]; then
    echo "e2e smoke requires the C++ engine checkout at $ENGINE_DIR" >&2
    exit 1
  fi

  local build_type="${E2E_CPP_ENGINE_BUILD_TYPE:-${CMAKE_BUILD_TYPE:-Debug}}"
  local cxx_standard="${E2E_CPP_ENGINE_CXX_STANDARD:-${CMAKE_CXX_STANDARD:-20}}"
  local build_args=(--build "$CPP_ENGINE_BUILD_DIR" --target engine_app --parallel)
  if [[ -n "${E2E_CPP_ENGINE_BUILD_JOBS:-}" ]]; then
    build_args+=("$E2E_CPP_ENGINE_BUILD_JOBS")
  fi

  if ! cmake -S "$ENGINE_DIR" -B "$CPP_ENGINE_BUILD_DIR" \
    -DCMAKE_BUILD_TYPE="$build_type" \
    -DCMAKE_CXX_STANDARD="$cxx_standard" \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON; then
    echo "failed to configure C++ engine build at $CPP_ENGINE_BUILD_DIR" >&2
    exit 1
  fi
  if ! cmake "${build_args[@]}"; then
    echo "failed to build C++ engine_app from $ENGINE_DIR; install librdkafka++" >&2
    exit 1
  fi

  if [[ -z "$(find_cpp_engine_app)" ]]; then
    echo "C++ engine_app was not built at $CPP_ENGINE_BUILD_DIR; install librdkafka++" >&2
    exit 1
  fi
}

seed_cpp_engine_group_to_end() {
  echo "seeding C++ engine group $CPP_ENGINE_GROUP_ID at engine.input end"
  if ! docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda \
    rpk group seek "$CPP_ENGINE_GROUP_ID" --to end --topics engine.input \
      --allow-new-topics --brokers localhost:9092 >/dev/null; then
    echo "failed to seed C++ engine group $CPP_ENGINE_GROUP_ID to the current engine.input end" >&2
    exit 1
  fi
}

start_cpp_engine() {
  local engine_app
  local log_file="$LOG_DIR/cpp-engine.log"
  local pid

  engine_app="$(find_cpp_engine_app)"
  if [[ -z "$engine_app" ]]; then
    echo "C++ engine_app not found under $CPP_ENGINE_BUILD_DIR; build step did not produce it" >&2
    exit 1
  fi

  prepare_cpp_engine_markets_config
  mkdir -p "$CPP_ENGINE_CHECKPOINT_DIR"
  seed_cpp_engine_group_to_end

  (
    cd "$ENGINE_DIR"
    export CEX_ENGINE_BOOTSTRAP_SERVERS="$CPP_ENGINE_BROKERS"
    export CEX_ENGINE_GROUP_ID="$CPP_ENGINE_GROUP_ID"
    export CEX_ENGINE_CHECKPOINT_DIR="$CPP_ENGINE_CHECKPOINT_DIR"
    export CEX_ENGINE_MARKETS_CONFIG="$CPP_ENGINE_MARKETS_CONFIG"
    if [[ -n "$CPP_ENGINE_POLL_LIMIT" ]]; then
      export CEX_ENGINE_POLL_LIMIT="$CPP_ENGINE_POLL_LIMIT"
    else
      unset CEX_ENGINE_POLL_LIMIT
    fi
    "$engine_app"
  ) >"$log_file" 2>&1 &

  pid="$!"
  register_service_pid "cpp-engine" "$pid" "$log_file"
}

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
  wait_until database-migrations bash -c \
    "docker compose -p '$PROJECT_NAME' -f '$COMPOSE_FILE' exec -T postgres psql -U postgres -d exchange -tAc \"SELECT to_regclass('public.markets') IS NOT NULL\" | grep -q t"
}

seed_smoke_markets() {
  echo "seeding smoke markets before ingress inputs"
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T postgres \
    psql -U postgres -d exchange <<'EOF_SQL' >/dev/null
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

check_services_alive() {
  local label="$1"
  local failed=0
  local i
  local name
  local pid
  local status

  for i in "${!SERVICE_PIDS[@]}"; do
    name="${SERVICE_NAMES[$i]}"
    pid="${SERVICE_PIDS[$i]}"

    if ! kill -0 "$pid" >/dev/null 2>&1; then
      status=0
      wait "$pid" || status=$?
      echo "service $name exited during $label (pid $pid, status $status)" >&2
      failed=1
    fi
  done

  return "$failed"
}

scan_service_logs() {
  local pattern="${E2E_LOG_ERROR_PATTERN:-panic|OffsetOutOfRange|task failed|redpanda client failed|Error:}"
  local allowlist="${E2E_LOG_ERROR_ALLOWLIST:-}"
  local allowlist_file="${E2E_LOG_ERROR_ALLOWLIST_FILE:-}"
  local log_file
  local matches
  local failed=0

  for log_file in "$LOG_DIR"/*.log; do
    [[ -e "$log_file" ]] || continue

    matches="$(grep -En "$pattern" "$log_file" || true)"
    if [[ -n "$matches" && -n "$allowlist" ]]; then
      matches="$(printf '%s\n' "$matches" | grep -Ev "$allowlist" || true)"
    fi
    if [[ -n "$matches" && -n "$allowlist_file" && -f "$allowlist_file" ]]; then
      matches="$(printf '%s\n' "$matches" | grep -Evf "$allowlist_file" || true)"
    fi

    if [[ -n "$matches" ]]; then
      echo "serious error pattern found in $(basename "$log_file"):" >&2
      printf '%s\n' "$matches" >&2
      failed=1
    fi
  done

  return "$failed"
}

need_command docker
need_command cargo
need_command cmake

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log
if [[ "$CPP_ENGINE_CHECKPOINT_DIR_MANAGED" == "1" ]]; then
  rm -rf "$CPP_ENGINE_CHECKPOINT_DIR"
fi
if [[ "$CPP_ENGINE_MARKETS_CONFIG_MANAGED" == "1" ]]; then
  rm -f "$CPP_ENGINE_MARKETS_CONFIG"
fi

echo "starting e2e infra"
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" up -d
wait_until postgres docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T postgres pg_isready -U postgres -d exchange
wait_until redpanda docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda rpk cluster info --brokers localhost:9092

echo "creating redpanda topics"
for topic in wallet.commands wallet.replies wallet.events engine.input engine.replies engine.events; do
  if [[ "$topic" == "engine.input" ]]; then
    create_engine_input_topic
  else
    create_topic "$topic" 3
  fi
done

echo "building e2e binaries"
(
  cd "$ROOT_DIR"
  cargo build -p wallet -p projector -p timeseries -p ws -p ledger -p server -p engine-ingress -p e2e-smoke
)
echo "building C++ engine_app"
build_cpp_engine_app

echo "starting exchange services"
start_service wallet
start_service projector
start_service timeseries
start_service ledger
start_cpp_engine
start_service ws
start_service server

sleep "${E2E_SERVICE_STARTUP_GRACE_SECONDS:-5}"
if ! check_services_alive "startup"; then
  exit 1
fi
wait_for_migrations
seed_smoke_markets
publish_mark_ingress_input

echo "running e2e smoke driver"
driver_status=0
(
  cd "$ROOT_DIR"
  env \
    DATABASE_URL="$DATABASE_URL" \
    E2E_SERVER_URL="$API_URL" \
    E2E_WS_URL="$WS_URL" \
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
