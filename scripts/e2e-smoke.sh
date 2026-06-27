#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MONOREPO_ROOT="$(cd "$ROOT_DIR/.." && pwd)"
ENGINE_DIR="${E2E_CPP_ENGINE_DIR:-$MONOREPO_ROOT/engine}"
COMPOSE_FILE="$ROOT_DIR/scripts/e2e-compose.yml"
PROJECT_NAME="${E2E_COMPOSE_PROJECT:-exchange-e2e}"
POSTGRES_PORT="${E2E_POSTGRES_PORT:-55432}"
REDPANDA_PORT="${E2E_REDPANDA_PORT:-19092}"
TIMESCALE_PORT="${E2E_TIMESCALE_PORT:-55433}"
MINIO_PORT="${E2E_MINIO_PORT:-59000}"
MINIO_CONSOLE_PORT="${E2E_MINIO_CONSOLE_PORT:-59001}"
SERVER_PORT="${E2E_SERVER_PORT:-18080}"
WS_PORT="${E2E_WS_PORT:-18081}"
DATABASE_URL="postgres://postgres:postgres@127.0.0.1:${POSTGRES_PORT}/exchange"
TIMESCALE_DB="${TIMESCALE_DB:-exchange_timeseries}"
TIMESCALE_USER="${TIMESCALE_USER:-postgres}"
TIMESCALE_PASSWORD="${TIMESCALE_PASSWORD:-postgres}"
TIMESCALE_IMAGE="${TIMESCALE_IMAGE:-timescale/timescaledb:latest-pg16}"
TIMESCALE_CONTAINER="${E2E_TIMESCALE_CONTAINER:-perpex-timescaledb}"
TIMESCALE_VOLUME="${E2E_TIMESCALE_VOLUME:-perpex-timescaledb-data}"
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:latest}"
MINIO_CONTAINER="${E2E_MINIO_CONTAINER:-perpex-minio}"
MINIO_VOLUME="${E2E_MINIO_VOLUME:-perpex-minio-data}"
SERVER_URL="http://127.0.0.1:${SERVER_PORT}"
API_URL="${SERVER_URL}/api"
WS_URL="ws://127.0.0.1:${WS_PORT}/ws"
E2E_MARK_INPUT_ID="${E2E_MARK_INPUT_ID:-${PROJECT_NAME}-mark-price-smoke}"
LOG_DIR="$ROOT_DIR/target/e2e-smoke"
TIMESERIES_DATABASE_URL="${TIMESERIES_DATABASE_URL:-postgres://${TIMESCALE_USER}:${TIMESCALE_PASSWORD}@127.0.0.1:${TIMESCALE_PORT}/${TIMESCALE_DB}}"
S3_ENDPOINT="${S3_ENDPOINT:-http://127.0.0.1:${MINIO_PORT}}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_BUCKET="${S3_BUCKET:-${MINIO_BUCKET:-exchange-checkpoints}}"
S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-${MINIO_ROOT_USER:-minioadmin}}"
S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD:-minioadmin}}"
S3_FORCE_PATH_STYLE="${S3_FORCE_PATH_STYLE:-true}"
MINIO_ROOT_USER="$S3_ACCESS_KEY_ID"
MINIO_ROOT_PASSWORD="$S3_SECRET_ACCESS_KEY"
MINIO_BUCKET="$S3_BUCKET"
export E2E_TIMESCALE_PORT="$TIMESCALE_PORT"
export E2E_MINIO_PORT="$MINIO_PORT"
export E2E_MINIO_CONSOLE_PORT="$MINIO_CONSOLE_PORT"
export TIMESCALE_DB TIMESCALE_USER TIMESCALE_PASSWORD
export MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_BUCKET
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

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

container_exists() {
  docker container inspect "$1" >/dev/null 2>&1
}

container_running() {
  [[ "$(docker container inspect -f '{{.State.Running}}' "$1" 2>/dev/null || true)" == "true" ]]
}

container_image_matches() {
  local container="$1"
  local expected_image="$2"
  local config_image
  local container_image_id
  local expected_image_id

  config_image="$(docker container inspect -f '{{.Config.Image}}' "$container")"
  if [[ "$config_image" == "$expected_image" ]]; then
    return 0
  fi

  container_image_id="$(docker container inspect -f '{{.Image}}' "$container")"
  expected_image_id="$(docker image inspect -f '{{.Id}}' "$expected_image" 2>/dev/null || true)"
  [[ -n "$expected_image_id" && "$container_image_id" == "$expected_image_id" ]]
}

require_container_image() {
  local container="$1"
  local expected_image="$2"
  local actual_image

  if container_image_matches "$container" "$expected_image"; then
    return
  fi

  actual_image="$(docker container inspect -f '{{.Config.Image}}' "$container")"
  echo "$container exists but uses image $actual_image; expected $expected_image" >&2
  echo "remove the container or set the matching image env before rerunning" >&2
  exit 1
}

require_container_port() {
  local container="$1"
  local container_port="$2"
  local host_port="$3"
  local published

  published="$(docker port "$container" "${container_port}/tcp" 2>/dev/null || true)"
  if printf '%s\n' "$published" | awk -F: '{ print $NF }' | grep -Fxq "$host_port"; then
    return
  fi

  echo "$container exists but does not publish ${container_port}/tcp on host port $host_port" >&2
  echo "current port mapping: ${published:-none}" >&2
  exit 1
}

require_container_env() {
  local container="$1"
  local key="$2"
  local expected="$3"

  if docker container inspect -f '{{range .Config.Env}}{{println .}}{{end}}' "$container" |
    grep -Fxq "${key}=${expected}"; then
    return
  fi

  echo "$container exists but does not have the expected $key value" >&2
  echo "remove the container or set the matching env before rerunning" >&2
  exit 1
}

remove_direct_storage_infra() {
  docker rm -f "$TIMESCALE_CONTAINER" "$MINIO_CONTAINER" >/dev/null 2>&1 || true
  docker volume rm "$TIMESCALE_VOLUME" "$MINIO_VOLUME" >/dev/null 2>&1 || true
}

create_timescale_container() {
  docker run -d \
    --name "$TIMESCALE_CONTAINER" \
    --label perpex.e2e.role=timescaledb \
    --label "perpex.e2e.project=$PROJECT_NAME" \
    -e "POSTGRES_DB=$TIMESCALE_DB" \
    -e "POSTGRES_USER=$TIMESCALE_USER" \
    -e "POSTGRES_PASSWORD=$TIMESCALE_PASSWORD" \
    -p "${TIMESCALE_PORT}:5432" \
    -v "${TIMESCALE_VOLUME}:/var/lib/postgresql/data" \
    "$TIMESCALE_IMAGE" >/dev/null
}

validate_timescale_container() {
  require_container_image "$TIMESCALE_CONTAINER" "$TIMESCALE_IMAGE"
  require_container_port "$TIMESCALE_CONTAINER" 5432 "$TIMESCALE_PORT"
  require_container_env "$TIMESCALE_CONTAINER" POSTGRES_DB "$TIMESCALE_DB"
  require_container_env "$TIMESCALE_CONTAINER" POSTGRES_USER "$TIMESCALE_USER"
  require_container_env "$TIMESCALE_CONTAINER" POSTGRES_PASSWORD "$TIMESCALE_PASSWORD"
}

ensure_timescale_container() {
  if container_exists "$TIMESCALE_CONTAINER"; then
    validate_timescale_container
    if ! container_running "$TIMESCALE_CONTAINER"; then
      docker start "$TIMESCALE_CONTAINER" >/dev/null
    fi
    return
  fi

  create_timescale_container
}

create_minio_container() {
  docker run -d \
    --name "$MINIO_CONTAINER" \
    --label perpex.e2e.role=minio \
    --label "perpex.e2e.project=$PROJECT_NAME" \
    -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
    -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
    -p "${MINIO_PORT}:9000" \
    -p "${MINIO_CONSOLE_PORT}:9001" \
    -v "${MINIO_VOLUME}:/data" \
    "$MINIO_IMAGE" server /data --console-address ":9001" >/dev/null
}

validate_minio_container() {
  require_container_image "$MINIO_CONTAINER" "$MINIO_IMAGE"
  require_container_port "$MINIO_CONTAINER" 9000 "$MINIO_PORT"
  require_container_port "$MINIO_CONTAINER" 9001 "$MINIO_CONSOLE_PORT"
  require_container_env "$MINIO_CONTAINER" MINIO_ROOT_USER "$MINIO_ROOT_USER"
  require_container_env "$MINIO_CONTAINER" MINIO_ROOT_PASSWORD "$MINIO_ROOT_PASSWORD"
}

ensure_minio_container() {
  if container_exists "$MINIO_CONTAINER"; then
    validate_minio_container
    if ! container_running "$MINIO_CONTAINER"; then
      docker start "$MINIO_CONTAINER" >/dev/null
    fi
    return
  fi

  create_minio_container
}

start_direct_storage_infra() {
  echo "starting direct storage containers: $TIMESCALE_CONTAINER, $MINIO_CONTAINER"
  if [[ "${E2E_KEEP_INFRA:-0}" != "1" ]]; then
    remove_direct_storage_infra
  fi

  ensure_timescale_container
  ensure_minio_container
}

print_direct_storage_context() {
  local lines="${1:-120}"
  local container

  echo >&2
  echo "direct storage containers:" >&2
  for container in "$TIMESCALE_CONTAINER" "$MINIO_CONTAINER"; do
    if container_exists "$container"; then
      docker container inspect \
        -f '{{.Name}} image={{.Config.Image}} state={{.State.Status}}' \
        "$container" >&2 || true
      docker port "$container" >&2 || true
    else
      echo "$container not found" >&2
    fi
  done

  echo >&2
  echo "direct storage log tails (last ${lines} lines):" >&2
  for container in "$TIMESCALE_CONTAINER" "$MINIO_CONTAINER"; do
    if container_exists "$container"; then
      echo "----- $container -----" >&2
      docker logs --tail "$lines" "$container" >&2 || true
    fi
  done
}

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
    print_direct_storage_context "$lines"
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

  if [[ "${E2E_KEEP_INFRA:-0}" != "1" ]]; then
    compose down -v --remove-orphans >/dev/null 2>&1 || true
    remove_direct_storage_infra
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
  compose ps >&2 || true
  print_direct_storage_context 80
  exit 1
}

source "$ROOT_DIR/scripts/e2e/redpanda.sh"
source "$ROOT_DIR/scripts/e2e/exchange-services.sh"
source "$ROOT_DIR/scripts/e2e/cpp-engine.sh"

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

timescale_psql() {
  docker exec -i "$TIMESCALE_CONTAINER" psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" "$@"
}

timescale_extension_ready() {
  timescale_psql -tAc \
    "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')" | grep -q t
}

timescale_sql_ready() {
  timescale_psql -tAc "SELECT 1" | grep -q 1
}

apply_timescale_init() {
  timescale_psql -v ON_ERROR_STOP=1 -f - <"$ROOT_DIR/scripts/timescale-init/001_timescaledb.sql"
}

timeseries_database_migrations_ready() {
  timescale_psql -tAc \
    "SELECT to_regclass('public.candles') IS NOT NULL AND to_regclass('public.timeseries_offsets') IS NOT NULL" | grep -q t
}

timeseries_markets_table_ready() {
  timescale_psql -tAc \
    "SELECT to_regclass('public.markets') IS NOT NULL" | grep -q t
}

wait_for_storage_infra() {
  echo "waiting for storage infra"
  wait_until timescaledb docker exec "$TIMESCALE_CONTAINER" pg_isready -U "$TIMESCALE_USER" -d "$TIMESCALE_DB"
  wait_until timescaledb-sql timescale_sql_ready
  wait_until minio curl -fsS "$S3_ENDPOINT/minio/health/ready"

  echo "applying Timescale init"
  wait_until timescale-init apply_timescale_init
  wait_until timescale-init timescale_extension_ready
  wait_until minio-bucket-create create_minio_bucket
  wait_until minio-bucket minio_bucket_exists
  clear_minio_checkpoint_objects
}

wait_for_timeseries_migrations() {
  wait_until timeseries-database-migrations timeseries_database_migrations_ready
}

minio_mc() {
  docker run --rm \
    --network "container:$MINIO_CONTAINER" \
    -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
    -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
    --entrypoint /bin/sh \
    "$MINIO_MC_IMAGE" \
    -c 'mc alias set local http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && exec "$@"' \
    sh "$@"
}

create_minio_bucket() {
  minio_mc mc mb --ignore-existing "local/$MINIO_BUCKET"
}

minio_bucket_exists() {
  minio_mc mc ls "local/$MINIO_BUCKET"
}

clear_minio_checkpoint_objects() {
  echo "clearing MinIO checkpoint bucket"
  minio_mc mc rm --recursive --force "local/$MINIO_BUCKET" >/dev/null
}

list_minio_checkpoint_objects() {
  minio_mc mc find "local/$MINIO_BUCKET" --name "*.checkpoint"
}

list_minio_objects() {
  minio_mc mc ls --recursive "local/$MINIO_BUCKET"
}

assert_minio_checkpoint_objects() {
  local objects
  local attempt

  echo "checking MinIO checkpoint objects"
  for attempt in $(seq 1 60); do
    objects="$(list_minio_checkpoint_objects || true)"
    if [[ -n "$objects" ]]; then
      printf 'MinIO checkpoint objects:\n%s\n' "$objects"
      return
    fi
    sleep 1
  done

  echo "expected MinIO checkpoint objects in bucket $S3_BUCKET, found none" >&2
  list_minio_objects >&2 || true
  return 1
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

  if [[ "$service" == "timescaledb" ]]; then
    seed_smoke_markets_sql | docker exec -i "$TIMESCALE_CONTAINER" \
      psql -U "$user" -d "$database" >/dev/null
    return
  fi

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

need_command docker
need_command cargo
need_command cmake
need_command curl

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log
if [[ "$CPP_ENGINE_CHECKPOINT_DIR_MANAGED" == "1" ]]; then
  rm -rf "$CPP_ENGINE_CHECKPOINT_DIR"
fi
if [[ "$CPP_ENGINE_MARKETS_CONFIG_MANAGED" == "1" ]]; then
  rm -f "$CPP_ENGINE_MARKETS_CONFIG"
fi

echo "starting e2e infra"
echo "storage infra: TimescaleDB $TIMESERIES_DATABASE_URL, MinIO $S3_ENDPOINT bucket $S3_BUCKET"
compose up -d --remove-orphans
start_direct_storage_infra
wait_until postgres compose exec -T postgres pg_isready -U postgres -d exchange
wait_until redpanda compose exec -T redpanda rpk cluster info --brokers localhost:9092
wait_for_storage_infra

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

storage_status=0
if ((driver_status == 0)); then
  assert_minio_checkpoint_objects || storage_status=$?
fi

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

if ((storage_status != 0)); then
  echo "storage verification failed" >&2
  exit 1
fi

if ((log_status != 0)); then
  echo "serious error patterns were found in service logs" >&2
  exit 1
fi

echo "e2e smoke complete"
