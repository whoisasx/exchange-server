# shellcheck shell=bash

if [[ -z "${ROOT_DIR:-}" ]]; then
  ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
fi

COMPOSE_FILE="${COMPOSE_FILE:-$ROOT_DIR/test-harness/e2e-compose.yml}"
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
MINIO_IMAGE="${MINIO_IMAGE:-minio/minio:latest}"
MINIO_MC_IMAGE="${MINIO_MC_IMAGE:-minio/mc:latest}"
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
export TIMESCALE_DB TIMESCALE_USER TIMESCALE_PASSWORD TIMESCALE_IMAGE
export MINIO_ROOT_USER MINIO_ROOT_PASSWORD MINIO_BUCKET MINIO_IMAGE

compose() {
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" "$@"
}

compose_default_network() {
  docker network ls \
    --filter "label=com.docker.compose.project=$PROJECT_NAME" \
    --filter "label=com.docker.compose.network=default" \
    --format '{{.Name}}' | head -n 1
}

need_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

wait_until() {
  local label="$1"
  shift
  local attempts="${E2E_WAIT_ATTEMPTS:-60}"

  for _ in $(seq 1 "$attempts"); do
    if "$@" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "timed out waiting for $label" >&2
  compose ps >&2 || true
  exit 1
}

timescale_psql() {
  compose exec -T timescaledb psql -U "$TIMESCALE_USER" -d "$TIMESCALE_DB" "$@"
}

timescale_extension_ready() {
  timescale_psql -tAc \
    "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='timescaledb')" | grep -q t
}

timescale_sql_ready() {
  timescale_psql -tAc "SELECT 1" | grep -q 1
}

apply_timescale_init() {
  timescale_psql -v ON_ERROR_STOP=1 -c "CREATE EXTENSION IF NOT EXISTS timescaledb;"
}

timeseries_database_migrations_ready() {
  timescale_psql -tAc \
    "SELECT to_regclass('public.candles') IS NOT NULL AND to_regclass('public.timeseries_offsets') IS NOT NULL" | grep -q t
}

timeseries_markets_table_ready() {
  timescale_psql -tAc \
    "SELECT to_regclass('public.markets') IS NOT NULL" | grep -q t
}

wait_for_compose_infra() {
  echo "waiting for compose infra"
  wait_until postgres compose exec -T postgres pg_isready -U postgres -d exchange
  wait_until redpanda compose exec -T redpanda rpk cluster info --brokers localhost:9092
  wait_until timescaledb compose exec -T timescaledb pg_isready -U "$TIMESCALE_USER" -d "$TIMESCALE_DB"
  wait_until timescaledb-sql timescale_sql_ready
  wait_until minio curl -fsS "$S3_ENDPOINT/minio/health/ready"
}

wait_for_storage_setup() {
  wait_until timescale-extension timescale_extension_ready
  wait_until minio-bucket minio_bucket_exists
}

setup_storage_infra() {
  echo "applying Timescale init"
  wait_until timescale-extension-create apply_timescale_init
  wait_until timescale-extension timescale_extension_ready
  wait_until minio-bucket-create create_minio_bucket
  wait_until minio-bucket minio_bucket_exists
  clear_minio_checkpoint_objects
}

wait_for_timeseries_migrations() {
  wait_until timeseries-database-migrations timeseries_database_migrations_ready
}

minio_mc() {
  local network

  network="$(compose_default_network)"
  if [[ -z "$network" ]]; then
    echo "could not find compose network for project $PROJECT_NAME" >&2
    exit 1
  fi

  docker run --rm \
    --network "$network" \
    -e "MINIO_ROOT_USER=$MINIO_ROOT_USER" \
    -e "MINIO_ROOT_PASSWORD=$MINIO_ROOT_PASSWORD" \
    --entrypoint /bin/sh \
    "$MINIO_MC_IMAGE" \
    -c 'mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && exec "$@"' \
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
