#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
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
LOG_DIR="$ROOT_DIR/target/e2e-smoke"
PIDS=()

cleanup() {
  if ((${#PIDS[@]} > 0)); then
    kill "${PIDS[@]}" >/dev/null 2>&1 || true
    wait "${PIDS[@]}" >/dev/null 2>&1 || true
  fi

  if [[ "${E2E_KEEP_INFRA:-0}" != "1" ]]; then
    docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
  fi
}

trap cleanup EXIT

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
  docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda \
    rpk topic create "$topic" --partitions 3 --brokers localhost:9092 >/dev/null 2>&1 || true
}

start_service() {
  local binary="$1"
  local log_file="$LOG_DIR/${binary}.log"

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

  PIDS+=("$!")
}

need_command docker
need_command cargo

mkdir -p "$LOG_DIR"
rm -f "$LOG_DIR"/*.log

echo "starting e2e infra"
docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" up -d
wait_until postgres docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T postgres pg_isready -U postgres -d exchange
wait_until redpanda docker compose -p "$PROJECT_NAME" -f "$COMPOSE_FILE" exec -T redpanda rpk cluster info --brokers localhost:9092

echo "creating redpanda topics"
for topic in wallet.commands wallet.replies wallet.events engine.commands engine.replies engine.events; do
  create_topic "$topic"
done

echo "building e2e binaries"
(
  cd "$ROOT_DIR"
  cargo build -p wallet -p projector -p timeseries -p fake-engine -p ws -p ledger -p server -p e2e-smoke
)

echo "starting exchange services"
start_service wallet
start_service projector
start_service timeseries
start_service ledger
start_service fake-engine
start_service ws
start_service server

sleep "${E2E_SERVICE_STARTUP_GRACE_SECONDS:-5}"

echo "running e2e smoke driver"
(
  cd "$ROOT_DIR"
  env \
    DATABASE_URL="$DATABASE_URL" \
    E2E_SERVER_URL="$API_URL" \
    E2E_WS_URL="$WS_URL" \
    cargo run -p e2e-smoke
)

echo "e2e smoke complete"
