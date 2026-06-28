# shellcheck shell=bash

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
  local service_database_url="$DATABASE_URL"
  local pid

  if [[ "$binary" == "timeseries" ]]; then
    service_database_url="$TIMESERIES_DATABASE_URL"
  fi

  (
    cd "$ROOT_DIR"
    env \
      DATABASE_URL="$service_database_url" \
      TIMESERIES_DATABASE_URL="$TIMESERIES_DATABASE_URL" \
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
