#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "$ROOT_DIR/scripts/e2e/common.sh"
source "$ROOT_DIR/scripts/e2e/redpanda.sh"

usage() {
  cat <<'USAGE'
Usage: scripts/e2e-infra.sh [up|down|status|logs]

Commands:
  up      Start and prepare Postgres, Redpanda, TimescaleDB, and MinIO.
  down    Stop compose infra and remove its volumes.
  status  Show compose service status.
  logs    Print compose infra logs.

Run `up`, ensure the independently managed engine is using this infra, then run
`scripts/e2e-smoke.sh`.
USAGE
}

command="${1:-up}"

case "$command" in
  up)
    need_command docker
    need_command curl

    echo "starting e2e infra"
    echo "Postgres: $DATABASE_URL"
    echo "Redpanda: 127.0.0.1:$REDPANDA_PORT"
    echo "TimescaleDB: $TIMESERIES_DATABASE_URL"
    echo "MinIO: $S3_ENDPOINT bucket $S3_BUCKET"

    compose up -d --remove-orphans
    wait_for_compose_infra
    setup_storage_infra
    create_e2e_topics

    cat <<EOF_READY
e2e infra ready

Run the exchange smoke when the independently managed engine is consuming
engine.input and publishing engine.replies plus engine.events:
  scripts/e2e-smoke.sh
EOF_READY
    ;;
  down)
    need_command docker
    compose down -v --remove-orphans
    ;;
  status)
    need_command docker
    compose ps
    ;;
  logs)
    need_command docker
    compose logs --no-color "${@:2}"
    ;;
  -h|--help)
    usage
    ;;
  *)
    echo "unknown command: $command" >&2
    usage >&2
    exit 2
    ;;
esac
