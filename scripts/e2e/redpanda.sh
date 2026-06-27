# shellcheck shell=bash

create_topic() {
  local topic="$1"
  local partitions="$2"
  local attempt
  shift 2

  for attempt in $(seq 1 10); do
    if compose exec -T redpanda \
      rpk topic create --if-not-exists --partitions "$partitions" --brokers localhost:9092 "$@" "$topic" >/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "failed to create topic $topic after retries" >&2
  compose exec -T redpanda \
    rpk topic create --if-not-exists --partitions "$partitions" --brokers localhost:9092 "$@" "$topic"
}

set_topic_config() {
  local topic="$1"
  local config="$2"
  local attempt

  for attempt in $(seq 1 10); do
    if compose exec -T redpanda \
      rpk topic alter-config "$topic" --set "$config" --brokers localhost:9092 >/dev/null; then
      return 0
    fi
    sleep 1
  done

  echo "failed to set topic config $topic $config after retries" >&2
  compose exec -T redpanda \
    rpk topic alter-config "$topic" --set "$config" --brokers localhost:9092
}

topic_partitions() {
  local topic="$1"

  compose exec -T redpanda \
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
