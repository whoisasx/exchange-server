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
  local partitions="${ENGINE_INPUT_PARTITIONS:-8}"
  local actual
  local missing

  create_topic engine.input "$partitions" -c retention.ms=1800000
  set_topic_config engine.input retention.ms=1800000
  actual="$(topic_partitions engine.input || true)"
  if [[ -n "$actual" && "$actual" =~ ^[0-9]+$ && "$actual" -lt "$partitions" ]]; then
    missing=$((partitions - actual))
    compose exec -T redpanda \
      rpk topic add-partitions engine.input --num "$missing" --brokers localhost:9092 >/dev/null
  fi
  assert_topic_partitions engine.input "$partitions"
}

create_e2e_topics() {
  local topic

  echo "creating redpanda topics"
  for topic in wallet.commands wallet.replies wallet.events engine.input engine.replies engine.events; do
    if [[ "$topic" == "engine.input" ]]; then
      create_engine_input_topic
    else
      create_topic "$topic" 3
    fi
  done
}

assert_e2e_topics_ready() {
  assert_topic_partitions wallet.commands 3
  assert_topic_partitions wallet.replies 3
  assert_topic_partitions wallet.events 3
  assert_topic_partitions engine.input "${ENGINE_INPUT_PARTITIONS:-8}"
  assert_topic_partitions engine.replies 3
  assert_topic_partitions engine.events 3
}
