# shellcheck shell=bash

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
  if ! compose exec -T redpanda \
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
    export S3_ENDPOINT="$S3_ENDPOINT"
    export S3_REGION="$S3_REGION"
    export S3_BUCKET="$S3_BUCKET"
    export S3_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID"
    export S3_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY"
    export S3_FORCE_PATH_STYLE="$S3_FORCE_PATH_STYLE"
    export AWS_ACCESS_KEY_ID="$S3_ACCESS_KEY_ID"
    export AWS_SECRET_ACCESS_KEY="$S3_SECRET_ACCESS_KEY"
    export AWS_REGION="$S3_REGION"
    export AWS_ENDPOINT_URL="$S3_ENDPOINT"
    export AWS_EC2_METADATA_DISABLED=true
    export CEX_ENGINE_CHECKPOINT_STORE="${CEX_ENGINE_CHECKPOINT_STORE:-s3}"
    export CEX_ENGINE_CHECKPOINT_S3_ENDPOINT="$S3_ENDPOINT"
    export CEX_ENGINE_CHECKPOINT_S3_REGION="$S3_REGION"
    export CEX_ENGINE_CHECKPOINT_S3_BUCKET="$S3_BUCKET"
    export CEX_ENGINE_CHECKPOINT_S3_ACCESS_KEY="$S3_ACCESS_KEY_ID"
    export CEX_ENGINE_CHECKPOINT_S3_SECRET_KEY="$S3_SECRET_ACCESS_KEY"
    export CEX_ENGINE_CHECKPOINT_S3_FORCE_PATH_STYLE="$S3_FORCE_PATH_STYLE"
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
