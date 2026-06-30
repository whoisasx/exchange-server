# Exchange Local Development

This page holds the operational setup that is intentionally kept out of the
top-level README.

## Prerequisites

- Rust stable
- Docker with Compose
- `sqlx-cli` for migrations in the harness

Install SQLx CLI:

```sh
cargo install sqlx-cli --version 0.9.0 --no-default-features --features rustls,postgres
```

## Get the Repos

Use sibling checkouts so exchange and engine harness paths line up:

```sh
mkdir -p ~/perpex
cd ~/perpex
git clone git@github.com:whoisasx/exchange-server.git exchange
git clone git@github.com:whoisasx/exchange-engine.git engine
```

HTTPS works too:

```sh
git clone https://github.com/whoisasx/exchange-server.git exchange
git clone https://github.com/whoisasx/exchange-engine.git engine
```

## Storage Containers

All required local data/storage services are started by the exchange harness:

```sh
cd ~/perpex/exchange
test-harness/infra.sh up
```

```mermaid
flowchart LR
    infra[test-harness/infra.sh up] --> pg[(Postgres)]
    infra --> rp[(Redpanda)]
    infra --> ts[(TimescaleDB)]
    infra --> minio[(MinIO)]
    rp --> services[exchange services]
    pg --> services
    ts --> services
    minio --> engine[engine checkpoints]
```

| Container | Purpose | Local endpoint |
|---|---|---|
| Postgres | Main exchange DB: users, balances, orders, projector rows, ledger rows, wallet outbox | `postgres://postgres:postgres@127.0.0.1:55432/exchange` |
| Redpanda | `wallet.commands`, `wallet.events`, `engine.input`, `engine.replies`, `engine.events` | `127.0.0.1:19092` |
| TimescaleDB | Trade and candle time-series data | `postgres://postgres:postgres@127.0.0.1:55433/exchange_timeseries` |
| MinIO | S3-compatible engine checkpoint storage | `http://127.0.0.1:59000` |

`infra.sh up` creates Redpanda topics, applies TimescaleDB setup, creates the
MinIO bucket `exchange-checkpoints`, and clears old checkpoint objects.

Stop and remove local infra:

```sh
test-harness/infra.sh down
```

## Full E2E Test

From the exchange repo:

```sh
test-harness/infra.sh up
```

In another terminal, start the engine from the engine repo:

```sh
cd ../engine
test-harness/run-exchange-e2e-engine.sh
```

Then run the exchange smoke:

```sh
cd ../exchange
test-harness/smoke.sh
```

Expected success:

```text
e2e smoke passed
e2e smoke complete
```

Cleanup:

```sh
cd ../exchange
test-harness/infra.sh down
```

Stop the engine with `Ctrl-C`.

## Useful Commands

```sh
cargo fmt --all -- --check
cargo test --workspace
test-harness/infra.sh status
test-harness/infra.sh logs
```

## Benchmarks

Command-flow benchmark:

```sh
bench-harness/run-command-flow.sh
```

Smoke-sized benchmark:

```sh
EXCHANGE_BENCH_COMMANDS=100 EXCHANGE_BENCH_WARMUP=10 bench-harness/run-command-flow.sh
```

Results are written to `target/exchange-bench/<run id>/`.
