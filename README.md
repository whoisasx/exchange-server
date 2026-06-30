# Perpex Exchange

Rust services for the exchange side of Perpex: HTTP API, wallet reservation and
accounting, stream consumers, read models, timeseries writes, and websocket
fanout.

The matching engine is a separate repo and process. Exchange publishes validated
inputs to `engine.input` and consumes `engine.replies` plus `engine.events`; it
does not start, stop, or probe the engine.

## Architecture

```mermaid
flowchart LR
    client[Client] --> server[apps/server<br/>REST API]
    client --> ws[apps/ws<br/>WebSocket]

    server --> walletCommands[(wallet.commands)]
    walletCommands --> wallet[apps/wallet]
    wallet --> pg[(Postgres<br/>exchange state)]

    wallet --> walletOutbox[(wallet_outbox)]
    walletOutbox --> walletEvents[(wallet.events)]
    walletOutbox --> engineInput[(engine.input)]

    engineInput --> engine[exchange-engine<br/>external process]
    engine --> engineReplies[(engine.replies)]
    engine --> engineEvents[(engine.events)]

    engineReplies --> server
    engineReplies --> wallet
    engineEvents --> wallet
    engineEvents --> projector[apps/projector]
    engineEvents --> timeseries[apps/timeseries]
    engineEvents --> ws
    walletEvents --> ledger[apps/ledger]
    walletEvents --> ws

    projector --> pg
    ledger --> pg
    server --> pg
    timeseries --> ts[(TimescaleDB)]
    server --> ts

    engine -. checkpoints .-> s3[(MinIO / S3)]
```

## Request Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant S as Server
    participant W as Wallet
    participant O as Wallet Outbox
    participant E as Engine
    participant P as Projector
    participant L as Ledger
    participant TS as Timeseries
    participant WS as WebSocket

    C->>S: Place/cancel/position request
    S->>W: wallet.commands
    W->>W: Validate balances and locks
    W->>O: Persist wallet event and/or engine input
    O->>E: engine.input
    O->>L: wallet.events
    E->>S: engine.replies
    E->>W: engine.events
    E->>P: engine.events
    E->>TS: engine.events
    E->>WS: engine.events
    L->>S: Ledger rows in Postgres
    P->>S: Orders, fills, positions, orderbook in Postgres
    TS->>S: Candles in TimescaleDB
    WS->>C: Live account and market updates
    S->>C: Request result / REST reads
```

## Repo Layout

```text
apps/server       HTTP API and request/reply coordination
apps/wallet       Balance checks, locks, wallet events, engine input outbox
apps/projector    Engine event projections into Postgres read models
apps/ledger       Accounting journal from wallet.events
apps/timeseries   Trades and candles in TimescaleDB
apps/ws           Live websocket fanout from wallet.events and engine.events
crates/config     Shared env/config helpers
crates/db         Database access and migrations
crates/protocol   Rust stream protocol types
tools/e2e-smoke   End-to-end smoke driver
tools/exchange-bench-driver
                  Exchange command-flow benchmark load driver
tools/exchange-bench-engine-peer
                  Benchmark-only engine peer for exchange latency tests
tools/engine-ingress
                  Manual mark/funding input publisher
test-harness      Manual infra and e2e test scripts
bench-harness     Exchange benchmark scripts
docs              Protocol notes and service-specific details
```

## Getting Started

Prerequisites:

- Rust stable
- Docker with Compose
- `sqlx-cli` for migrations in the harness

Install SQLx CLI:

```sh
cargo install sqlx-cli --version 0.9.0 --no-default-features --features rustls,postgres
```

Use sibling repo checkouts:

```sh
mkdir -p ~/perpex
cd ~/perpex
git clone git@github.com:whoisasx/exchange-server.git exchange
git clone git@github.com:whoisasx/exchange-engine.git engine
```

## Storage Containers

All required local data/storage services are started by the exchange harness:

```sh
cd ~/perpex/exchange
test-harness/infra.sh up
```

This starts and prepares:

| Container | Purpose | Local endpoint |
|---|---|---|
| Postgres | Main exchange DB: users, balances, orders, projector rows, ledger rows, wallet outbox | `postgres://postgres:postgres@127.0.0.1:55432/exchange` |
| Redpanda | Streams and queues: `wallet.commands`, `wallet.events`, `engine.input`, `engine.replies`, `engine.events` | `127.0.0.1:19092` |
| TimescaleDB | Time-series DB for trades and candles | `postgres://postgres:postgres@127.0.0.1:55433/exchange_timeseries` |
| MinIO | S3-compatible object storage for engine checkpoints | `http://127.0.0.1:59000` |

`infra.sh up` also creates Redpanda topics, creates the Timescale extension,
creates the MinIO bucket `exchange-checkpoints`, and clears old checkpoint
objects. Stop and remove local infra with:

```sh
test-harness/infra.sh down
```

## Run The Full E2E Test

From this repo:

```sh
test-harness/infra.sh up
```

In another terminal, start the engine:

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

## Run Benchmarks

Exchange benchmarks measure the API-to-wallet-to-stream command flow. The
benchmark uses JSON protocol messages and starts a small benchmark-only engine
peer so this repo can measure its own latency without depending on a live engine
process.

```mermaid
flowchart LR
    driver[exchange-bench-driver] --> server[server]
    server --> commands[(wallet.commands)]
    commands --> wallet[wallet]
    wallet --> outbox[(wallet_outbox)]
    outbox --> input[(engine.input)]
    input --> peer[benchmark engine peer]
    peer --> replies[(engine.replies)]
    peer --> events[(engine.events)]
    replies --> server
```

Run the command-flow benchmark:

```sh
bench-harness/run-command-flow.sh
```

Smoke-sized run:

```sh
EXCHANGE_BENCH_COMMANDS=100 EXCHANGE_BENCH_WARMUP=10 bench-harness/run-command-flow.sh
```

Results are written to `target/exchange-bench/<run id>/`.

Stop benchmark infra when you are done:

```sh
test-harness/infra.sh down
```

## Configuration

Start from `.env.example` for local service runs. The main connection points are:

- `DATABASE_URL`: Postgres exchange state
- `TIMESERIES_DATABASE_URL`: TimescaleDB candles/trades
- `REDPANDA_BROKERS`: Redpanda brokers
- `S3_*`: MinIO/S3 checkpoint settings used by tests and engine-adjacent flows
- `JWT_SECRET`, `SERVER_*`, `WS_*`: API and websocket settings

## More Detail

- [Test harness](test-harness/README.md)
- [Benchmark harness](bench-harness/README.md)
- [Engine stream contract](docs/engine-contract.md)
- [Wallet events](docs/wallet-events.md)
- [Timeseries](docs/timeseries.md)
- [WebSocket](docs/websocket.md)
- [Ledger](docs/ledger.md)
- [Orderbook](docs/orderbook.md)
