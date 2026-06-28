# Test Harness

This is the canonical local test path for the exchange. The matching engine is
an external stream participant: exchange does not start it, stop it, or probe
its process health.

Run it from the exchange checkout in this order:

```sh
scripts/e2e-infra.sh up
# ensure the independently managed engine is using this infra
scripts/e2e-smoke.sh
scripts/e2e-infra.sh down
```

`scripts/e2e-infra.sh up` starts the four local infra containers, applies
TimescaleDB setup, creates/clears the MinIO checkpoint bucket, and creates the
Redpanda topics. `scripts/e2e-smoke.sh` checks the prepared exchange infra, runs
the Rust exchange test suite, starts the exchange services, and drives the REST
and websocket flow with `tools/e2e-smoke`. If no engine consumes `engine.input`
and publishes `engine.replies` plus `engine.events`, the flow times out through
normal request/event assertions.

Infra:

- Postgres, Redpanda, TimescaleDB, and MinIO are started from
  `scripts/e2e-compose.yml`.
- TimescaleDB uses `timescale/timescaledb:latest-pg16`.
- MinIO uses `minio/minio:latest`.

Test coverage:

- `cargo test --workspace`
- full E2E flow through server, wallet, the external engine, stream consumers,
  TimescaleDB, and websocket delivery

The infra harness waits for TimescaleDB on `127.0.0.1:55433` and MinIO on
`127.0.0.1:59000`, exposes the MinIO console on `127.0.0.1:59001`, applies
`scripts/timescale-init/001_timescaledb.sql`, checks that the `timescaledb`
extension is installed, creates the MinIO checkpoint bucket with an ephemeral
`minio/mc` helper container, and clears that bucket before the run.

The harness passes
`TIMESERIES_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55433/exchange_timeseries`
to `timeseries`, `server`, and `tools/e2e-smoke`. The smoke driver keeps core
exchange assertions on the main `DATABASE_URL`, but reads candle and
`timeseries_offsets` assertions from `TIMESERIES_DATABASE_URL`.

The infra harness provisions `engine.input` as a single-partition topic with
`retention.ms=1800000` and keeps exchange service logs under `target/e2e-smoke`.
The smoke queues one mark-price input through `tools/engine-ingress` and
verifies the wallet outbox relay publishes that row. The smoke driver also waits
for `wallet_outbox` to drain and checks that ledger rows consumed from
`wallet.events` have unique logical event ids.
