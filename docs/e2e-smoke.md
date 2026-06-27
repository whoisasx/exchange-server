# E2E Smoke Harness

The smoke harness runs the Rust exchange services against the C++ matching engine.

Run it from the exchange checkout:

```sh
scripts/e2e-smoke.sh
```

By default the script expects the C++ engine checkout at `../engine`. Use
`E2E_CPP_ENGINE_DIR` when the engine lives elsewhere:

```sh
E2E_CPP_ENGINE_DIR=/path/to/engine scripts/e2e-smoke.sh
```

The GitHub workflow checks out the C++ engine from `whoisasx/exchange-engine` by
default. Set the repository variable `CPP_ENGINE_REPOSITORY` when CI should use
a different engine repository.

The script starts local Postgres and Redpanda with Docker Compose, starts
TimescaleDB and MinIO as direct Docker containers, creates stream topics, builds
`engine_app`, starts `wallet`, `projector`, `timeseries`, `ledger`,
`cpp-engine`, `ws`, and `server`, then drives the REST and websocket flow with
`tools/e2e-smoke`.

The smoke harness creates or validates the named direct containers
`perpex-timescaledb` from `timescale/timescaledb:latest-pg16` and
`perpex-minio` from `minio/minio:latest`. It waits for TimescaleDB on
`127.0.0.1:55433` and MinIO on `127.0.0.1:59000`, exposes the MinIO console on
`127.0.0.1:59001`, applies `scripts/timescale-init/001_timescaledb.sql`, checks
that the `timescaledb` extension is installed, creates the MinIO checkpoint
bucket with `minio/mc`, and clears that bucket before the run.

The harness passes
`TIMESERIES_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:55433/exchange_timeseries`
to `timeseries`, `server`, and `tools/e2e-smoke`. The smoke driver keeps core
exchange assertions on the main `DATABASE_URL`, but reads candle and
`timeseries_offsets` assertions from `TIMESERIES_DATABASE_URL`. The C++ engine
receives `S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`, `S3_ACCESS_KEY_ID`,
`S3_SECRET_ACCESS_KEY`, `S3_FORCE_PATH_STYLE`, AWS-compatible aliases, and the
engine checkpoint aliases `CEX_ENGINE_CHECKPOINT_S3_ENDPOINT`,
`CEX_ENGINE_CHECKPOINT_S3_BUCKET`, `CEX_ENGINE_CHECKPOINT_S3_ACCESS_KEY`,
`CEX_ENGINE_CHECKPOINT_S3_SECRET_KEY`, and
`CEX_ENGINE_CHECKPOINT_S3_REGION`. After the smoke driver succeeds, the harness
fails unless MinIO contains at least one `*.checkpoint` object.

Default storage values can be overridden with `E2E_TIMESCALE_PORT`,
`E2E_TIMESCALE_CONTAINER`, `E2E_TIMESCALE_VOLUME`, `TIMESCALE_IMAGE`,
`TIMESCALE_DB`, `TIMESCALE_USER`, `TIMESCALE_PASSWORD`, `E2E_MINIO_PORT`,
`E2E_MINIO_CONSOLE_PORT`, `E2E_MINIO_CONTAINER`, `E2E_MINIO_VOLUME`,
`MINIO_IMAGE`, `MINIO_MC_IMAGE`, `S3_ENDPOINT`, `S3_REGION`, `S3_BUCKET`,
`S3_ACCESS_KEY_ID`, and `S3_SECRET_ACCESS_KEY`.

The harness provisions `engine.input` as a single-partition topic with
`retention.ms=1800000`, writes a two-market C++ engine config for `SOL-PERP`
and `ETH-PERP`, and uses an isolated checkpoint/build directory under
`target/e2e-smoke`. It queues one mark-price input through `tools/engine-ingress` and
verifies the wallet outbox relay publishes that row. The smoke driver also waits
for `wallet_outbox` to drain and checks that ledger rows consumed from
`wallet.events` have unique logical event ids.
