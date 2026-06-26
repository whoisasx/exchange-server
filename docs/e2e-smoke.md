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

The script starts local Postgres and Redpanda with Docker Compose, creates stream
topics, builds `engine_app`, starts `wallet`, `projector`, `timeseries`,
`ledger`, `cpp-engine`, `ws`, and `server`, then drives the REST and websocket
flow with `tools/e2e-smoke`.

The harness provisions `engine.input` as a single-partition topic with
`retention.ms=1800000`, writes a two-market C++ engine config for `SOL-PERP`
and `ETH-PERP`, and uses an isolated checkpoint/build directory under
`target/e2e-smoke`. It queues one mark-price input through `tools/engine-ingress` and
verifies the wallet outbox relay publishes that row. The smoke driver also waits
for `wallet_outbox` to drain and checks that ledger rows consumed from
`wallet.events` have unique logical event ids.
