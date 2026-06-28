# Exchange Test Harness

This folder is the only manual test surface for the exchange repo. The exchange
harness owns shared local infra and exchange services only; it does not start,
stop, or probe the engine process.

## Get the Repos

Use sibling checkouts so the exchange and engine harness docs line up:

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

## Full E2E Test

From the exchange repo:

```sh
test-harness/infra.sh up
```

This starts Postgres, Redpanda, TimescaleDB, and MinIO, applies TimescaleDB
setup, clears the MinIO checkpoint bucket, and creates the stream topics.

In another terminal, start the engine from the engine repo:

```sh
cd ../engine
test-harness/run-exchange-e2e-engine.sh
```

Then run the exchange smoke from the exchange repo:

```sh
cd ../exchange
test-harness/smoke.sh
```

Expected success:

```txt
e2e smoke passed
e2e smoke complete
```

Stop the engine with `Ctrl-C`, then stop infra:

```sh
cd ../exchange
test-harness/infra.sh down
```

## Scripts

- `infra.sh up|down|status|logs`: manages local Postgres, Redpanda,
  TimescaleDB, MinIO, storage setup, and topics.
- `smoke.sh`: runs Rust tests, starts exchange services, and drives the REST,
  wallet, stream, TimescaleDB, and websocket flow.

## Coverage

- Server receives requests.
- Wallet validates, locks balances, and publishes to `engine.input`.
- The independently managed engine consumes `engine.input` and publishes
  `engine.replies` plus `engine.events`.
- Exchange consumers update Postgres, TimescaleDB, ledger rows, and websocket
  delivery.
- The smoke waits for `wallet_outbox` to drain and verifies unique ledger event
  ids.
