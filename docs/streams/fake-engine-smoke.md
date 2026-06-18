# Fake Engine Smoke Harness

`apps/fake-engine` is a development-only stand-in for the external C++ engine. It speaks the real stream contract, so server, wallet, and projector can be tested before the C++ engine is available.

Run the automated smoke:

```sh
scripts/e2e-smoke.sh
```

The script starts local Postgres and Redpanda with Docker Compose, creates the stream topics, starts `wallet`, `projector`, `timeseries`, `ledger`, `fake-engine`, `ws`, and `server`, then drives the REST and websocket flow with `apps/e2e-smoke`.

To run it manually, start the services alongside each other:

```sh
cargo run -p wallet
cargo run -p projector
cargo run -p timeseries
cargo run -p ledger
cargo run -p fake-engine
cargo run -p ws
cargo run -p server
```

Behavior:

- Consumes `engine.commands` from latest offsets.
- Observes `wallet.events` so trade and cancel events can use wallet-compatible reservation amounts.
- Publishes `OrderAccepted` or `CancelAccepted` replies to `engine.replies` using the command envelope's `reply_partition`.
- Publishes `OrderOpened`, `OrderCancelled`, and `TradeExecuted` events to `engine.events` keyed by `market_id`, with per-market `engine_sequence` and engine-assigned `engine_timestamp_ms`.
- Matches a new order against the oldest opposite-side resting order on the same market when prices cross.

Smoke path:

1. Deposit enough collateral for two users.
2. Place a limit order for user A.
3. Place an opposite-side limit order for user B on the same market and crossing price.
4. Connect to `GET /ws?token=<jwt>` for each user and subscribe to the traded market.
5. Confirm the server receives engine replies, wallet receives settlement events, projector writes the order/fill read models, timeseries writes candles, ledger writes audit entries, and websocket clients receive account plus market updates.

The fake engine is intentionally not a production matching engine. It exists only to validate stream wiring and consumer behavior.
