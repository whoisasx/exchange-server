# Engine Stream Contract

This contract is the wire agreement between this Rust workspace and the external C++ matching engine.

The Rust source of truth is:

- `crates/protocol/src/common.rs`
- `crates/protocol/src/engine.rs`
- `crates/protocol/src/wallet.rs`

All stream messages are JSON encoded with this shape:

```json
{
  "type": "VariantName",
  "payload": {}
}
```

## Topics

| Topic | Producer | Consumer |
| --- | --- | --- |
| `engine.commands` | wallet service | C++ engine |
| `engine.replies` | C++ engine | server reply consumer |
| `engine.events` | C++ engine | wallet service, projector, websocket, ledger, timeseries |

## Partition Keys

| Topic | Record key | Partition rule |
| --- | --- | --- |
| `engine.commands` | `market_id` as string | Wallet publishes each market to a stable partition by key. |
| `engine.replies` | `request_id` | Engine must publish to `payload.envelope.reply_partition` from the original command. |
| `engine.events` | `market_id` as string | Engine should keep events for one market ordered on the same partition. |

Engine events must be self-routeable by payload. WebSocket consumers must not need database lookups to identify the affected account or market.

`engine.events` ordering is defined by the engine payload, not by consumer receive time. For every `market_id`, the engine must emit a strictly increasing `engine_sequence` on every event for that market. Redpanda offsets are still used by consumers for replay progress, but consumers should use `engine_sequence` when reconstructing market state.

## Common Types

Enums are serialized as exact uppercase strings:

```text
Asset: USDC, USDT, SOL, ETH, BTC, PERP, HYP
Side: LONG, SHORT
OrderType: LIMIT, MARKET
ExecutionReason: TRADE, LIQUIDATION
```

All IDs, sequence, timestamp, price, quantity, amount, and margin fields are integer values compatible with signed 64-bit integers.

`CommandEnvelope` is carried from server to wallet to engine:

```json
{
  "request_id": "req_01HZ8YEXAMPLE",
  "idempotency_key": "client-order-1",
  "user_id": 42,
  "reply_partition": 0
}
```

The engine must copy `request_id` into its reply so the server can resolve the original request.

## Commands

### PlaceOrder

Topic: `engine.commands`

Record key: `market_id`

Fixtures:

- `docs/streams/examples/engine-place-order.command.json`
- `docs/streams/examples/engine-place-order-reduce-only.command.json`

Required engine behavior:

- Validate and accept or reject the order.
- Treat `reduce_only=true` as a hard constraint: the order must not increase or reverse the user's position. Reject or expire any unmatched remainder that would create exposure.
- Publish exactly one `OrderAccepted` or `OrderRejected` reply for the command.
- If the order rests on the book, publish `OrderOpened`.
- If the order matches, publish `TradeExecuted`.

Reduce-only conformance cases:

| Case | Expected engine behavior |
| --- | --- |
| `reduce_only=false` normal order | Accept or reject using normal order validation. Resting remainder may publish `OrderOpened`. |
| `reduce_only=true` and user has no reducible position on that market | Publish `OrderRejected`; do not publish order or trade events. |
| `reduce_only=true` and command quantity exceeds current reducible position quantity | Publish `OrderRejected`; do not cap into a smaller order at command acceptance. |
| `reduce_only=true` full close with enough liquidity | Publish `OrderAccepted`, publish matching `TradeExecuted`, and do not publish `OrderOpened` for that order. |
| `reduce_only=true` partial close with enough liquidity for requested quantity | Publish `OrderAccepted`, publish matching `TradeExecuted`, and reduce but do not reverse the position. |
| `reduce_only=true` with partial liquidity below requested quantity | Publish `OrderAccepted`, publish matching `TradeExecuted` for the matched portion, and expire the unmatched remainder without `OrderOpened`. |
| `reduce_only=true` where exposure changes while matching | Stop matching before increasing or reversing exposure, then expire any excess remainder without `OrderOpened`. |

The server validates close quantity against the projector's current position snapshot, but that is not authoritative enough for matching. The engine owns final reduce-only enforcement because it has the current orderbook and position state at match time.

### CancelOrder

Topic: `engine.commands`

Record key: `market_id`

Fixture: `docs/streams/examples/engine-cancel-order.command.json`

Required engine behavior:

- Publish exactly one `CancelAccepted` or `CancelRejected` reply for the command.
- If cancellation releases reserved funds, publish `OrderCancelled`.

### LiquidatePosition

Topic: `engine.commands`

Record key: `market_id`

Fixture: `docs/streams/examples/engine-liquidate-position.command.json`

Required engine behavior:

- Treat liquidation eligibility as engine-owned state. Rust read models may show indicative risk, but they are not authoritative.
- Validate current mark/index price, account exposure, margin state, and available execution path at command time.
- Publish exactly one `LiquidationAccepted` or `LiquidationRejected` reply for the command.
- If accepted and executed, emit `TradeExecuted` with `execution_reason="LIQUIDATION"`.
- The liquidation order must be reduce-only for the liquidated account and must not increase or reverse exposure.
- If the liquidated side is represented as a synthetic taker order, set `taker_reservation_id` to `liquidation_id` so consumers can resolve command context.

## Replies

Replies go to `engine.replies` and must be produced to the original command envelope's `reply_partition`.

| Reply | Fixture |
| --- | --- |
| `OrderAccepted` | `docs/streams/examples/engine-order-accepted.reply.json` |
| `OrderRejected` | `docs/streams/examples/engine-order-rejected.reply.json` |
| `CancelAccepted` | `docs/streams/examples/engine-cancel-accepted.reply.json` |
| `CancelRejected` | `docs/streams/examples/engine-cancel-rejected.reply.json` |
| `LiquidationAccepted` | `docs/streams/examples/engine-liquidation-accepted.reply.json` |
| `LiquidationRejected` | `docs/streams/examples/engine-liquidation-rejected.reply.json` |

Reply variants:

```text
OrderAccepted: request_id, order_id, reservation_id
OrderRejected: request_id, reservation_id, reason
CancelAccepted: request_id, order_id
CancelRejected: request_id, order_id, reason
LiquidationAccepted: request_id, liquidation_id, order_id
LiquidationRejected: request_id, liquidation_id, reason
```

`OrderRejected.reservation_id` may be `null` if no reservation should be released by downstream consumers.

## Events

Events go to `engine.events` and should be keyed by `market_id`.

All engine events include:

```text
engine_sequence: strictly increasing per market_id
engine_timestamp_ms: Unix epoch milliseconds assigned by the engine
```

For ordering, consumers should trust `engine_sequence` before `engine_timestamp_ms`. Timestamps are for bucketing, display, and latency analysis.

| Event | Fixture |
| --- | --- |
| `OrderOpened` | `docs/streams/examples/engine-order-opened.event.json` |
| `OrderCancelled` | `docs/streams/examples/engine-order-cancelled.event.json` |
| `TradeExecuted` | `docs/streams/examples/engine-trade-executed.event.json` |
| `OrderBookDelta` | `docs/streams/examples/engine-orderbook-delta.event.json` |

Event routing fields:

```text
OrderOpened: engine_sequence, engine_timestamp_ms, order_id, reservation_id, user_id, market_id
OrderCancelled: engine_sequence, engine_timestamp_ms, order_id, reservation_id, user_id, market_id, released_amount
TradeExecuted: engine_sequence, engine_timestamp_ms, fill_id, market_id, price, quantity, maker_order_id, taker_order_id, maker_user_id, taker_user_id, maker_reservation_id, taker_reservation_id, execution_reason, settlements
OrderBookDelta: engine_sequence, engine_timestamp_ms, market_id, bids, asks
```

Event consumers use these events as follows:

- Wallet consumes `OrderCancelled` to release reserved funds.
- Wallet consumes `TradeExecuted.settlements` to settle reservations.
- Projector consumes all engine events to update DB read models.
- WebSocket consumes all engine events for live user and market updates.
- Ledger may consume engine events for audit context, while wallet events remain the accounting source.
- Timeseries consumes `TradeExecuted` to build market candles from engine timestamps.
- Projector consumes `OrderBookDelta` to maintain REST orderbook snapshots.

`TradeExecuted.execution_reason` is `TRADE` for normal execution and `LIQUIDATION` for engine-authorized liquidation execution. Projector uses this field only to choose the closed-position reason; it does not decide liquidation.

`TradeExecuted.settlements` is the wallet-facing settlement instruction. Each settlement becomes a wallet `SettleTrade` command. Liquidation trades may omit settlements for synthetic liquidation order IDs unless those IDs correspond to wallet reservations.

`OrderBookDelta` is price-level based. `bids` are LONG-side levels, `asks` are SHORT-side levels, and `quantity=0` means delete that price level.

Orderbook snapshots and deltas use the same per-market sequence. A snapshot carries the latest applied `engine_sequence`. Clients that need a gap-free orderbook should connect to websocket first, subscribe to the market, buffer live `OrderBookDelta` messages, then fetch the REST snapshot. After the snapshot is loaded, discard buffered deltas with `engine_sequence <= snapshot.engine_sequence` and apply only greater sequences.

## Compatibility Rules

- Do not rename JSON fields without updating Rust protocol tests and fixtures.
- Do not change enum casing.
- Always include the top-level `type` and `payload` fields.
- Unknown extra fields are ignored by Rust serde today, but the engine should not rely on that for required behavior.
- New optional fields should be added in a backward-compatible way.
- Required field changes should be treated as a protocol version change.

## Fixture Validation

The protocol crate has tests that deserialize every fixture in `docs/streams/examples`.

Run:

```sh
cargo test -p protocol
```
