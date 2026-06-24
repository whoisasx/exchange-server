# Engine System Plan

This plan describes the exchange flow with the C++ engine wired as the matching
and market-risk owner. The Rust workspace keeps the API, wallet, projector,
websocket, ledger, timeseries, and ingress services.

## Decisions

- One engine process can own many markets for now.
- Margin mode is isolated per `user_id + market_id`.
- All engine-affecting inputs must enter one ordered engine input log.
- The engine input log is not partitioned by market for MVP. The dispatcher reads it in order and routes each input to the owning market thread.
- The wallet outbox relay is the publisher into the engine input log. Wallet
  enqueues order/cancel inputs after collateral checks; `engine-ingress`
  enqueues mark price and funding inputs.
- Mark price is a separate ordered input, not attached to every user order.
- Funding rate is a separate ordered input, sent on the funding cadence.
- Liquidation runs after trades, mark updates, funding settlement, and startup restore.
- Engine recovery uses private engine checkpoints. Orderbook snapshots are public market-data artifacts and are not enough for recovery.
- Queue retention can stay at 30 minutes for now, with the known limitation that recovery fails if the latest valid checkpoint is older than retained input data.

## High-Level Flow

```text
API servers
  -> wallet.commands
  -> wallet / engine-ingress
  -> wallet_outbox
  -> wallet outbox relay
  -> engine.input
  -> engine dispatcher
  -> market threads
  -> engine.replies
  -> engine.events
  -> wallet, projector, websocket, server, ledger, timeseries
```

## API Server Responsibilities

The API server handles user-facing and static validation before publishing to `wallet.commands`.

It should:

- authenticate the user
- create the `CommandEnvelope`
- validate market existence
- validate price tick, quantity lot size, min/max quantity, and order type
- validate leverage is within the market's allowed bounds
- compute the required initial margin
- reject malformed requests from direct curl/API clients
- publish `PlaceOrderIntent` or `CancelOrderIntent` to `wallet.commands`

It should not:

- decide fillability
- enforce reduce-only authoritatively
- calculate liquidation eligibility
- calculate current PnL
- trust client-supplied margin, market metadata, or risk state

## Wallet Responsibilities

Wallet remains the hot-path collateral and reservation owner.

It should:

- own deposits, withdrawals, balances, locked funds, and reservations
- deduplicate wallet commands
- reserve collateral for valid `PlaceOrderIntent` commands
- reject insufficient-fund requests before engine ingress
- enqueue reserved orders to `wallet_outbox` for `engine.input`
- enqueue cancel requests to `wallet_outbox` for `engine.input`
- apply engine-originated account deltas, releases, fees, trade settlement, and funding settlement
- enqueue wallet events in `wallet_outbox`; the wallet relay publishes them to `wallet.events` as the accounting source consumed by ledger

It should not:

- calculate mark price meaning
- calculate funding rate meaning
- own orderbook state
- own position state
- own liquidation decisions
- own ADL decisions
- own reduce-only enforcement
- own matching, fees, or PnL calculation

`engine-ingress` is the durable mark/funding ingress. It parses trusted
mark-price, funding-rate, and funding-settlement inputs, writes an idempotent
`wallet_outbox` row targeting `engine.input`, and leaves Redpanda publishing to
the wallet outbox relay.

## Engine Inputs

All engine inputs should be ordered by the final engine input log. The engine dispatcher reads the log in offset order and routes each input to the owning market thread. Per-market threads may process independently after dispatch, but recovery is anchored to the single engine input offset.

Minimum input variants:

- `PlaceOrder`
- `CancelOrder`
- `MarkPriceUpdated`
- `FundingRateUpdated`
- `FundingSettlementTick`

Later input variants:

- `AddIsolatedMargin`
- `RemoveIsolatedMargin`
- `RiskConfigUpdated`
- `MarketConfigUpdated`
- trusted keeper hints such as `EvaluateLiquidation`

### PlaceOrder

`PlaceOrder` must be self-contained enough for the engine to perform final risk and settlement decisions.

Required fields:

- envelope
- reservation_id
- market_id
- market_name
- side
- order_type
- quantity
- price
- reduce_only
- margin_asset
- reserved_margin_amount
- leverage

The engine owns final validation because it has current orderbook, position, mark price, fee, and isolated margin state.

### MarkPriceUpdated

Mark price is a first-class risk input. It is not included inside every user order.

Conceptual fields:

- market_id
- mark_price
- index_price
- source_timestamp_ms
- published_at_ms
- valid_until_ms
- source_sequence
- source_status

Publishing policy for MVP:

- publish every 5 seconds per market, or
- publish immediately if mark moves by at least 0.25%, or
- publish immediately if source status changes

The engine keeps the latest valid mark per market. If the mark is stale, the engine should reject risk-increasing orders and allow reduce-only and cancel actions.

### FundingRateUpdated

Funding calculation can live outside the engine for now. The resulting rate enters the engine as an ordered input.

Conceptual fields:

- market_id
- funding_interval_id
- rate
- interval_start_ms
- interval_end_ms
- source_timestamp_ms

The engine stores the latest funding rate and interval in market state and checkpoint state.

### FundingSettlementTick

Funding settlement should be explicit. A rate update tells the engine the current rate; a settlement tick tells it to apply funding for an interval.

Conceptual fields:

- market_id
- funding_interval_id
- settle_at_ms

On this input, the engine applies funding to open isolated positions, emits account deltas, and then runs liquidation for the market because funding can change account equity.

## Engine Runtime Model

```text
engine.input
  -> dispatcher
  -> market thread by market_id
```

Each market thread owns:

- orderbook
- open orders
- user isolated positions for that market
- mark price and index price
- funding state
- fee schedule
- liquidation and bankruptcy rules
- insurance fund state
- ADL ranking state
- per-market engine sequence

There is no cross-market risk in the MVP. A BTC-PERP mark update should only run BTC-PERP risk logic.

## Engine Processing Loop

For each input:

1. Deduplicate the input.
2. Route it to the market thread.
3. Apply the input to market state.
4. Match orders if applicable.
5. Update position, margin, fee, funding, and orderbook state.
6. Run the liquidation loop if the input can affect risk.
7. Append replies and events.
8. Publish replies and events.
9. Advance checkpoint/outbox state.

## Liquidation Triggers

Run the liquidation loop after:

- every `TradeExecuted`
- every `MarkPriceUpdated`
- every `FundingSettlementTick`
- startup restore after replay catches up

Optional targeted checks:

- after a cancel releases margin or removes open exposure
- after margin add/remove support is added
- after trusted keeper `EvaluateLiquidation` hints

Do not rely only on user order flow. A user can become liquidatable when mark price moves even if no one places an order.

## Liquidation Loop

For isolated margin, liquidation is market-local.

Conceptual loop:

1. Track candidates by margin ratio or liquidation price.
2. Recompute candidate health using the latest mark price.
3. If unhealthy, freeze the user-market position for liquidation.
4. Cancel or ignore exposure-increasing open orders for that user-market.
5. Recheck health after any internal release.
6. Execute a synthetic reduce-only liquidation order.
7. Charge liquidation fee.
8. Route losses to insurance fund if needed.
9. Trigger ADL if insurance/liquidity is insufficient.
10. Continue until the account is healthy, flat, blocked by no liquidity, or loop budget is reached.

## Engine Outputs

Replies are request lifecycle messages only. Durable side effects must come from events.

Reply examples:

- `OrderAccepted`
- `OrderRejected`
- `CancelAccepted`
- `CancelRejected`

Event examples:

- `OrderOpened`
- `OrderCancelled`
- `OrderExpired`
- `ReservationReleased`
- `TradeExecuted`
- `OrderBookDelta`
- `MarkPriceUpdated`
- `FundingRateUpdated`
- `FundingPaymentApplied`
- `PositionChanged`
- `RiskStateUpdated`
- `FeeCharged`
- `LiquidationStarted`
- `LiquidationExecuted`
- `LiquidationCompleted`
- `AdlExecuted`
- `AccountDelta`

Every engine event should include:

- `engine_event_id`
- `market_id`
- `engine_sequence`
- `engine_timestamp_ms`
- source input offset or source input id

Consumers should use `(market_id, engine_sequence)` for market ordering. Global ordering across markets is not guaranteed.

## Consumer Responsibilities

Wallet consumes engine events that move money:

- trade settlement
- reservation release
- funding payment
- fee charge
- liquidation settlement
- insurance fund transfer
- ADL settlement

Projector consumes engine events for read models:

- orders
- fills
- positions
- orderbook
- risk state
- funding history
- mark price history

Websocket consumes engine and wallet events for live fanout. It does not own recovery.

Server reply consumers use wallet and engine replies to unblock HTTP requests. Replies are not durable accounting facts.

Ledger consumes `wallet.events` as the accounting source. Engine events may be audit context, but wallet events are the ledger source for balance mutations.

Wallet event schema and ledger/ws routing fields are documented in
`docs/wallet-events.md`.

Timeseries consumes `TradeExecuted` for candles and trade history. It should use `engine_timestamp_ms` for buckets and `(market_id, engine_sequence)` for idempotency.

## Recovery

Use two snapshot types:

- `OrderBookSnapshot`: public market-data dump for S3/history/client recovery.
- `EngineCheckpoint`: private engine recovery artifact.

An `EngineCheckpoint` must include:

- all market orderbooks with order priority
- all open orders
- all isolated positions
- latest mark and index prices
- funding rate and interval state
- fee config version
- insurance fund state
- ADL state
- generated ID counters
- per-market engine sequences
- processed input IDs
- engine input offset

Restore flow:

1. Load latest complete engine checkpoint.
2. Verify checksum and schema/config version.
3. Verify the checkpoint input offset is still within retained input data.
4. Replay engine inputs after the checkpoint in silent/rebuild mode.
5. Do not publish during recovery replay.
6. Once caught up, switch to live mode and publish outputs for new inputs.
7. Run invariant checks before serving live traffic.

For MVP, queue retention remains 30 minutes. The engine must fail loudly if it cannot replay from the latest checkpoint because the input queue has already discarded needed records.

## Example Flow

### Mark Update

```text
wallet/engine-ingress -> wallet_outbox -> engine.input:
MarkPriceUpdated {
  market_id: 1,
  mark_price: 100,
  index_price: 99,
  valid_until_ms: 1710000005000
}
```

Engine applies the mark update and runs liquidation for market `1`.

```text
engine.events:
MarkPriceUpdated {
  market_id: 1,
  engine_sequence: 10,
  mark_price: 100
}
```

### User Opens Position

```text
backend -> wallet.commands:
PlaceOrderIntent {
  user_id: 42,
  market_id: 1,
  side: LONG,
  quantity: 10,
  price: 100,
  leverage: 10,
  required_margin: 100
}
```

Wallet reserves collateral.

```text
wallet.events:
FundsReserved {
  user_id: 42,
  reservation_id: "res_1",
  asset: USDC,
  amount: 100
}
```

Wallet forwards the reserved order.

```text
wallet/engine-ingress -> wallet_outbox -> engine.input:
PlaceOrder {
  reservation_id: "res_1",
  market_id: 1,
  side: LONG,
  quantity: 10,
  price: 100,
  margin_asset: USDC,
  reserved_margin_amount: 100,
  leverage: 10
}
```

Engine accepts and rests the order.

```text
engine.replies:
OrderAccepted {
  request_id: "req_1",
  order_id: 7001,
  reservation_id: "res_1"
}

engine.events:
OrderOpened {
  market_id: 1,
  engine_sequence: 11,
  order_id: 7001
}

engine.events:
OrderBookDelta {
  market_id: 1,
  engine_sequence: 12
}
```

### Mark Drops And Liquidation Runs

```text
wallet/engine-ingress -> wallet_outbox -> engine.input:
MarkPriceUpdated {
  market_id: 1,
  mark_price: 80,
  index_price: 80
}
```

Engine applies the mark, detects an unhealthy isolated position, and liquidates.

```text
engine.events:
MarkPriceUpdated {
  market_id: 1,
  engine_sequence: 13,
  mark_price: 80
}

engine.events:
LiquidationStarted {
  market_id: 1,
  engine_sequence: 14,
  user_id: 42
}

engine.events:
TradeExecuted {
  market_id: 1,
  engine_sequence: 15,
  execution_reason: LIQUIDATION
}

engine.events:
FeeCharged {
  market_id: 1,
  engine_sequence: 16
}

engine.events:
PositionChanged {
  market_id: 1,
  engine_sequence: 17,
  user_id: 42
}

engine.events:
LiquidationCompleted {
  market_id: 1,
  engine_sequence: 18,
  user_id: 42
}

engine.events:
AccountDelta {
  market_id: 1,
  engine_sequence: 19,
  user_id: 42
}
```

Wallet applies `AccountDelta` and emits wallet events. Ledger records wallet events. Projector updates read models. Websocket publishes live account and market changes. Timeseries records the liquidation trade.
