# Timeseries Service

`apps/timeseries` consumes `engine.events` and writes market candles.

Run:

```sh
cargo run -p timeseries
```

The service ignores non-trade engine events. For every `TradeExecuted`, it writes:

- `timeseries_trades`: one idempotency row keyed by `(market_id, engine_sequence)`.
- `candles`: OHLCV rows for `1m`, `5m`, `15m`, `1h`, and `1d`.
- `timeseries_offsets`: consumed Redpanda offsets.

`engine_sequence` is the ordering and idempotency key. `engine_timestamp_ms` is used to choose the candle bucket.

Read candles through the REST server:

```text
GET /api/markets/{market_id}/candles?interval=1m&start_ms=1710000000000&end_ms=1710003600000&limit=500
```

Supported intervals are `1m`, `5m`, `15m`, `1h`, and `1d`. The response is sorted oldest to newest after selecting the latest matching rows up to `limit`.
