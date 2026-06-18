# Orderbook Market Data

The orderbook read model is built from `OrderBookDelta` events on `engine.events`.

`OrderBookDelta` carries changed price levels only:

```json
{
  "type": "OrderBookDelta",
  "payload": {
    "engine_sequence": 4,
    "engine_timestamp_ms": 1710000003000,
    "market_id": 1,
    "bids": [{"price": 100, "quantity": 10}],
    "asks": []
  }
}
```

`bids` are LONG-side levels, `asks` are SHORT-side levels, and `quantity=0` removes a level.

The projector writes the current snapshot to `orderbook_state` and `orderbook_levels`.

Read snapshots through the REST server:

```text
GET /api/markets/{market_id}/orderbook?depth=50
```

The response includes `engine_sequence`. Clients should connect to websocket first, subscribe, buffer `OrderBookDelta` events, fetch this snapshot, then apply only deltas with `engine_sequence > snapshot.engine_sequence`.
