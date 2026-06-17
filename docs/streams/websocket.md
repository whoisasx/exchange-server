# WebSocket Live Updates

`apps/ws` is the live update service for connected clients. It consumes durable stream events and fans them out to authenticated sockets. It does not connect to Postgres; `engine.events` and `wallet.events` must carry the account and market routing fields needed by websocket clients.

Run:

```sh
cargo run -p ws
```

Connect:

```text
GET /ws?token=<jwt>
Authorization: Bearer <jwt>
```

The token uses the same HS256 `JWT_SECRET` and claim shape as the REST server: `userid`, `username`, and `exp`.

## Client Messages

Subscribe to market feeds:

```json
{"type":"Subscribe","payload":{"markets":[1,2]}}
```

Unsubscribe from market feeds:

```json
{"type":"Unsubscribe","payload":{"markets":[1]}}
```

Application-level ping:

```json
{"type":"Ping","payload":{"nonce":"optional"}}
```

## Server Messages

Private account updates are sent to the authenticated user:

```json
{
  "type": "AccountEvent",
  "payload": {
    "source": "engine",
    "event": {"type":"OrderOpened","payload":{}},
    "metadata": {"topic":"engine.events","partition":0,"offset":123}
  }
}
```

Market updates are sent only after subscribing to a market:

```json
{
  "type": "MarketEvent",
  "payload": {
    "market_id": 1,
    "source": "engine",
    "event": {"type":"TradeExecuted","payload":{}},
    "metadata": {"topic":"engine.events","partition":0,"offset":124}
  }
}
```

The service starts from latest Redpanda offsets. Clients should fetch initial state from REST endpoints before opening a socket.
