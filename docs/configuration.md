# Exchange Configuration

Start from `.env.example` for local service runs. The test harness exports the
standard local values when it starts services.

## Main Settings

| Environment variable | Purpose |
|---|---|
| `DATABASE_URL` | Postgres exchange state |
| `TIMESERIES_DATABASE_URL` | TimescaleDB candles and trades |
| `REDPANDA_BROKERS` | Redpanda broker list |
| `S3_*` | MinIO/S3 checkpoint settings used by tests and engine-adjacent flows |
| `JWT_SECRET` | API auth signing secret |
| `SERVER_*` | HTTP server settings |
| `WS_*` | Websocket service settings |

## Local Harness Wiring

```mermaid
flowchart LR
    env[.env / harness env] --> server[apps/server]
    env --> wallet[apps/wallet]
    env --> projector[apps/projector]
    env --> ledger[apps/ledger]
    env --> timeseries[apps/timeseries]
    env --> ws[apps/ws]

    server --> pg[(Postgres)]
    wallet --> pg
    projector --> pg
    ledger --> pg
    timeseries --> ts[(TimescaleDB)]
    wallet --> rp[(Redpanda)]
    ws --> rp
```

The standard local endpoints are:

- Postgres: `postgres://postgres:postgres@127.0.0.1:55432/exchange`
- TimescaleDB: `postgres://postgres:postgres@127.0.0.1:55433/exchange_timeseries`
- Redpanda: `127.0.0.1:19092`
- MinIO: `http://127.0.0.1:59000`

Use the harness for the expected local setup:

```sh
test-harness/infra.sh up
test-harness/smoke.sh
```
