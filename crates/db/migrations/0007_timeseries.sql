CREATE TABLE timeseries_offsets(
  topic             TEXT NOT NULL,
  partition         INT NOT NULL,
  next_offset       BIGINT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(topic, partition),
  CHECK(next_offset >= 0)
);

CREATE TABLE timeseries_trades(
  market_id           BIGINT NOT NULL REFERENCES markets(market_id),
  engine_sequence     BIGINT NOT NULL,
  fill_id             BIGINT NOT NULL,
  engine_timestamp_ms BIGINT NOT NULL,
  executed_at         TIMESTAMPTZ NOT NULL,
  price               BIGINT NOT NULL,
  quantity            BIGINT NOT NULL,
  topic               TEXT NOT NULL,
  partition           INT NOT NULL,
  offset_value        BIGINT NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(market_id, engine_sequence),
  UNIQUE(fill_id),
  UNIQUE(topic, partition, offset_value),
  CHECK(engine_sequence > 0),
  CHECK(engine_timestamp_ms >= 0),
  CHECK(price >= 0),
  CHECK(quantity > 0),
  CHECK(offset_value >= 0)
);

CREATE TABLE candles(
  market_id              BIGINT NOT NULL REFERENCES markets(market_id),
  interval               TEXT NOT NULL,
  bucket_start           TIMESTAMPTZ NOT NULL,
  open                   BIGINT NOT NULL,
  high                   BIGINT NOT NULL,
  low                    BIGINT NOT NULL,
  close                  BIGINT NOT NULL,
  volume                 BIGINT NOT NULL,
  trade_count            BIGINT NOT NULL,
  first_engine_sequence  BIGINT NOT NULL,
  last_engine_sequence   BIGINT NOT NULL,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(market_id, interval, bucket_start),
  CHECK(interval IN ('1m','5m','15m','1h','1d')),
  CHECK(open >= 0),
  CHECK(high >= 0),
  CHECK(low >= 0),
  CHECK(close >= 0),
  CHECK(volume > 0),
  CHECK(trade_count > 0),
  CHECK(first_engine_sequence > 0),
  CHECK(last_engine_sequence >= first_engine_sequence)
);

CREATE INDEX timeseries_trades_market_executed_at_idx
  ON timeseries_trades(market_id, executed_at DESC);

CREATE INDEX candles_market_interval_bucket_desc_idx
  ON candles(market_id, interval, bucket_start DESC);
