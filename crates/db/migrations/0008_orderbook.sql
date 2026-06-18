CREATE TABLE orderbook_events(
  market_id           BIGINT NOT NULL REFERENCES markets(market_id),
  engine_sequence     BIGINT NOT NULL,
  engine_timestamp_ms BIGINT NOT NULL,
  topic               TEXT NOT NULL,
  partition           INT NOT NULL,
  offset_value        BIGINT NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(market_id, engine_sequence),
  UNIQUE(topic, partition, offset_value),
  CHECK(engine_sequence > 0),
  CHECK(engine_timestamp_ms >= 0),
  CHECK(offset_value >= 0)
);

CREATE TABLE orderbook_state(
  market_id           BIGINT PRIMARY KEY NOT NULL REFERENCES markets(market_id),
  engine_sequence     BIGINT NOT NULL,
  engine_timestamp_ms BIGINT NOT NULL,
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK(engine_sequence > 0),
  CHECK(engine_timestamp_ms >= 0)
);

CREATE TABLE orderbook_levels(
  market_id            BIGINT NOT NULL REFERENCES markets(market_id),
  side                 TEXT NOT NULL,
  price                BIGINT NOT NULL,
  quantity             BIGINT NOT NULL,
  last_engine_sequence BIGINT NOT NULL,
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(market_id, side, price),
  CHECK(side IN ('BID','ASK')),
  CHECK(price >= 0),
  CHECK(quantity > 0),
  CHECK(last_engine_sequence > 0)
);

CREATE INDEX orderbook_levels_bid_idx
  ON orderbook_levels(market_id, price DESC)
  WHERE side='BID';

CREATE INDEX orderbook_levels_ask_idx
  ON orderbook_levels(market_id, price ASC)
  WHERE side='ASK';
