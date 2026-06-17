CREATE TABLE projector_offsets(
  topic             TEXT NOT NULL,
  partition         INT NOT NULL,
  next_offset       BIGINT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(topic, partition),
  CHECK(next_offset >= 0)
);

CREATE TABLE projector_order_context(
  reservation_id    TEXT PRIMARY KEY NOT NULL,
  request_id        TEXT NOT NULL,
  order_id          BIGINT UNIQUE,
  user_id           BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  market_id         BIGINT NOT NULL REFERENCES markets(market_id),
  market_name       TEXT NOT NULL,
  side              side_type NOT NULL,
  order_type        order_type NOT NULL,
  quantity          BIGINT NOT NULL,
  price             BIGINT NOT NULL,
  status            TEXT NOT NULL DEFAULT 'PENDING',
  reject_reason     TEXT,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE(request_id),
  CHECK(quantity > 0),
  CHECK(price >= 0),
  CHECK(status IN ('PENDING', 'ACCEPTED', 'REJECTED', 'OPEN', 'PARTIAL', 'FILLED', 'CANCELLED'))
);

CREATE INDEX projector_order_context_order_id_idx ON projector_order_context(order_id);
CREATE INDEX projector_order_context_user_market_idx ON projector_order_context(user_id, market_id);
