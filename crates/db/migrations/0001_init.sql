CREATE TYPE side_type AS ENUM('LONG', 'SHORT');
CREATE TYPE order_type AS ENUM('LIMIT', 'MARKET');
CREATE TYPE margin_type AS ENUM('ISOLATED', 'CROSS');
CREATE TYPE order_status AS ENUM(
  'PENDING',
  'OPEN',
  'FILLED',
  'PARTIAL',
  'CANCELLED',
  'REJECTED'
);

CREATE TYPE close_type AS ENUM('TRADE', 'LIQUIDATION');

CREATE TYPE asset_type AS ENUM(
  'USDC',
  'USDT',
  'SOL',
  'ETH',
  'BTC',
  'PERP',
  'HYP'
);

CREATE TABLE users(
  user_id           BIGINT PRIMARY KEY NOT NULL,
  username          TEXT UNIQUE NOT NULL,
  hashed_password   TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE markets(
  market_id         BIGINT PRIMARY KEY NOT NULL,
  market_name       TEXT UNIQUE NOT NULL,
  base_asset        asset_type NOT NULL,
  quote_asset       asset_type NOT NULL,
  decimal_base      INT NOT NULL,
  decimal_quote     INT NOT NULL,
  last_traded_price BIGINT NOT NULL DEFAULT 0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE user_collaterals(
  user_id           BIGINT REFERENCES users(user_id) ON DELETE CASCADE NOT NULL,
  asset             asset_type NOT NULL,
  total             BIGINT NOT NULL DEFAULT 0,
  locked            BIGINT NOT NULL DEFAULT 0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (user_id, asset),
  CHECK(total>=0),
  CHECK(locked>=0),
  CHECK(locked<=total)
);

CREATE TABLE orders(
  order_id          BIGINT PRIMARY KEY NOT NULL,
  user_id           BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  market_id         BIGINT NOT NULL REFERENCES markets(market_id),
  market_name       TEXT NOT NULL,
  side              side_type NOT NULL,
  order_type        order_type NOT NULL,
  quantity          BIGINT NOT NULL,
  price             BIGINT NOT NULL,
  status            order_status NOT NULL DEFAULT 'PENDING',
  margin            BIGINT NOT NULL DEFAULT 0,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (quantity > 0),
  CHECK (price >= 0),
  CHECK (margin >= 0)
);

CREATE TABLE positions(
  position_id         BIGINT PRIMARY KEY NOT NULL,
  user_id             BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  market_id           BIGINT NOT NULL REFERENCES markets(market_id),
  market_name         TEXT NOT NULL,
  side                side_type NOT NULL,
  quantity            BIGINT NOT NULL,
  unrealized_pnl      BIGINT NOT NULL,
  initial_margin      BIGINT NOT NULL,
  maintenance_margin  BIGINT NOT NULL,
  margin_chosen       margin_type NOT NULL,
  liquidation_price   BIGINT NOT NULL,
  average_price       BIGINT NOT NULL,
  opened_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (user_id,market_id),
  CHECK (quantity>0)
);

CREATE TABLE closed_positions(
  position_id               BIGINT PRIMARY KEY NOT NULL,
  user_id                   BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  market_id                 BIGINT NOT NULL REFERENCES markets(market_id),
  market_name               TEXT NOT NULL,
  side                      side_type NOT NULL,
  quantity                  BIGINT NOT NULL,
  entry_price               BIGINT NOT NULL,
  exit_price                BIGINT NOT NULL,
  realized_pnl              BIGINT NOT NULL,
  initial_margin            BIGINT NOT NULL,
  closing_fee               BIGINT NOT NULL,
  opened_at                 TIMESTAMPTZ NOT NULL,
  closed_at                 TIMESTAMPTZ NOT NULL DEFAULT now(),
  open_order_id             BIGINT NOT NULL REFERENCES orders(order_id),
  close_order_id            BIGINT NOT NULL REFERENCES orders(order_id),
  close_reason              close_type NOT NULL,

  CHECK (quantity > 0)
);

CREATE TABLE fills(
  fill_id               BIGINT PRIMARY KEY NOT NULL,
  maker_id              BIGINT NOT NULL REFERENCES users(user_id),
  taker_id              BIGINT NOT NULL REFERENCES users(user_id),
  maker_order_id        BIGINT NOT NULL REFERENCES orders(order_id),
  taker_order_id        BIGINT NOT NULL REFERENCES orders(order_id),
  price                 BIGINT NOT NULL,
  quantity              BIGINT NOT NULL,
  maker_position        side_type NOT NULL,
  taker_position        side_type NOT NULL,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (price >= 0),
  CHECK (quantity >0 )
);

CREATE TABLE position_fills(
  position_id             BIGINT NOT NULL REFERENCES positions(position_id) ON DELETE CASCADE,
  fill_id                 BIGINT NOT NULL REFERENCES fills(fill_id) ON DELETE CASCADE,

  PRIMARY KEY (position_id,fill_id)
);

CREATE TABLE closed_position_fills(
  position_id             BIGINT NOT NULL REFERENCES closed_positions(position_id) ON DELETE CASCADE,
  fill_id                 BIGINT NOT NULL REFERENCES fills(fill_id) ON DELETE CASCADE,

  PRIMARY KEY (position_id, fill_id)
);

CREATE INDEX orders_user_id_idx ON orders(user_id);
CREATE INDEX orders_market_status_idx ON orders(market_id, status);
CREATE INDEX positions_user_id_idx ON positions(user_id);
CREATE INDEX closed_positions_user_market_idx ON closed_positions(user_id,
market_id);
CREATE INDEX fills_maker_id_idx ON fills(maker_id);
CREATE INDEX fills_taker_id_idx ON fills(taker_id);