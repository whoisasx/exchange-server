CREATE TABLE wallet_reservations(
  reservation_id    TEXT PRIMARY KEY NOT NULL,
  user_id           BIGINT REFERENCES users(user_id) ON DELETE CASCADE NOT NULL,
  asset             asset_type NOT NULL,
  amount            BIGINT NOT NULL,
  remaining         BIGINT NOT NULL,
  status            TEXT NOT NULL,
  idempotency_key   TEXT NOT NULL,
  request_id        TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE(user_id, idempotency_key),
  CHECK(amount > 0),
  CHECK(remaining >= 0),
  CHECK(remaining <= amount),
  CHECK(status IN ('ACTIVE', 'RELEASED', 'SETTLED'))
);

CREATE TABLE wallet_ledger(
  ledger_id         BIGSERIAL PRIMARY KEY NOT NULL,
  user_id           BIGINT REFERENCES users(user_id) ON DELETE CASCADE NOT NULL,
  asset             asset_type NOT NULL,
  amount            BIGINT NOT NULL,
  kind              TEXT NOT NULL,
  reference_id      TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE(user_id, asset, kind, reference_id)
);

CREATE TABLE wallet_idempotency(
  user_id           BIGINT REFERENCES users(user_id) ON DELETE CASCADE NOT NULL,
  command_type      TEXT NOT NULL,
  idempotency_key   TEXT NOT NULL,
  request_id        TEXT NOT NULL,
  reply_payload     JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(user_id, command_type, idempotency_key)
);

CREATE INDEX wallet_reservations_user_status_idx ON wallet_reservations(user_id, status);
CREATE INDEX wallet_ledger_user_asset_idx ON wallet_ledger(user_id, asset);
