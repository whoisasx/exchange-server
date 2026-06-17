CREATE TABLE ledger_offsets(
  topic             TEXT NOT NULL,
  partition         INT NOT NULL,
  next_offset       BIGINT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(topic, partition),
  CHECK(next_offset >= 0)
);

CREATE TABLE ledger_events(
  event_id          BIGSERIAL PRIMARY KEY NOT NULL,
  topic             TEXT NOT NULL,
  partition         INT NOT NULL,
  offset_value      BIGINT NOT NULL,
  event_type        TEXT NOT NULL,
  user_id           BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  payload           JSONB NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE(topic, partition, offset_value),
  CHECK(offset_value >= 0)
);

CREATE TABLE ledger_entries(
  entry_id          BIGSERIAL PRIMARY KEY NOT NULL,
  event_id          BIGINT NOT NULL REFERENCES ledger_events(event_id) ON DELETE CASCADE,
  user_id           BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  asset             asset_type NOT NULL,
  kind              TEXT NOT NULL,
  total_delta       BIGINT NOT NULL,
  locked_delta      BIGINT NOT NULL,
  reference_id      TEXT NOT NULL,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK(kind IN ('DEPOSIT','WITHDRAWAL','RESERVE','RELEASE','TRADE_DEBIT','TRADE_CREDIT'))
);

CREATE INDEX ledger_events_user_type_idx ON ledger_events(user_id, event_type);
CREATE INDEX ledger_entries_user_asset_idx ON ledger_entries(user_id, asset);
CREATE INDEX ledger_entries_reference_idx ON ledger_entries(reference_id);
