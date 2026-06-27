CREATE TABLE wallet_outbox(
  outbox_id        BIGSERIAL PRIMARY KEY NOT NULL,
  dedupe_key       TEXT NOT NULL,
  topic            TEXT NOT NULL,
  partition        INTEGER,
  message_key      TEXT NOT NULL,
  payload_type     TEXT NOT NULL,
  payload          JSONB NOT NULL,
  status           TEXT NOT NULL DEFAULT 'PENDING',
  attempts         INTEGER NOT NULL DEFAULT 0,
  next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_error       TEXT,
  published_partition INTEGER,
  published_offset BIGINT,
  published_at     TIMESTAMPTZ,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE(dedupe_key),
  CHECK(partition IS NULL OR partition >= 0),
  CHECK(published_partition IS NULL OR published_partition >= 0),
  CHECK(published_offset IS NULL OR published_offset >= 0),
  CHECK(attempts >= 0),
  CHECK(status IN ('PENDING', 'PROCESSING', 'PUBLISHED'))
);

CREATE INDEX wallet_outbox_pending_idx
  ON wallet_outbox(next_attempt_at, outbox_id)
  WHERE status='PENDING';

CREATE INDEX wallet_outbox_processing_idx
  ON wallet_outbox(updated_at, outbox_id)
  WHERE status='PROCESSING';
