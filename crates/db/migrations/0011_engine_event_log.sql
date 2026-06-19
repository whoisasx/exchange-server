CREATE TABLE engine_event_log(
  engine_event_id     TEXT PRIMARY KEY NOT NULL,
  event_type          TEXT NOT NULL,
  market_id           BIGINT REFERENCES markets(market_id),
  engine_sequence     BIGINT,
  engine_timestamp_ms BIGINT NOT NULL,
  topic               TEXT NOT NULL,
  partition           INT NOT NULL,
  offset_value        BIGINT NOT NULL,
  payload             JSONB NOT NULL,
  created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE(topic, partition, offset_value),
  CHECK(engine_event_id <> ''),
  CHECK(engine_sequence IS NULL OR engine_sequence > 0),
  CHECK(engine_timestamp_ms >= 0),
  CHECK(offset_value >= 0)
);

CREATE INDEX engine_event_log_type_created_idx
  ON engine_event_log(event_type, created_at DESC);

CREATE INDEX engine_event_log_market_sequence_idx
  ON engine_event_log(market_id, engine_sequence)
  WHERE market_id IS NOT NULL AND engine_sequence IS NOT NULL;
