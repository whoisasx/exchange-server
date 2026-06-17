CREATE TABLE wallet_queue_offsets(
  topic             TEXT NOT NULL,
  partition         INT NOT NULL,
  next_offset       BIGINT NOT NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY(topic, partition),
  CHECK(next_offset >= 0)
);
