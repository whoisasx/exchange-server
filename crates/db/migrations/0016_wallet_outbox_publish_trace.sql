ALTER TABLE wallet_outbox
  ADD COLUMN IF NOT EXISTS published_partition INTEGER,
  ADD COLUMN IF NOT EXISTS published_offset BIGINT;

ALTER TABLE wallet_outbox
  DROP CONSTRAINT IF EXISTS wallet_outbox_published_partition_check,
  ADD CONSTRAINT wallet_outbox_published_partition_check
    CHECK(published_partition IS NULL OR published_partition >= 0);

ALTER TABLE wallet_outbox
  DROP CONSTRAINT IF EXISTS wallet_outbox_published_offset_check,
  ADD CONSTRAINT wallet_outbox_published_offset_check
    CHECK(published_offset IS NULL OR published_offset >= 0);
