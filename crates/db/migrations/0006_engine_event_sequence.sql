ALTER TABLE fills ADD COLUMN market_id BIGINT REFERENCES markets(market_id);
ALTER TABLE fills ADD COLUMN engine_sequence BIGINT;
ALTER TABLE fills ADD COLUMN executed_at TIMESTAMPTZ;

UPDATE fills
SET
  market_id=orders.market_id,
  engine_sequence=fills.fill_id,
  executed_at=fills.created_at
FROM orders
WHERE orders.order_id=fills.maker_order_id;

ALTER TABLE fills ALTER COLUMN market_id SET NOT NULL;
ALTER TABLE fills ALTER COLUMN engine_sequence SET NOT NULL;
ALTER TABLE fills ALTER COLUMN executed_at SET NOT NULL;

ALTER TABLE fills ADD CONSTRAINT fills_engine_sequence_positive CHECK(engine_sequence > 0);
ALTER TABLE fills ADD CONSTRAINT fills_market_engine_sequence_unique UNIQUE(market_id, engine_sequence);

CREATE INDEX fills_market_executed_at_idx ON fills(market_id, executed_at DESC);
