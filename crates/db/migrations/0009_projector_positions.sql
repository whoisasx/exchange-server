CREATE SEQUENCE projector_position_id_seq AS BIGINT;

SELECT setval(
  'projector_position_id_seq',
  GREATEST(
    COALESCE((SELECT MAX(position_id) FROM positions), 0),
    COALESCE((SELECT MAX(position_id) FROM closed_positions), 0),
    1
  ),
  GREATEST(
    COALESCE((SELECT MAX(position_id) FROM positions), 0),
    COALESCE((SELECT MAX(position_id) FROM closed_positions), 0)
  ) > 0
);

ALTER TABLE positions ADD COLUMN open_order_id BIGINT REFERENCES orders(order_id);

CREATE INDEX positions_open_order_id_idx ON positions(open_order_id);
