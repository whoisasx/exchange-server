ALTER TABLE orders
  ADD COLUMN reduce_only BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE projector_order_context
  ADD COLUMN reduce_only BOOLEAN NOT NULL DEFAULT false;
