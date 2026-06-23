CREATE SEQUENCE order_id_seq AS BIGINT MINVALUE 1 START WITH 1;

SELECT setval(
  'order_id_seq',
  COALESCE((SELECT MAX(order_id) FROM orders), 0) + 1,
  false
);
