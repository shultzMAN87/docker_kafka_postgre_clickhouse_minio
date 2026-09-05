-- Таблица-приёмник для топика orders.
-- JDBC Sink работает с auto.create=false, поэтому структура задаётся здесь
-- и должна соответствовать Avro-схеме demo.orders.Order из генератора.

CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT PRIMARY KEY,
    customer    TEXT,
    item        TEXT,
    quantity    INTEGER,
    amount      DOUBLE PRECISION,
    created_at  TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_orders_created_at ON orders (created_at);
