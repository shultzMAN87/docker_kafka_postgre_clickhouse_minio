-- ClickHouse читает топик metrics НАПРЯМУЮ, минуя Kafka Connect.
-- Классическая связка из трёх объектов:
--   1) таблица с движком Kafka   — это и есть consumer;
--   2) обычная MergeTree-таблица — постоянное хранилище;
--   3) MATERIALIZED VIEW         — перекладывает данные из (1) в (2).
--
-- Читать напрямую из таблицы Kafka не нужно: каждое чтение вычитывает
-- сообщения и сдвигает offset. Запросы делаются к metrics.

CREATE DATABASE IF NOT EXISTS kafka_sink;

-- 1. Consumer
CREATE TABLE IF NOT EXISTS kafka_sink.metrics_queue
(
    host    String,
    metric  String,
    value   Float64,
    ts      Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka-0:9091,kafka-1:9091,kafka-2:9091',
    kafka_topic_list = 'metrics',
    kafka_group_name = 'clickhouse-metrics',
    kafka_format = 'AvroConfluent',
    format_avro_schema_registry_url = 'http://schema-registry:8081',
    kafka_num_consumers = 1,
    kafka_max_block_size = 1048576;

-- 2. Хранилище
CREATE TABLE IF NOT EXISTS kafka_sink.metrics
(
    host        String,
    metric      String,
    value       Float64,
    ts          DateTime64(3),
    inserted_at DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(ts)
ORDER BY (host, metric, ts);

-- 3. Перекладка. ts приходит как миллисекунды с эпохи -> DateTime64(3).
CREATE MATERIALIZED VIEW IF NOT EXISTS kafka_sink.metrics_mv
TO kafka_sink.metrics
AS
SELECT
    host,
    metric,
    value,
    toDateTime64(ts / 1000, 3) AS ts
FROM kafka_sink.metrics_queue;
