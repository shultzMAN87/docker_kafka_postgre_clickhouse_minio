# 21. Kafka MultiSink — кластер KRaft и три разных приёмника

Учебный стенд, объединяющий несколько тем в один работающий контур: кластер
Kafka из трёх нод в режиме KRaft, генератор синтетических данных и три
независимых потребителя, каждый со своим топиком и своим способом чтения.

```
                                  ┌──────────────┐
                          orders  │ Kafka Connect│   JDBC Sink
                       ┌─────────>│   (воркер)   ├──────────────> PostgreSQL
                       │          └──────────────┘
┌───────────┐   ┌──────┴──────┐   ┌──────────────┐
│ Генератор ├──>│   Kafka     │   │ Kafka Connect│   S3 Sink
│ (Python)  │   │  3 брокера  ├──>│   (тот же)   ├──────────────> MinIO
└───────────┘   │   KRaft     │   └──────────────┘
                └──────┬──────┘
                       │ metrics  ┌──────────────┐
                       └─────────>│  ClickHouse  │  движок Kafka
                                  │ Kafka engine │  (без Connect)
                                  └──────────────┘
```

Все три топика передают данные в **Avro**, схемы живут в Schema Registry.

## Что где

| Топик | Приёмник | Механизм | Итог |
|---|---|---|---|
| `orders` | PostgreSQL | Kafka Connect, JDBC Sink | таблица `kafka_sink.orders` |
| `events` | MinIO | Kafka Connect, S3 Sink | Avro-файлы в бакете `kafka-events` |
| `metrics` | ClickHouse | движок Kafka + MATERIALIZED VIEW | таблица `kafka_sink.metrics` |

Три разных механизма выбраны намеренно: JDBC и S3 показывают работу через
Kafka Connect с готовыми плагинами, а ClickHouse — как система может быть
потребителем сама, без промежуточного слоя.

## Запуск

```bash
cp .env.example .env      # при желании поправить пароли и интенсивность
docker compose up -d --build
```

Первая сборка занимает несколько минут: образ Kafka Connect скачивает два
плагина с Confluent Hub. Дальше порядок старта выдерживается автоматически
через healthcheck'и и `depends_on`, вмешиваться не нужно.

Посмотреть, что происходит:

```bash
docker compose logs -f kafka-init            # создание топиков
docker compose logs -f register-connectors   # регистрация коннекторов
docker compose logs -f generator             # поток синтетики
```

Остановка с сохранением данных — `docker compose down`.
Полный сброс, включая тома — `docker compose down -v`.

## Интерфейсы

| Сервис | Адрес | Доступ |
|---|---|---|
| Kafka UI | http://localhost:8086 | — |
| Schema Registry | http://localhost:8081 | — |
| REST Proxy | http://localhost:8082 | — |
| Kafka Connect REST | http://localhost:8083 | — |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| pgAdmin | http://localhost:5050 | из `.env` |
| ClickHouse HTTP | http://localhost:8123 | `default` / `clickhouse` |

Брокеры снаружи доступны как `localhost:29092`, `29093`, `29094`.
Внутри сети контейнеров — `kafka-0:9091` и так далее.

## Проверка, что всё доехало

**PostgreSQL:**

```bash
docker exec -it postgres psql -U postgres -d kafka_sink \
  -c "SELECT count(*), max(created_at) FROM orders;"
```

**ClickHouse:**

```bash
docker exec -it clickhouse clickhouse-client --password clickhouse \
  --query "SELECT host, metric, count(), round(avg(value),2) FROM kafka_sink.metrics GROUP BY host, metric ORDER BY host"
```

**MinIO:** откройте консоль на порту 9001, бакет `kafka-events`. Файлы
появляются по мере накопления: S3 Sink пишет их пачками по `flush.size`
(50 записей) либо раз в минуту, в структуре `year=/month=/day=/hour=`.
Первые файлы стоит ждать примерно через минуту после старта генератора.

**Статус коннекторов:**

```bash
curl -s http://localhost:8083/connectors?expand=status | python -m json.tool
```

## Устройство каталога

```
21_Kafka_MultiSink/
├── compose.yml                     весь стенд
├── .env.example                    пароли и настройки генератора
├── connect/
│   └── Dockerfile                  образ Connect с плагинами S3 и JDBC
├── connectors/
│   ├── jdbc-sink-orders.json       конфигурация JDBC Sink
│   └── s3-sink-events.json         конфигурация S3 Sink
├── generator/
│   ├── Dockerfile
│   ├── generator.py                генератор синтетики по трём топикам
│   └── requirements.txt
├── init/
│   ├── postgres/01-orders.sql      таблица-приёмник
│   └── clickhouse/01-metrics.sql   Kafka engine + MergeTree + MV
└── scripts/
    ├── create-topics.sh            создание топиков при старте
    └── register-connectors.sh      регистрация коннекторов при старте
```

## Настройка генератора

Интенсивность задаётся в `.env` и меняется без пересборки образа —
достаточно `docker compose up -d generator`:

| Переменная | По умолчанию | Смысл |
|---|---|---|
| `GEN_INTERVAL_SEC` | 2 | пауза между тиками |
| `GEN_ORDERS_PER_TICK` | 3 | заказов за тик |
| `GEN_EVENTS_PER_TICK` | 10 | событий за тик |
| `GEN_METRICS_PER_TICK` | 20 | метрик за тик |
| `GEN_MAX_TICKS` | 0 | 0 — бесконечно, иначе остановиться после N тиков |

Генератор корректно обрабатывает `docker stop`: дописывает буфер продюсера
и только потом завершается.

## О чём стоит знать

**Схемы задаются в двух местах.** Avro-схема живёт в `generator.py`, а
структура таблицы-приёмника — в `init/postgres/01-orders.sql` и
`init/clickhouse/01-metrics.sql`. Если поменять схему в генераторе, надо
поправить и SQL, иначе JDBC Sink уйдёт в FAILED. Это цена связки
`auto.create=false`, зато видно, что именно создаётся.

**Init-скрипты выполняются один раз.** И PostgreSQL, и ClickHouse запускают
содержимое `docker-entrypoint-initdb.d` только при первичной инициализации
пустого тома. После правки SQL нужен `docker compose down -v`.

**Не читайте `metrics_queue` напрямую.** Таблица с движком Kafka — это
consumer: любое обращение к ней вычитывает сообщения и сдвигает offset,
после чего материализованное представление их уже не увидит. Запросы —
только к `kafka_sink.metrics`.

**Топики создаются с репликацией 3 и `min.insync.replicas=2`.** Это значит,
что кластер переживёт потерю одной ноды, но не двух: при двух упавших
брокерах продюсер с `acks=all` начнёт получать ошибки. Хороший повод
поэкспериментировать — `docker compose stop kafka-2` и посмотреть логи
генератора.

**Данные Kafka лежат в томах.** В отличие от более ранних проектов серии,
здесь `docker compose down` не стирает топики. Для чистого старта нужен
флаг `-v`.

## Если сборка падает

**`md5 checksum verification failed` при установке плагина.**
Загрузка архива с Confluent Hub оборвалась, и файл не сошёлся по
контрольной сумме. Плагин S3 весит несколько сотен мегабайт, так что на
нестабильном канале или через прокси это случается регулярно. В
`connect/Dockerfile` уже заложены пять попыток с паузой, обычно этого
хватает. Если нет — пересоберите только этот образ:

```bash
docker compose build --no-cache kafka-connect
```

Когда загрузка не проходит стабильно, остаётся ручной путь: скачать ZIP
со страниц плагинов на Confluent Hub
([S3](https://www.confluent.io/hub/confluentinc/kafka-connect-s3),
[JDBC](https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc)),
положить их в `connect/plugins/` и переключить `connect/Dockerfile` на
запасной вариант — он там же, в конце файла, закомментирован. Установка
из локального архива идёт той же командой `confluent-hub install`, только
вместо имени компонента указывается путь к файлу.

**Плагины ставятся двумя отдельными слоями.** Сделано намеренно: если
упадёт S3, уже установленный JDBC возьмётся из кэша и не будет качаться
заново. Поэтому при повторной сборке не стоит без нужды указывать
`--no-cache` для всего стенда — только для проблемного сервиса.

**Таблица `kafka_sink.metrics` пустая, хотя в топике сообщения есть.**
Проверьте, что говорит сам потребитель:

```sql
SELECT database, table, assignments.current_offset,
       num_messages_read, num_commits, exceptions.text
FROM system.kafka_consumers;
```

Если там `Local: Required feature not supported by broker`, а
`num_messages_read` растёт при `num_commits = 0` и `current_offset = -1001` —
это несовместимость версий, а не ошибка конфигурации. Kafka 4.x (то есть
`cp-kafka:8.1.0` и новее) убрала старые версии протокольных API по
KIP-896, а ClickHouse со старым форком librdkafka продолжает их
запрашивать. Сообщения при этом вычитываются, но offset'ы не
фиксируются, и всё уходит в отбраковку. Лечится свежим образом
ClickHouse: `clickhouse/clickhouse-server:24.8` уже не годится.

Понижать весь Confluent-стек до 7.9.x (Apache Kafka 3.9) — рабочая
альтернатива, но помните, что KRaft не умеет откатывать метаданные:
понадобится `docker compose down -v`.

**Не делайте `SELECT` из `metrics_queue`.** В графическом клиенте вроде
DBeaver таблица видна в дереве и её хочется открыть. Не надо: это
consumer, каждое чтение вычитывает сообщения и сдвигает offset, после
чего материализованное представление их уже не увидит. Данные просто
исчезнут. Запросы — только к `kafka_sink.metrics`.

**`FORMAT Vertical` не работает через JDBC.** DBeaver сам управляет
форматом вывода и отклоняет `FORMAT` в тексте запроса. Для построчного
вида пользуйтесь `clickhouse-client` в контейнере.

## Что можно достроить

- Авторизация: сейчас всё в PLAINTEXT, шифрование разбирается отдельно
  в проекте `19_Kafka_TLS`, а SASL/ACL не покрыты нигде.
- Source-коннектор: все три приёмника — sink, данные только вытекают из
  Kafka. Обратное направление (например, Debezium и CDC из PostgreSQL)
  напрашивается следующим шагом.
- Kafka Streams или ksqlDB между топиками: пока никакой обработки
  на лету нет, данные идут «как есть».
