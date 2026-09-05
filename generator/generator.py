"""
Генератор синтетических данных для стенда Kafka MultiSink.

Пишет в три топика, каждый — со своей Avro-схемой:

    orders  -> потом уезжает в PostgreSQL через JDBC Sink
    events  -> потом уезжает в MinIO через S3 Sink
    metrics -> напрямую вычитывается ClickHouse'ом (движок Kafka)

Схемы регистрируются в Schema Registry автоматически при первой отправке.
Все настройки — через переменные окружения (см. compose.yml).
"""

import os
import random
import signal
import string
import sys
import time
import uuid
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext

# ============================================================
# НАСТРОЙКИ
# ============================================================

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "kafka-0:9091")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")

TOPIC_ORDERS = os.getenv("TOPIC_ORDERS", "orders")
TOPIC_EVENTS = os.getenv("TOPIC_EVENTS", "events")
TOPIC_METRICS = os.getenv("TOPIC_METRICS", "metrics")

INTERVAL_SEC = float(os.getenv("GEN_INTERVAL_SEC", "2"))
ORDERS_PER_TICK = int(os.getenv("GEN_ORDERS_PER_TICK", "3"))
EVENTS_PER_TICK = int(os.getenv("GEN_EVENTS_PER_TICK", "10"))
METRICS_PER_TICK = int(os.getenv("GEN_METRICS_PER_TICK", "20"))
MAX_TICKS = int(os.getenv("GEN_MAX_TICKS", "0"))  # 0 = бесконечно

# ============================================================
# AVRO-СХЕМЫ
# ============================================================

SCHEMA_ORDER = """
{
  "type": "record",
  "name": "Order",
  "namespace": "demo.orders",
  "fields": [
    {"name": "order_id",   "type": "string"},
    {"name": "customer",   "type": "string"},
    {"name": "item",       "type": "string"},
    {"name": "quantity",   "type": "int"},
    {"name": "amount",     "type": "double"},
    {"name": "created_at", "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""

SCHEMA_EVENT = """
{
  "type": "record",
  "name": "Event",
  "namespace": "demo.events",
  "fields": [
    {"name": "event_id",   "type": "string"},
    {"name": "session_id", "type": "string"},
    {"name": "event_type", "type": "string"},
    {"name": "page",       "type": "string"},
    {"name": "ts",         "type": {"type": "long", "logicalType": "timestamp-millis"}}
  ]
}
"""

# У метрик ts — просто long (миллисекунды), без logicalType.
# ClickHouse читает его в Int64 и превращает в DateTime64 внутри
# материализованного представления.
SCHEMA_METRIC = """
{
  "type": "record",
  "name": "Metric",
  "namespace": "demo.metrics",
  "fields": [
    {"name": "host",   "type": "string"},
    {"name": "metric", "type": "string"},
    {"name": "value",  "type": "double"},
    {"name": "ts",     "type": "long"}
  ]
}
"""

# ============================================================
# СПРАВОЧНИКИ ДЛЯ СИНТЕТИКИ
# ============================================================

CUSTOMERS = [
    "ООО Ромашка", "ЗАО Вектор", "ИП Иванов", "ООО Сириус",
    "АО Прогресс", "ООО Мастер", "ИП Петрова", "ООО Логист",
]

ITEMS = [
    "Ноутбук", "Монитор 27\"", "Клавиатура", "Мышь беспроводная",
    "SSD 1 ТБ", "Кабель HDMI", "Док-станция", "Веб-камера",
]

EVENT_TYPES = ["page_view", "click", "add_to_cart", "checkout", "search", "logout"]
PAGES = ["/", "/catalog", "/catalog/laptops", "/cart", "/checkout", "/profile", "/search"]

HOSTS = ["app-01", "app-02", "app-03", "db-01", "worker-01"]
METRIC_NAMES = ["cpu_usage", "mem_usage", "disk_io", "request_latency_ms", "queue_depth"]

# ============================================================
# СЛУЖЕБНОЕ
# ============================================================

_running = True


def _handle_stop(signum, frame):
    """Аккуратная остановка по docker stop / Ctrl+C."""
    global _running
    print(f"\nПолучен сигнал {signum}, останавливаемся...", flush=True)
    _running = False


def now_ms() -> int:
    return int(time.time() * 1000)


def wait_for_schema_registry(url: str, retries: int = 30, delay: int = 5) -> bool:
    print(f"Ожидание Schema Registry: {url}", flush=True)
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(f"{url}/subjects", timeout=5)
            if response.status_code == 200:
                print("Schema Registry доступен", flush=True)
                return True
        except requests.RequestException:
            pass
        print(f"  попытка {attempt}/{retries}...", flush=True)
        time.sleep(delay)
    return False


def delivery_report(err, msg):
    if err is not None:
        print(f"  ОШИБКА доставки в {msg.topic() if msg else '?'}: {err}", flush=True)


# ============================================================
# ГЕНЕРАЦИЯ ЗАПИСЕЙ
# ============================================================

def make_order() -> dict:
    quantity = random.randint(1, 10)
    price = round(random.uniform(500, 90000), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "customer": random.choice(CUSTOMERS),
        "item": random.choice(ITEMS),
        "quantity": quantity,
        "amount": round(price * quantity, 2),
        "created_at": now_ms(),
    }


def make_event(session_id: str) -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "event_type": random.choice(EVENT_TYPES),
        "page": random.choice(PAGES),
        "ts": now_ms(),
    }


def make_metric() -> dict:
    metric = random.choice(METRIC_NAMES)
    if metric in ("cpu_usage", "mem_usage"):
        value = round(random.uniform(0, 100), 2)
    elif metric == "request_latency_ms":
        value = round(random.uniform(1, 2000), 2)
    else:
        value = round(random.uniform(0, 5000), 2)
    return {
        "host": random.choice(HOSTS),
        "metric": metric,
        "value": value,
        "ts": now_ms(),
    }


def new_session_id() -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=12))


# ============================================================
# MAIN
# ============================================================

def main() -> int:
    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)

    print("=" * 60, flush=True)
    print("Генератор синтетических данных", flush=True)
    print(f"  брокеры:         {BOOTSTRAP_SERVERS}", flush=True)
    print(f"  schema registry: {SCHEMA_REGISTRY_URL}", flush=True)
    print(f"  топики:          {TOPIC_ORDERS} / {TOPIC_EVENTS} / {TOPIC_METRICS}", flush=True)
    print(f"  интервал:        {INTERVAL_SEC} сек", flush=True)
    print(f"  за тик:          {ORDERS_PER_TICK} заказов, "
          f"{EVENTS_PER_TICK} событий, {METRICS_PER_TICK} метрик", flush=True)
    print("=" * 60, flush=True)

    if not wait_for_schema_registry(SCHEMA_REGISTRY_URL):
        print("Schema Registry недоступен, выходим", flush=True)
        return 1

    sr_client = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})

    # to_dict=lambda obj, ctx: obj — записи уже являются словарями
    serializers = {
        TOPIC_ORDERS: AvroSerializer(sr_client, SCHEMA_ORDER, lambda o, c: o),
        TOPIC_EVENTS: AvroSerializer(sr_client, SCHEMA_EVENT, lambda o, c: o),
        TOPIC_METRICS: AvroSerializer(sr_client, SCHEMA_METRIC, lambda o, c: o),
    }

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "synthetic-generator",
        "acks": "all",
        "retries": 5,
        "linger.ms": 50,
    })

    def send(topic: str, key: str, record: dict):
        payload = serializers[topic](
            record, SerializationContext(topic, MessageField.VALUE)
        )
        producer.produce(topic=topic, key=key, value=payload, on_delivery=delivery_report)

    tick = 0
    total = {TOPIC_ORDERS: 0, TOPIC_EVENTS: 0, TOPIC_METRICS: 0}

    while _running:
        tick += 1

        for _ in range(ORDERS_PER_TICK):
            order = make_order()
            send(TOPIC_ORDERS, order["order_id"], order)
            total[TOPIC_ORDERS] += 1

        session_id = new_session_id()
        for _ in range(EVENTS_PER_TICK):
            event = make_event(session_id)
            send(TOPIC_EVENTS, event["session_id"], event)
            total[TOPIC_EVENTS] += 1

        for _ in range(METRICS_PER_TICK):
            metric = make_metric()
            send(TOPIC_METRICS, metric["host"], metric)
            total[TOPIC_METRICS] += 1

        producer.poll(0)

        stamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
        print(
            f"[{stamp}] тик {tick}: "
            f"orders={total[TOPIC_ORDERS]} "
            f"events={total[TOPIC_EVENTS]} "
            f"metrics={total[TOPIC_METRICS]}",
            flush=True,
        )

        if MAX_TICKS and tick >= MAX_TICKS:
            print(f"Достигнут предел в {MAX_TICKS} тиков", flush=True)
            break

        # Сон дробим, чтобы быстро реагировать на сигнал остановки
        slept = 0.0
        while _running and slept < INTERVAL_SEC:
            time.sleep(min(0.2, INTERVAL_SEC - slept))
            slept += 0.2

    print("Дописываем буфер продюсера...", flush=True)
    producer.flush(timeout=30)
    print(f"Итого отправлено: {total}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
