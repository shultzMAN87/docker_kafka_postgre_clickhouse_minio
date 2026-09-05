#!/bin/bash
# Создание рабочих топиков стенда.
# Запускается один раз сервисом kafka-init и завершается.
set -e

BOOTSTRAP="kafka-0:9091,kafka-1:9091,kafka-2:9091"

create_topic () {
  local NAME=$1
  local PARTITIONS=$2
  echo "--> топик '${NAME}' (partitions=${PARTITIONS}, replication=3)"
  kafka-topics --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${NAME}" \
    --partitions "${PARTITIONS}" \
    --replication-factor 3 \
    --config min.insync.replicas=2
}

echo "=== Ожидание готовности кластера ==="
until kafka-broker-api-versions --bootstrap-server "${BOOTSTRAP}" >/dev/null 2>&1; do
  echo "    кластер ещё не отвечает, ждём..."
  sleep 3
done

echo "=== Создание топиков ==="
create_topic orders  3
create_topic events  6
create_topic metrics 6

echo "=== Текущий список топиков ==="
kafka-topics --bootstrap-server "${BOOTSTRAP}" --list

echo "=== Готово ==="
