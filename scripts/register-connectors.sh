#!/bin/sh
# Регистрация коннекторов из ./connectors/*.json
# Запускается один раз сервисом register-connectors и завершается.
#
# Логика простая: если коннектор с таким именем уже есть — удаляем и
# создаём заново. Для учебного стенда это надёжнее, чем частичное
# обновление конфигурации (и не требует jq в контейнере).
set -e

CONNECT_URL="http://kafka-connect:8083"

echo "=== Ожидание Kafka Connect ==="
until curl -sf "${CONNECT_URL}/connectors" >/dev/null; do
  echo "    Connect ещё не отвечает, ждём..."
  sleep 5
done
echo "    Connect доступен"

for FILE in /connectors/*.json; do
  NAME=$(basename "${FILE}" .json)
  echo ""
  echo "--> ${NAME}"

  if curl -sf "${CONNECT_URL}/connectors/${NAME}" >/dev/null 2>&1; then
    echo "    коннектор уже существует — пересоздаём"
    curl -s -X DELETE "${CONNECT_URL}/connectors/${NAME}"
    sleep 3
  fi

  HTTP_CODE=$(curl -s -o /tmp/resp.json -w "%{http_code}" \
    -X POST "${CONNECT_URL}/connectors" \
    -H "Content-Type: application/json" \
    --data-binary @"${FILE}")

  echo "    HTTP ${HTTP_CODE}"
  if [ "${HTTP_CODE}" != "201" ] && [ "${HTTP_CODE}" != "200" ]; then
    echo "    ОШИБКА:"
    cat /tmp/resp.json
    echo ""
  fi
done

echo ""
echo "=== Ждём 10 секунд и смотрим статусы ==="
sleep 10
curl -s "${CONNECT_URL}/connectors?expand=status"
echo ""
echo "=== Готово ==="
