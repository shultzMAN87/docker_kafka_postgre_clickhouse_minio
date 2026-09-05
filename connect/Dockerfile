FROM confluentinc/cp-kafka-connect:8.1.0

# Два sink-коннектора в одном воркере:
#   JDBC — для записи топика orders в PostgreSQL
#          (драйвер PostgreSQL входит в состав плагина)
#   S3   — для выгрузки топика events в MinIO
#
# Плагины ставятся ОТДЕЛЬНЫМИ слоями и с повторными попытками.
# Причина: архивы с Confluent Hub весят сотни мегабайт, и при обрыве
# загрузки confluent-hub падает с "md5 checksum verification failed".
# Раздельные слои позволяют не перекачивать уже установленный плагин
# при повторной сборке.

# Сначала JDBC — он заметно меньше и ставится быстрее
RUN set -eu; \
    for ATTEMPT in 1 2 3 4 5; do \
        echo "=== JDBC, попытка ${ATTEMPT} ==="; \
        if confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest; then \
            break; \
        fi; \
        echo "--- неудача, чистим остатки и ждём 15 секунд"; \
        rm -rf /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc; \
        sleep 15; \
    done; \
    test -d /usr/share/confluent-hub-components/confluentinc-kafka-connect-jdbc \
        || (echo "JDBC-плагин установить не удалось" && exit 1)

# Затем S3 — самый тяжёлый, именно он чаще всего и обрывается
RUN set -eu; \
    for ATTEMPT in 1 2 3 4 5; do \
        echo "=== S3, попытка ${ATTEMPT} ==="; \
        if confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest; then \
            break; \
        fi; \
        echo "--- неудача, чистим остатки и ждём 15 секунд"; \
        rm -rf /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3; \
        sleep 15; \
    done; \
    test -d /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3 \
        || (echo "S3-плагин установить не удалось" && exit 1)

# Проверяем, что оба плагина на месте
RUN ls -1 /usr/share/confluent-hub-components/

# ---------------------------------------------------------------------
# ЗАПАСНОЙ ВАРИАНТ: установка из заранее скачанных архивов.
#
# Если загрузка с Confluent Hub стабильно не проходит, скачайте ZIP
# вручную со страниц
#     https://www.confluent.io/hub/confluentinc/kafka-connect-s3
#     https://www.confluent.io/hub/confluentinc/kafka-connect-jdbc
# положите их в каталог connect/plugins/, закомментируйте два блока RUN
# выше и раскомментируйте строки ниже.
#
# COPY plugins/*.zip /tmp/plugins/
# RUN for ZIP in /tmp/plugins/*.zip; do \
#         confluent-hub install --no-prompt "${ZIP}"; \
#     done \
#  && rm -rf /tmp/plugins
# ---------------------------------------------------------------------
