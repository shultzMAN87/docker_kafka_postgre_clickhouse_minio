# Базовый образ Python
FROM python:3.10-slim

# Установка рабочей диреектории
WORKDIR /app

# Установим системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

# Установим pip и необходимые библиотеки
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Копирование файла в рабочую директорию
# . - текущая папка 
# . - рабочая директория в контейнере
COPY . .

CMD ["python", "consumer.py"]