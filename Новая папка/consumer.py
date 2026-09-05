from confluent_kafka import Consumer, KafkaError
import requests

bot_token = "5204346049:AAHdJVDpgd1YSgGcrgFl9p1OVLrxvOVY5jo"
chat_id = "-1002022499957"


def send_message(bot_token, chat_id, text):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        response = requests.post(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка отправки сообщения: {e}")
        return None 

conf = {
    #'bootstrap.servers': 'localhost:29092',
    'bootstrap.servers': 'kafka1:9092',
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}


consumer = Consumer(conf)
topic = 'topic1'
consumer.subscribe([topic])

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print('Конец партиции {0} [{1}] достигнут на смещении {2}'.format(msg.topic(), msg.partition(), msg.offset()))
            else:
                print('Ошибка: {}'.format(msg.error()))
            continue

        print('Получено сообщение: {0}'.format(msg.value().decode('utf-8')))
        message_text = f"Номенклатура: {msg.value().decode('utf-8')}"
        result = send_message(bot_token, chat_id, message_text) 

except KeyboardInterrupt:
    pass

finally:
    consumer.close()