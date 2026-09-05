from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:29092'})

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"Record successfully produced to {msg.topic()} [{msg.partition()}]")

producer.produce('topic1', key='key', value='Hello Kafka!', callback=delivery_report)
producer.flush()
