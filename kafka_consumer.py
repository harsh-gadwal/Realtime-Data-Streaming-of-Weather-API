from kafka import KafkaConsumer
import json

try:
    consumer = KafkaConsumer(
        'weather_topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )
    print("Waiting for messages...")
    for message in consumer:
        print(f"Received data: {message.value}")
except Exception as e:
    print(f"Error: {e}")
