from confluent_kafka import Producer
import json
import time
from dotenv import load_dotenv
import os

load_dotenv("F:\Pyspark-2025\pyspark\MongoDB\.env")

bootstrap_server = os.getenv("bootstrap_server")
api_key = os.getenv("api_key")
api_secret = os.getenv("api_secret")

# Configuration
conf = {
    'bootstrap.servers': bootstrap_server, 
    'security.protocol': 'SASL_SSL',    
    'sasl.mechanisms': 'PLAIN',     
    'sasl.username': api_key,     
    'sasl.password': api_secret, 
}

# Create a producer instance
producer = Producer(conf)


topic = 'gopinath-de'

def delivery_callback(err, msg):
    if err:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

for i in range(1, 11):
    data = {"index": i, "message": f"Hello Kafka! From Gopi Nath- {i}"}
    producer.produce(topic, key=str(i), value=json.dumps(data),callback=delivery_callback)
producer.flush()
print("All messages sent to Kafka")

# producer.produce(topic, key='test_key', value='test_value', callback=delivery_callback)
producer.flush()
print("Test message sent to Kafka")