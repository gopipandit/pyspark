from confluent_kafka import Consumer
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
    'group.id': 'your_group_id',  # Add a unique consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading from the earliest message
}
#Topic
topic = 'gopinath-de'

# Create a producer instance
consumer = Consumer(conf)

consumer.subscribe([topic])
print("listening to the msg.............")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer Error {msg.error()}")
            continue
        data = json.loads(msg.value().decode('utf-8'))
        print(f"I have received this message- {data}")
except Exception as e:
    print(e)

finally:
    consumer.close()