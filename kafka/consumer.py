from confluent_kafka import Consumer, KafkaError
import json
import time
from dotenv import load_dotenv
import os
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load environment variables from the .env file
load_dotenv("F:\Pyspark-2025\pyspark\MongoDB\.env")

# Fetch environment variables
uri = os.getenv("MONGODB_URL")
bootstrap_server = os.getenv("bootstrap_server")
api_key = os.getenv("api_key")
api_secret = os.getenv("api_secret")

# Kafka consumer configuration
conf = {
    'bootstrap.servers': bootstrap_server, 
    'security.protocol': 'SASL_SSL',    
    'sasl.mechanisms': 'PLAIN',     
    'sasl.username': os.getenv('api_key2'),
    'sasl.password': os.getenv('api_secret2'),
    'group.id': 'ABS_Bank_Ltd',  # Add a unique consumer group ID
    'auto.offset.reset': 'earliest',  # Start reading from the earliest message
}

# MongoDB connection
client = MongoClient(uri, server_api=ServerApi('1'))

# Test MongoDB connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(f"MongoDB connection error: {e}")
    exit(1)  # Exit the script if MongoDB connection fails

# Access the database and collection
db = client['kafkaDB']
collection = db['bank_txn']

# Kafka topic
topic = 'txn_data'

# Create a consumer instance
consumer = Consumer(conf)

# Subscribe to the topic
consumer.subscribe([topic])
print("Listening to the messages.............")

try:
    while True:
        # Poll for messages (wait up to 1 second)
        msg = consumer.poll(timeout=1.0)
        
        if msg is None:
            continue  # No message received

        if msg.error():
            # Handle errors
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                print(f"End of partition reached {msg.partition()}")
            else:
                print(f"Consumer Error: {msg.error()}")
            continue

        try:
            # Decode the message value
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Data to insert: {data}")  # Debug print

            # Insert the document into MongoDB
            collection.insert_one(data)
            print(f"Inserted document: {data}")

        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
        except Exception as e:
            print(f"MongoDB insert error: {e}")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the consumer and MongoDB client gracefully
    consumer.close()
    client.close()
    print("Consumer and MongoDB client closed.")