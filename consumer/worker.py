import json
import os
from confluent_kafka import Consumer
from dotenv import load_dotenv
load_dotenv()
consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "group.id": "user-service",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["user.created"])

users = {}

print("Consumer started...")

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    event = json.loads(msg.value().decode())

    users[event["user_id"]] = event
    print("User stored:", event)
