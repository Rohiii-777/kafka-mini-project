import json
from confluent_kafka import Producer
import os
from dotenv import load_dotenv

load_dotenv()
producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
})

def send_event(topic: str, key: str, payload: dict):
    producer.produce(
        topic=topic,
        key=key,
        value=json.dumps(payload).encode(),
    )
    producer.flush()
