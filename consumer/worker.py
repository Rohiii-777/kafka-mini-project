import json
import os
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

from db import SessionLocal, User, init_db

load_dotenv()
init_db()

consumer = Consumer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
    "group.id": "user-service-db",
    "auto.offset.reset": "earliest",

    # IMPORTANT
    "enable.auto.commit": True,
    "session.timeout.ms": 10000,
    "max.poll.interval.ms": 300000,  # 5 minutes
})

producer = Producer({
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS"),
})

consumer.subscribe(["user.created"])

print("DB-backed consumer started...")


def send_to_dlq(event, reason):
    producer.produce(
        topic="user.created.dlq",
        value=json.dumps({
            "event": event,
            "error": reason
        }).encode(),
    )
    producer.flush()


while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    session = SessionLocal()

    try:
        event = json.loads(msg.value().decode())

        # backward compatibility
        if "data" in event:
            data = event["data"]
        else:
            data = event

        # simulate failure
        if data["email"].endswith("@fail.com"):
            raise ValueError("Simulated processing failure")

        user = User(
            user_id=data["user_id"],
            email=data["email"],
            name=data["name"],
        )

        session.merge(user)
        session.commit()

        print("User persisted:", data["user_id"])


    except Exception as e:
        session.rollback()
        print("Error:", e)
        send_to_dlq(event, str(e))

    finally:
        session.close()
