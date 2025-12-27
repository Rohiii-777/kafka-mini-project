import json
import os
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv
import time
from db import SessionLocal, User, init_db

load_dotenv()
init_db()
SUPPORTED_VERSIONS = {1}
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

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

def parse_user_created_event(event: dict) -> dict:
    """
    Returns normalized user data.
    Raises ValueError for unsupported schema.
    """

    # New versioned event
    if "event_version" in event:
        version = event["event_version"]

        if version not in SUPPORTED_VERSIONS:
            raise ValueError(f"Unsupported event version: {version}")

        return event["data"]

    # Legacy event (pre-versioning)
    return event

while True:
    msg = consumer.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        print(msg.error())
        continue

    session = SessionLocal()

    event = json.loads(msg.value().decode())

    for attempt in range(1, MAX_RETRIES + 1):
        session = SessionLocal()

        try:
            data = parse_user_created_event(event)

            if data["email"].endswith("@fail.com"):
                raise ValueError("Simulated processing failure")

            user = User(
                user_id=data["user_id"],
                email=data["email"],
                name=data["name"],
            )

            session.merge(user)
            session.commit()

            print(f"User persisted: {data['user_id']} (attempt {attempt})")
            break  # success → exit retry loop

        except Exception as e:
            session.rollback()
            print(f"Error on attempt {attempt}: {e}")

            if attempt == MAX_RETRIES:
                send_to_dlq(event, str(e))
                print("Sent to DLQ")
            else:
                time.sleep(RETRY_BACKOFF_SECONDS)

        finally:
            session.close()

