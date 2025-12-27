from fastapi import FastAPI
from pydantic import BaseModel
from uuid import uuid4

from app.kafka import send_event

app = FastAPI()


class UserCreate(BaseModel):
    email: str
    name: str


@app.post("/users")
def create_user(payload: UserCreate):
    user_id = str(uuid4())

    event = {
        "user_id": user_id,
        "email": payload.email,
        "name": payload.name,
    }

    send_event(
        topic="user.created",
        key=user_id,
        payload=event,
    )

    return {
        "status": "published",
        "user_id": user_id,
    }
