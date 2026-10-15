import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, model_validator


class SubscriberIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    webhook_url: HttpUrl


class SubscriberCreated(BaseModel):
    id: uuid.UUID
    name: str
    webhook_url: str
    secret: str


class SubscriberOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    webhook_url: str
    created_at: datetime
    last_seen_at: datetime
    last_delivery_ok_at: datetime | None = None


class SubscriptionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    filter: dict[str, Any] | None = None
    keys: list[str] | None = None
    by: Literal["key", "id"] = "key"

    @model_validator(mode="after")
    def _xor(self) -> SubscriptionIn:
        if (self.filter is None) == (self.keys is None):
            raise ValueError("exactly one of 'filter' or 'keys' must be given")
        if self.keys is not None and not self.keys:
            raise ValueError("'keys' must be non-empty")
        return self


class SubscriptionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    subscriber_id: uuid.UUID
    type_name: str
    filter: dict[str, Any] | None = None
    keys: list[str] | None = None
    by: str
    created_at: datetime


class PublishAccepted(BaseModel):
    event_id: uuid.UUID


class ReplayOut(BaseModel):
    requeued: int


class ClientInfo(BaseModel):
    id: uuid.UUID
    name: str
    webhook_url: str
    subscriptions: int
    pending: int
    failed: int
    last_seen_at: datetime
    last_delivery_ok_at: datetime | None


class StatsOut(BaseModel):
    received: int
    matched: int
    delivered: int
    failed: int
    subscribers: int
    subscriptions: int


class RemoveIdleOut(BaseModel):
    removed: list[str]


class LogLevelIn(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"]
