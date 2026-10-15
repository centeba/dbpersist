import uuid
from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import BaseModel, ConfigDict, Field

from dbpersist_common.events import ObjectEvent


class ObjectRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: uuid.UUID | None = None
    type_name: str
    key: str
    version: int
    data: dict[str, Any]
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_event(cls, ev: ObjectEvent) -> ObjectRecord:
        return cls(
            id=ev.object_id, type_name=ev.type_name, key=ev.key, version=ev.version, data=ev.data
        )


class ObjectTypeInfo(BaseModel):
    name: str
    key_fields: list[str]
    json_schema: dict[str, Any]


class OpResult(BaseModel):
    op: Literal["insert", "update", "delete"]
    object: ObjectRecord


class TransactionResult(BaseModel):
    transaction_id: uuid.UUID
    results: list[OpResult]


class SubscriberInfo(BaseModel):
    id: uuid.UUID
    name: str
    webhook_url: str
    secret: str


class SubscriptionHandle(BaseModel):
    id: uuid.UUID
    type_name: str
    filter: dict[str, Any] | None = None
    keys: list[str] | None = None
    by: Literal["key", "id"] = "key"


class WebhookPayload(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event: ObjectEvent
    subscription_ids: list[uuid.UUID] = Field(default_factory=list)
    delivery_id: int | None = None


class Callbacks(Protocol):
    """What a subscriber implements to receive change notifications."""

    async def on_insert(self, record: ObjectRecord) -> None: ...
    async def on_update(self, record: ObjectRecord, changed_fields: list[str]) -> None: ...
    async def on_delete(self, record: ObjectRecord) -> None: ...
    async def on_error(self, error: Exception) -> None: ...


class BaseCallbacks:
    """No-op implementation to subclass when only some callbacks matter."""

    async def on_insert(self, record: ObjectRecord) -> None:
        return None

    async def on_update(self, record: ObjectRecord, changed_fields: list[str]) -> None:
        return None

    async def on_delete(self, record: ObjectRecord) -> None:
        return None

    async def on_error(self, error: Exception) -> None:
        return None
