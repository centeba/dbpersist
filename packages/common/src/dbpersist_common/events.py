"""The change event that persistence emits and pubsub fans out."""

import uuid
from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

EventOp = Literal["insert", "update", "delete"]

PUBLISH_TASK_NAME = "pubsub.publish_event"
EVENTS_QUEUE = "events"
DELIVERIES_QUEUE = "deliveries"


def utcnow() -> datetime:
    return datetime.now(UTC)


class ObjectEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: uuid.UUID = Field(default_factory=uuid.uuid4)
    occurred_at: datetime = Field(default_factory=utcnow)
    type_name: str
    object_id: uuid.UUID | None = None
    key: str
    op: EventOp
    version: int = 0
    data: dict[str, Any]
    changed_fields: list[str] = Field(default_factory=list)
    transaction_id: uuid.UUID | None = None

    def to_wire(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
