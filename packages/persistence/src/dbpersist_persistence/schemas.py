import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from dbpersist_common.events import EventOp
from dbpersist_common.inbound import (
    InboundAuth,
    InboundMapping,
    InboundPull,
    MappedEvent,
    MappingProblem,
)


class ObjectTypeIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    # identifier segments joined by dots, so domains can namespace their types (orders.Order)
    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")
    key_fields: list[str] = Field(min_length=1)
    json_schema: dict[str, Any] = Field(default_factory=dict)


class ObjectTypeOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    key_fields: list[str]
    json_schema: dict[str, Any]
    created_at: datetime


class ObjectOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    type_name: str
    key: str
    version: int
    data: dict[str, Any]
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None = None


class UpdateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    version: int
    data: dict[str, Any]


class OrderBy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    desc: bool = False


class QueryIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filter: dict[str, Any] | None = None
    order_by: list[OrderBy] = Field(default_factory=list)
    limit: int = Field(default=100, ge=1, le=1000)
    offset: int = Field(default=0, ge=0)
    cursor: str | None = None
    expand: list[str] = Field(default_factory=list)


class QueryOut(BaseModel):
    items: list[ObjectOut]
    next_cursor: str | None = None


class CountIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    filter: dict[str, Any] | None = None


class CountOut(BaseModel):
    count: int


class InsertOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["insert"]
    type: str
    data: dict[str, Any]


class UpdateOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["update"]
    type: str
    id: uuid.UUID
    version: int
    data: dict[str, Any]
    mode: Literal["replace", "merge"] = "replace"


class DeleteOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["delete"]
    type: str
    id: uuid.UUID
    version: int


TransactionOp = Annotated[InsertOp | UpdateOp | DeleteOp, Field(discriminator="op")]


class TransactionIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ops: list[TransactionOp] = Field(min_length=1)


class OpResult(BaseModel):
    op: Literal["insert", "update", "delete"]
    object: ObjectOut


class TransactionOut(BaseModel):
    transaction_id: uuid.UUID
    results: list[OpResult]


class PublishIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    op: EventOp
    data: dict[str, Any]
    key: str | None = None
    changed_fields: list[str] = Field(default_factory=list)


class PublishOut(BaseModel):
    event_id: uuid.UUID
    key: str


# ---- inbound sources -------------------------------------------------------------------------


class InboundSourceIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[A-Za-z0-9_.-]+$", max_length=100)
    description: str = ""
    enabled: bool = True
    mode: Literal["upsert", "publish"] = "upsert"
    update_mode: Literal["merge", "replace"] = "merge"
    auto_register_types: bool = False
    processing: Literal["queue", "sync"] = "queue"
    auth: InboundAuth
    mapping: InboundMapping
    pull: InboundPull | None = None


class InboundSourceOut(BaseModel):
    """Source as shown to operators: the auth secret is masked."""

    name: str
    description: str
    enabled: bool
    mode: str
    update_mode: str
    auto_register_types: bool
    processing: str
    auth: dict[str, Any]
    mapping: InboundMapping
    pull: InboundPull | None
    pull_cursor: str | None
    last_pulled_at: datetime | None
    last_pull_error: str | None
    ingest_path: str
    queue: dict[str, int]
    received_count: int
    accepted_count: int
    duplicate_count: int
    rejected_count: int
    last_received_at: datetime | None
    created_at: datetime
    updated_at: datetime


class IngestedEvent(BaseModel):
    index: int
    type_name: str
    key: str
    op: EventOp
    event_id: uuid.UUID | None = None
    status: Literal["accepted", "duplicate", "unchanged", "rejected"]
    reason: str | None = None


class IngestOut(BaseModel):
    source: str
    received: int
    accepted: int
    duplicates: int
    rejected: int
    events: list[IngestedEvent]


class InboundTestIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    payload: Any
    headers: dict[str, str] = Field(default_factory=dict)
    body: str | None = Field(
        default=None,
        description="exact request body for signature checks; defaults to the JSON of payload",
    )


class InboundTestOut(BaseModel):
    auth_ok: bool | None
    auth_error: str | None
    events: list[MappedEvent]
    problems: list[MappingProblem]
    unknown_types: list[str]


class InboundReceiptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    external_id: str
    type_name: str
    key: str
    op: str
    event_id: uuid.UUID | None
    received_at: datetime


class EnqueuedOut(BaseModel):
    source: str
    message_id: int
    queued: bool = True


class InboundMessageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    origin: str
    status: str
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    result: dict[str, Any] | None
    received_at: datetime
    processed_at: datetime | None


class RetryOut(BaseModel):
    requeued: int


class PullOut(BaseModel):
    pulled: bool
    pull_cursor: str | None
    last_pull_error: str | None
