import uuid
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from dbpersist_common.events import EventOp


class ObjectTypeIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")
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
