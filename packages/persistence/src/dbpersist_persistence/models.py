import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Index, Integer, Text, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from dbpersist_persistence.db import Base


class ObjectType(Base):
    __tablename__ = "object_types"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    key_fields: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False)
    json_schema: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class Object(Base):
    __tablename__ = "objects"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type_name: Mapped[str] = mapped_column(Text, ForeignKey("object_types.name"), nullable=False)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    __table_args__ = (
        Index(
            "objects_type_key_live",
            "type_name",
            "key",
            unique=True,
            postgresql_where=text("deleted_at IS NULL"),
        ),
        Index(
            "objects_data_gin",
            "data",
            postgresql_using="gin",
            postgresql_ops={"data": "jsonb_path_ops"},
        ),
        Index("objects_type_updated", "type_name", "updated_at", "id"),
    )


class InboundSource(Base):
    """An external sender whose webhooks are accepted at POST /v1/inbound/{name}."""

    __tablename__ = "inbound_sources"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    mode: Mapped[str] = mapped_column(Text, nullable=False, default="upsert")  # upsert | publish
    update_mode: Mapped[str] = mapped_column(Text, nullable=False, default="merge")
    auto_register_types: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    processing: Mapped[str] = mapped_column(Text, nullable=False, default="queue")  # queue | sync
    auth: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    mapping: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    pull: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
    pull_cursor: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_pulled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_pull_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    received_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    duplicate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_received_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class InboundReceipt(Base):
    """One accepted external event, keyed by the sender's own id so redeliveries are no-ops."""

    __tablename__ = "inbound_receipts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_name: Mapped[str] = mapped_column(
        Text, ForeignKey("inbound_sources.name", ondelete="CASCADE"), nullable=False
    )
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    type_name: Mapped[str] = mapped_column(Text, nullable=False)
    key: Mapped[str] = mapped_column(Text, nullable=False)
    op: Mapped[str] = mapped_column(Text, nullable=False)
    event_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index("inbound_receipts_source_external", "source_name", "external_id", unique=True),
        Index("inbound_receipts_source_received", "source_name", "received_at"),
    )


class InboundMessage(Base):
    """A raw inbound request (push) or pulled feed page, queued durably until processed.

    Messages of one source are processed one at a time in id order. ``status`` is ``pending``
    (waiting, possibly for a retry at ``next_attempt_at``), ``done`` (processed; ``result`` holds
    the per-event outcome) or ``failed`` (gave up after ``attempts``; retry via the admin API).
    """

    __tablename__ = "inbound_messages"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_name: Mapped[str] = mapped_column(
        Text, ForeignKey("inbound_sources.name", ondelete="CASCADE"), nullable=False
    )
    origin: Mapped[str] = mapped_column(Text, nullable=False, default="push")  # push | pull
    body: Mapped[Any] = mapped_column(JSONB, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB(none_as_null=True), nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    __table_args__ = (
        Index("inbound_messages_source_status_id", "source_name", "status", "id"),
        Index("inbound_messages_received", "received_at"),
    )
