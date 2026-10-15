"""inbound sources, durable inbound message queue, receipts

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-15
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "inbound_sources",
        sa.Column("name", sa.Text(), primary_key=True),
        sa.Column("description", sa.Text(), nullable=False, server_default=""),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("mode", sa.Text(), nullable=False, server_default="upsert"),
        sa.Column("update_mode", sa.Text(), nullable=False, server_default="merge"),
        sa.Column("auto_register_types", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("processing", sa.Text(), nullable=False, server_default="queue"),
        sa.Column("auth", postgresql.JSONB(), nullable=False),
        sa.Column("mapping", postgresql.JSONB(), nullable=False),
        sa.Column("pull", postgresql.JSONB(), nullable=True),
        sa.Column("pull_cursor", sa.Text(), nullable=True),
        sa.Column("last_pulled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_pull_error", sa.Text(), nullable=True),
        sa.Column("received_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("accepted_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duplicate_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("rejected_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_table(
        "inbound_messages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "source_name",
            sa.Text(),
            sa.ForeignKey("inbound_sources.name", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("origin", sa.Text(), nullable=False, server_default="push"),
        sa.Column("body", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False, server_default="pending"),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("result", postgresql.JSONB(), nullable=True),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "inbound_messages_source_status_id", "inbound_messages", ["source_name", "status", "id"]
    )
    op.create_index("inbound_messages_received", "inbound_messages", ["received_at"])
    op.create_table(
        "inbound_receipts",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column(
            "source_name",
            sa.Text(),
            sa.ForeignKey("inbound_sources.name", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("external_id", sa.Text(), nullable=False),
        sa.Column("type_name", sa.Text(), nullable=False),
        sa.Column("key", sa.Text(), nullable=False),
        sa.Column("op", sa.Text(), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "received_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index(
        "inbound_receipts_source_external",
        "inbound_receipts",
        ["source_name", "external_id"],
        unique=True,
    )
    op.create_index(
        "inbound_receipts_source_received", "inbound_receipts", ["source_name", "received_at"]
    )


def downgrade() -> None:
    op.drop_table("inbound_receipts")
    op.drop_table("inbound_messages")
    op.drop_table("inbound_sources")
