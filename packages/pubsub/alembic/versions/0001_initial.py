"""initial pubsub schema

Revision ID: 0001
Revises:
Create Date: 2026-10-15
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "subscribers",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text(), nullable=False, unique=True),
        sa.Column("webhook_url", sa.Text(), nullable=False),
        sa.Column("secret", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "last_seen_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("last_delivery_ok_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_table(
        "subscriptions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "subscriber_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("type_name", sa.Text(), nullable=False),
        sa.Column("filter", postgresql.JSONB(), nullable=True),
        sa.Column("keys", postgresql.ARRAY(sa.Text()), nullable=True),
        sa.Column("by", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "(filter IS NOT NULL AND keys IS NULL) OR (filter IS NULL AND keys IS NOT NULL)",
            name="subscriptions_filter_xor_keys",
        ),
        sa.CheckConstraint("by IN ('key', 'id')", name="subscriptions_by_valid"),
    )
    op.create_index("subscriptions_type", "subscriptions", ["type_name"])
    op.create_index("subscriptions_subscriber", "subscriptions", ["subscriber_id"])
    op.create_table(
        "deliveries",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "subscriber_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("subscribers.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "subscription_ids", postgresql.ARRAY(postgresql.UUID(as_uuid=True)), nullable=False
        ),
        sa.Column("event", postgresql.JSONB(), nullable=False),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("deliveries_subscriber_created", "deliveries", ["subscriber_id", "created_at"])
    op.create_index("deliveries_created", "deliveries", ["created_at"])


def downgrade() -> None:
    op.drop_table("deliveries")
    op.drop_table("subscriptions")
    op.drop_table("subscribers")
