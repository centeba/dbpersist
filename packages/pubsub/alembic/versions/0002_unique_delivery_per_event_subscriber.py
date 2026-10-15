"""unique delivery per (event, subscriber)

Revision ID: 0002
Revises: 0001
"""

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Collapse duplicates left by redelivered publish_event messages before adding the index:
    # keep the row that was delivered, else the oldest one.
    op.execute(
        """
        DELETE FROM deliveries d
         USING deliveries k
         WHERE d.event_id = k.event_id AND d.subscriber_id = k.subscriber_id AND d.id <> k.id
           AND (k.status = 'delivered' AND d.status <> 'delivered'
                OR (k.status = 'delivered') = (d.status = 'delivered') AND k.id < d.id)
        """
    )
    op.create_index(
        "deliveries_event_subscriber", "deliveries", ["event_id", "subscriber_id"], unique=True
    )


def downgrade() -> None:
    op.drop_index("deliveries_event_subscriber", table_name="deliveries")
