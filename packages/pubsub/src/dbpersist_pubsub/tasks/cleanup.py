"""Housekeeping: idle subscriber removal and delivery log pruning."""

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, or_, select
from sqlalchemy.orm import Session

from dbpersist_pubsub.celery_app import celery_app, settings
from dbpersist_pubsub.db import sync_sessionmaker
from dbpersist_pubsub.models import Delivery, Subscriber

log = logging.getLogger(__name__)


def remove_idle_subscribers(
    session: Session, idle_minutes: int, now: datetime | None = None
) -> list[str]:
    cutoff = (now or datetime.now(UTC)) - timedelta(minutes=idle_minutes)
    stmt = select(Subscriber).where(
        Subscriber.last_seen_at < cutoff,
        or_(Subscriber.last_delivery_ok_at.is_(None), Subscriber.last_delivery_ok_at < cutoff),
    )
    idle = list(session.execute(stmt).scalars())
    names = [s.name for s in idle]
    for s in idle:
        session.delete(s)
    session.commit()
    if names:
        log.info("removed idle subscribers: %s", names)
    return names


def prune_old_deliveries(session: Session, retention_days: int, now: datetime | None = None) -> int:
    cutoff = (now or datetime.now(UTC)) - timedelta(days=retention_days)
    result = session.execute(delete(Delivery).where(Delivery.created_at < cutoff))
    session.commit()
    return int(getattr(result, "rowcount", 0) or 0)


@celery_app.task(name="pubsub.cleanup_idle_subscribers")
def cleanup_idle_subscribers() -> list[str]:
    with sync_sessionmaker()() as session:
        return remove_idle_subscribers(session, settings.idle_subscriber_minutes)


@celery_app.task(name="pubsub.prune_deliveries")
def prune_deliveries() -> int:
    with sync_sessionmaker()() as session:
        return prune_old_deliveries(session, settings.delivery_retention_days)
