"""Transactional outbox: change events are stored with the change and relayed afterwards.

Every write path calls :func:`stage` inside the transaction that makes the change, so the event
row commits or rolls back with the change itself. The relay loop (:func:`run_relay`, started
from the API lifespan behind ``OUTBOX_RELAY_ENABLED``) hands unsent rows to the
:class:`EventPublisher` in id order and stamps ``sent_at``. A broker outage therefore never
fails a write and never loses an event: rows simply wait until the next successful tick.

Ordering: a transaction-scoped advisory lock keeps one relay active at a time across all API
processes, and rows are sent in id order. Ids are assigned when the row is flushed, which for one
object happens under that object's row lock, so events of one object leave in version order.
``FOR UPDATE SKIP LOCKED`` is kept on the row select so a relay never waits on rows another
transaction still holds.

Delivery to the broker is at-least-once: a crash between ``send_task`` and the commit that marks
the row sent re-sends that row. Pub/sub fan-out is idempotent per ``event_id``, so a resend
creates no duplicate deliveries.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime, timedelta

from fastapi import Request
from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dbpersist_common.events import ObjectEvent
from dbpersist_persistence.models import OutboxEvent
from dbpersist_persistence.services.events import EventPublisher
from dbpersist_persistence.settings import Settings

log = logging.getLogger(__name__)

# pg_advisory_xact_lock key held by the relay for the duration of one batch
RELAY_LOCK_KEY = 744_201_001
RELAY_BACKOFF_MAX_SECONDS = 30.0
PRUNE_INTERVAL_SECONDS = 60.0


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


async def stage(session: AsyncSession, settings: Settings, events: list[ObjectEvent]) -> None:
    """Add outbox rows for ``events`` to the current transaction (nothing when pub/sub is off).

    Call it after the change itself, before the commit: the rows then share the change's fate,
    and their ids follow the order in which the changes took their row locks.
    """
    if not settings.pubsub_enabled or not events:
        return
    session.add_all(OutboxEvent(event=ev.to_wire()) for ev in events)
    await session.flush()


async def relay_once(
    maker: async_sessionmaker[AsyncSession], publisher: EventPublisher, batch_size: int
) -> int:
    """Send up to ``batch_size`` unsent rows in id order; returns how many were sent.

    Rows sent before a publisher failure are committed as sent; the failing row gets its
    ``attempts`` and ``last_error`` recorded, the rows after it are left untouched, and the
    exception is re-raised so the loop backs off. Returns 0 without touching anything when
    another relay holds the lock.
    """
    async with maker() as session:
        lock = await session.execute(select(func.pg_try_advisory_xact_lock(RELAY_LOCK_KEY)))
        if not lock.scalar_one():
            return 0
        result = await session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.sent_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
        rows = result.scalars().all()
        sent = 0
        for row in rows:
            event = ObjectEvent.model_validate(row.event)
            row.attempts += 1
            try:
                await asyncio.to_thread(publisher.publish, [event])
            except Exception as exc:
                row.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                await session.commit()
                log.warning(
                    "outbox relay failed",
                    extra={
                        "outbox_id": row.id,
                        "event_id": str(event.event_id),
                        "attempt": row.attempts,
                        "error": row.last_error,
                    },
                )
                raise
            row.sent_at = datetime.now(UTC)
            row.last_error = None
            sent += 1
        await session.commit()
        return sent


async def prune_sent(maker: async_sessionmaker[AsyncSession], retention_hours: int) -> int:
    """Delete rows sent more than ``retention_hours`` ago; returns how many were removed."""
    cutoff = datetime.now(UTC) - timedelta(hours=retention_hours)
    async with maker() as session:
        res = await session.execute(delete(OutboxEvent).where(OutboxEvent.sent_at < cutoff))
        await session.commit()
    return int(getattr(res, "rowcount", 0) or 0)


async def run_relay(
    maker: async_sessionmaker[AsyncSession], settings: Settings, publisher: EventPublisher
) -> None:
    """Background loop for the API process: relay unsent rows every tick, prune sent ones."""
    log.info(
        "outbox relay started",
        extra={"tick_seconds": settings.outbox_tick_seconds, "batch": settings.outbox_batch_size},
    )
    failures = 0
    last_prune = 0.0
    while True:
        try:
            sent = await relay_once(maker, publisher, settings.outbox_batch_size)
            failures = 0
            if time.monotonic() - last_prune >= PRUNE_INTERVAL_SECONDS:
                last_prune = time.monotonic()
                pruned = await prune_sent(maker, settings.outbox_retention_hours)
                if pruned:
                    log.info("outbox pruned", extra={"rows": pruned})
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001  (database or broker down: wait and retry)
            failures += 1
            delay = min(RELAY_BACKOFF_MAX_SECONDS, settings.outbox_tick_seconds * 2**failures)
            log.warning(
                "outbox relay tick failed", extra={"error": repr(exc), "retry_in_seconds": delay}
            )
            await asyncio.sleep(delay)
            continue
        await asyncio.sleep(0.05 if sent else settings.outbox_tick_seconds)
