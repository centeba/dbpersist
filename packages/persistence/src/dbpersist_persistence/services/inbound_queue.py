"""Durable inbound queue: enqueue, drain one source at a time in order, retry, and pull.

Why a queue: the external sender gets an answer as soon as its message is safely in Postgres,
so slow or failing processing never makes it time out and re-send; processing can be retried
without the sender's cooperation; and if dbpersist is down the sender sees a 503 and retries,
which is the one case the queue cannot absorb.

Concurrency: several API processes may run the drain loop. A source row is locked with
``FOR UPDATE SKIP LOCKED`` while its messages are processed, so each source has one processor
at a time and its messages stay in arrival order; different sources proceed in parallel.
"""

import asyncio
import logging
import random
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from dbpersist_common.extract import MISSING, evaluate
from dbpersist_common.inbound import InboundPull
from dbpersist_persistence.models import InboundMessage, InboundSource
from dbpersist_persistence.services.events import EventPublisher
from dbpersist_persistence.services.inbound import ingest
from dbpersist_persistence.settings import Settings

log = logging.getLogger(__name__)

ClientFactory = Callable[[], httpx.AsyncClient]


def default_client_factory() -> httpx.AsyncClient:
    return httpx.AsyncClient(follow_redirects=False)


def backoff(attempts: int, cap: int) -> timedelta:
    base = min(cap, 2**attempts)
    return timedelta(seconds=base * random.uniform(0.5, 1.5))


# ---- enqueue ---------------------------------------------------------------------------------


async def enqueue(
    session: AsyncSession, source: InboundSource, body: Any, origin: str
) -> InboundMessage:
    msg = InboundMessage(source_name=source.name, origin=origin, body=body)
    session.add(msg)
    await session.flush()
    return msg


# ---- processing ------------------------------------------------------------------------------


def _transient(exc: BaseException) -> bool:
    return isinstance(exc, (OperationalError, DBAPIError, OSError, asyncio.TimeoutError))


async def process_message(
    session: AsyncSession, source: InboundSource, msg: InboundMessage, settings: Settings
) -> list[Any]:
    """Process one message in the current transaction; returns events to publish after commit."""
    out, events = await ingest(session, source, msg.body)
    msg.status = "done"
    msg.result = out.model_dump(mode="json")
    msg.last_error = None
    msg.processed_at = datetime.now(UTC)
    return events


async def drain_source(
    maker: async_sessionmaker[AsyncSession],
    name: str,
    settings: Settings,
    publisher: EventPublisher,
    limit: int = 100,
) -> int:
    """Process up to ``limit`` due messages of one source, in order. Returns how many were handled.

    Each message gets its own transaction so a crash loses at most one in-flight message, which
    is then retried. The source row lock is taken in a separate short transaction per message
    so a long batch never holds other writers off the source row.
    """
    handled = 0
    for _ in range(limit):
        async with maker() as session:
            # lock the source for the duration of this one message
            src = (
                await session.execute(
                    select(InboundSource)
                    .where(InboundSource.name == name)
                    .with_for_update(skip_locked=True)
                )
            ).scalar_one_or_none()
            if src is None:
                return handled  # gone, or another processor holds it
            now = datetime.now(UTC)
            msg = (
                await session.execute(
                    select(InboundMessage)
                    .where(
                        InboundMessage.source_name == name,
                        InboundMessage.status == "pending",
                        InboundMessage.next_attempt_at <= now,
                    )
                    .order_by(InboundMessage.id)
                    .limit(1)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if msg is None:
                return handled
            msg.attempts += 1
            savepoint = await session.begin_nested()
            try:
                events = await process_message(session, src, msg, settings)
                await savepoint.commit()
            except Exception as exc:  # noqa: BLE001
                await savepoint.rollback()
                events = []
                if _transient(exc) and msg.attempts < settings.inbound_max_attempts:
                    msg.next_attempt_at = now + backoff(
                        msg.attempts, settings.inbound_backoff_max_seconds
                    )
                    msg.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                    log.warning(
                        "inbound message deferred",
                        extra={
                            "source": name,
                            "message_id": msg.id,
                            "attempt": msg.attempts,
                            "error": msg.last_error,
                        },
                    )
                else:
                    msg.status = "failed"
                    msg.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                    msg.processed_at = now
                    log.error(
                        "inbound message failed",
                        extra={
                            "source": name,
                            "message_id": msg.id,
                            "attempt": msg.attempts,
                            "error": msg.last_error,
                        },
                    )
            await session.commit()
            if events:
                publisher.publish(events)
            handled += 1
            if msg.status == "pending":
                return handled  # deferred: keep order, come back after the backoff
    return handled


async def due_sources(session: AsyncSession) -> list[str]:
    now = datetime.now(UTC)
    stmt = (
        select(InboundMessage.source_name)
        .where(InboundMessage.status == "pending", InboundMessage.next_attempt_at <= now)
        .group_by(InboundMessage.source_name)
    )
    return [r[0] for r in (await session.execute(stmt)).all()]


async def drain_once(
    maker: async_sessionmaker[AsyncSession], settings: Settings, publisher: EventPublisher
) -> int:
    """One pass over every source with due messages. Used by the loop and by tests."""
    async with maker() as session:
        names = await due_sources(session)
    total = 0
    for name in names:
        total += await drain_source(maker, name, settings, publisher)
    return total


# ---- pull ------------------------------------------------------------------------------------


async def pull_source(
    maker: async_sessionmaker[AsyncSession],
    name: str,
    client_factory: ClientFactory = default_client_factory,
    force: bool = False,
) -> bool:
    """Fetch one page from the source feed and enqueue it; True when a message was queued."""
    async with maker() as session:
        src = (
            await session.execute(
                select(InboundSource)
                .where(InboundSource.name == name)
                .with_for_update(skip_locked=True)
            )
        ).scalar_one_or_none()
        if src is None or not src.pull or not src.enabled:
            return False
        pull = InboundPull.model_validate(src.pull)
        if not pull.enabled and not force:
            return False
        now = datetime.now(UTC)
        if not force and src.last_pulled_at is not None:
            if now - src.last_pulled_at < timedelta(seconds=pull.interval_seconds):
                return False
        src.last_pulled_at = now

        cursor = src.pull_cursor if src.pull_cursor is not None else pull.initial_cursor
        params = dict(pull.params)
        body = dict(pull.body) if pull.body else None
        if cursor is not None and pull.cursor_param:
            if pull.method == "POST" and body is not None:
                body[pull.cursor_param] = cursor
            else:
                params[pull.cursor_param] = cursor
        try:
            async with client_factory() as http:
                resp = await http.request(
                    pull.method,
                    pull.url,
                    params=params,
                    headers=pull.headers,
                    json=body if pull.method == "POST" else None,
                    timeout=pull.timeout_seconds,
                )
                resp.raise_for_status()
                page = resp.json()
        except (httpx.HTTPError, ValueError) as exc:
            src.last_pull_error = f"{type(exc).__name__}: {exc}"[:2000]
            await session.commit()
            log.warning("inbound pull failed", extra={"source": name, "error": src.last_pull_error})
            return False

        src.last_pull_error = None
        if pull.cursor_path:
            nxt = evaluate(pull.cursor_path, page)
            if nxt is not MISSING and nxt is not None:
                src.pull_cursor = str(nxt)
        await enqueue(session, src, page, origin="pull")
        await session.commit()
        log.info("inbound pull queued a page", extra={"source": name, "cursor": src.pull_cursor})
        return True


async def pull_due(maker: async_sessionmaker[AsyncSession], client_factory: ClientFactory) -> int:
    async with maker() as session:
        names = [
            r[0]
            for r in (
                await session.execute(
                    select(InboundSource.name).where(
                        InboundSource.enabled.is_(True), InboundSource.pull.is_not(None)
                    )
                )
            ).all()
        ]
    n = 0
    for name in names:
        if await pull_source(maker, name, client_factory):
            n += 1
    return n


# ---- the loop --------------------------------------------------------------------------------


async def run_processor(
    maker: async_sessionmaker[AsyncSession],
    settings: Settings,
    publisher: EventPublisher,
    client_factory: ClientFactory = default_client_factory,
) -> None:
    """Background loop for the API process: drain due messages and run due pulls every tick."""
    log.info("inbound processor started", extra={"tick_seconds": settings.inbound_tick_seconds})
    while True:
        try:
            await pull_due(maker, client_factory)
            handled = await drain_once(maker, settings, publisher)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001  (database down, broker down: wait and retry)
            log.warning("inbound processor tick failed", extra={"error": repr(exc)})
            handled = 0
        await asyncio.sleep(0.05 if handled else settings.inbound_tick_seconds)


async def queue_counts(session: AsyncSession, name: str) -> dict[str, int]:
    stmt = (
        select(InboundMessage.status, func.count())
        .where(InboundMessage.source_name == name)
        .group_by(InboundMessage.status)
    )
    counts = {"pending": 0, "done": 0, "failed": 0}
    for status, n in (await session.execute(stmt)).all():
        counts[status] = int(n)
    return counts


async def retry_failed(session: AsyncSession, name: str, message_id: int | None = None) -> int:
    stmt = (
        update(InboundMessage)
        .where(InboundMessage.source_name == name, InboundMessage.status == "failed")
        .values(status="pending", attempts=0, next_attempt_at=datetime.now(UTC), last_error=None)
    )
    if message_id is not None:
        stmt = stmt.where(InboundMessage.id == message_id)
    res = await session.execute(stmt)
    return int(getattr(res, "rowcount", 0) or 0)
