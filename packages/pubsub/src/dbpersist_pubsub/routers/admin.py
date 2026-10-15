"""Operational admin endpoints: clients, criteria, stats, idle cleanup, log level."""

import logging
import uuid

from fastapi import APIRouter, Depends, Query, Request
from sqlalchemy import case, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import require_api_key
from dbpersist_pubsub.db import get_session
from dbpersist_pubsub.models import Delivery, Subscriber, Subscription
from dbpersist_pubsub.routers.subscribers import get_subscriber
from dbpersist_pubsub.schemas import (
    ClientInfo,
    LogLevelIn,
    RemoveIdleOut,
    StatsOut,
    SubscriptionOut,
)
from dbpersist_pubsub.stats import COUNTERS, get_stats
from dbpersist_pubsub.tasks.cleanup import remove_idle_subscribers

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(require_api_key)])


@router.get("/clients", response_model=list[ClientInfo])
async def clients(session: AsyncSession = Depends(get_session)) -> list[ClientInfo]:
    sub_counts = (
        select(Subscription.subscriber_id, func.count().label("n"))
        .group_by(Subscription.subscriber_id)
        .subquery()
    )
    del_counts = (
        select(
            Delivery.subscriber_id,
            func.sum(case((Delivery.status == "pending", 1), else_=0)).label("pending"),
            func.sum(case((Delivery.status == "failed", 1), else_=0)).label("failed"),
        )
        .group_by(Delivery.subscriber_id)
        .subquery()
    )
    stmt = (
        select(Subscriber, sub_counts.c.n, del_counts.c.pending, del_counts.c.failed)
        .outerjoin(sub_counts, sub_counts.c.subscriber_id == Subscriber.id)
        .outerjoin(del_counts, del_counts.c.subscriber_id == Subscriber.id)
        .order_by(Subscriber.name)
    )
    out: list[ClientInfo] = []
    for s, n, pending, failed in (await session.execute(stmt)).all():
        out.append(
            ClientInfo(
                id=s.id,
                name=s.name,
                webhook_url=s.webhook_url,
                subscriptions=int(n or 0),
                pending=int(pending or 0),
                failed=int(failed or 0),
                last_seen_at=s.last_seen_at,
                last_delivery_ok_at=s.last_delivery_ok_at,
            )
        )
    return out


@router.get("/clients/{subscriber_id}/criteria", response_model=list[SubscriptionOut])
async def criteria_by_client(
    subscriber_id: uuid.UUID, session: AsyncSession = Depends(get_session)
) -> list[Subscription]:
    await get_subscriber(session, subscriber_id)
    stmt = select(Subscription).where(Subscription.subscriber_id == subscriber_id)
    return list((await session.execute(stmt)).scalars())


@router.get("/stats", response_model=StatsOut)
async def stats(session: AsyncSession = Depends(get_session)) -> StatsOut:
    st = get_stats()
    n_subs = int((await session.execute(select(func.count()).select_from(Subscriber))).scalar_one())
    n_sub = int(
        (await session.execute(select(func.count()).select_from(Subscription))).scalar_one()
    )
    counts = {c: st.get(c) for c in COUNTERS}
    return StatsOut(subscribers=n_subs, subscriptions=n_sub, **counts)


@router.post("/remove-idle", response_model=RemoveIdleOut)
async def remove_idle(
    request: Request,
    idle_minutes: int | None = Query(default=None, ge=1),
    session: AsyncSession = Depends(get_session),
) -> RemoveIdleOut:
    minutes = idle_minutes or request.app.state.settings.idle_subscriber_minutes
    # the cleanup helper is sync (shared with the Celery task); run it on the session's sync side
    removed = await session.run_sync(remove_idle_subscribers, minutes)
    return RemoveIdleOut(removed=removed)


@router.put("/log-level")
async def set_log_level(body: LogLevelIn) -> dict[str, str]:
    logging.getLogger("dbpersist_pubsub").setLevel(body.level)
    return {"level": body.level}
