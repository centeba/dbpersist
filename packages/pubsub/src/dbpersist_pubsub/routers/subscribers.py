import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import require_api_key
from dbpersist_common.signing import new_secret
from dbpersist_pubsub.db import get_session
from dbpersist_pubsub.models import Delivery, Subscriber
from dbpersist_pubsub.schemas import ReplayOut, SubscriberCreated, SubscriberIn, SubscriberOut
from dbpersist_pubsub.tasks.deliver import deliver_webhook

router = APIRouter(
    prefix="/subscribers", tags=["subscribers"], dependencies=[Depends(require_api_key)]
)


async def get_subscriber(session: AsyncSession, subscriber_id: uuid.UUID) -> Subscriber:
    sub = await session.get(Subscriber, subscriber_id)
    if sub is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "subscriber not found")
    return sub


@router.post("", response_model=SubscriberCreated, status_code=status.HTTP_201_CREATED)
async def create_subscriber(
    body: SubscriberIn, session: AsyncSession = Depends(get_session)
) -> SubscriberCreated:
    secret = new_secret()
    sub = Subscriber(name=body.name, webhook_url=str(body.webhook_url), secret=secret)
    session.add(sub)
    try:
        await session.commit()
    except IntegrityError as exc:
        await session.rollback()
        raise HTTPException(
            status.HTTP_409_CONFLICT, f"subscriber {body.name!r} already exists"
        ) from exc
    return SubscriberCreated(id=sub.id, name=sub.name, webhook_url=sub.webhook_url, secret=secret)


@router.get("", response_model=list[SubscriberOut])
async def list_subscribers(session: AsyncSession = Depends(get_session)) -> list[Subscriber]:
    return list((await session.execute(select(Subscriber).order_by(Subscriber.name))).scalars())


@router.get("/{subscriber_id}", response_model=SubscriberOut)
async def read_subscriber(
    subscriber_id: uuid.UUID, session: AsyncSession = Depends(get_session)
) -> Subscriber:
    return await get_subscriber(session, subscriber_id)


@router.delete("/{subscriber_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscriber(
    subscriber_id: uuid.UUID, session: AsyncSession = Depends(get_session)
) -> None:
    sub = await get_subscriber(session, subscriber_id)
    await session.delete(sub)
    await session.commit()


@router.post("/{subscriber_id}/heartbeat", response_model=SubscriberOut)
async def heartbeat(
    subscriber_id: uuid.UUID, session: AsyncSession = Depends(get_session)
) -> Subscriber:
    sub = await get_subscriber(session, subscriber_id)
    sub.last_seen_at = datetime.now(UTC)
    await session.commit()
    return sub


@router.post("/{subscriber_id}/replay", response_model=ReplayOut)
async def replay(
    subscriber_id: uuid.UUID,
    since: datetime = Query(
        ..., description="re-send undelivered events created at or after this time"
    ),
    session: AsyncSession = Depends(get_session),
) -> ReplayOut:
    """Re-queue this subscriber's undelivered events created since a given time."""
    await get_subscriber(session, subscriber_id)
    stmt = select(Delivery).where(
        Delivery.subscriber_id == subscriber_id,
        Delivery.status != "delivered",
        Delivery.created_at >= since,
    )
    rows = list((await session.execute(stmt)).scalars())
    for d in rows:
        d.status = "pending"
    await session.commit()
    for d in rows:
        deliver_webhook.delay(d.id)  # pyright: ignore[reportFunctionMemberAccess]
    return ReplayOut(requeued=len(rows))
