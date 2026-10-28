import uuid
from datetime import UTC, datetime

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import Principal, get_principal
from dbpersist_common.signing import new_secret
from dbpersist_pubsub.db import get_session
from dbpersist_pubsub.models import Delivery, Subscriber
from dbpersist_pubsub.schemas import ReplayOut, SubscriberCreated, SubscriberIn, SubscriberOut
from dbpersist_pubsub.tasks.deliver import deliver_webhook

router = APIRouter(
    prefix="/subscribers", tags=["subscribers"], dependencies=[Depends(get_principal)]
)


async def get_subscriber(
    session: AsyncSession, subscriber_id: uuid.UUID, principal: Principal | None = None
) -> Subscriber:
    """Load a subscriber; with a principal, also check it may act on it.

    A subscriber belongs to the key that created it (``owner``). Platform admins see every
    subscriber; rows registered before keys were scoped have no owner and stay open.
    """
    sub = await session.get(Subscriber, subscriber_id)
    if sub is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "subscriber not found")
    if principal is not None and not owns(principal, sub):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN, f"subscriber {sub.name!r} belongs to another key"
        )
    return sub


def owns(principal: Principal, sub: Subscriber) -> bool:
    return principal.is_admin or sub.owner is None or sub.owner == principal.name


@router.post("", response_model=SubscriberCreated, status_code=status.HTTP_201_CREATED)
async def create_subscriber(
    body: SubscriberIn,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> SubscriberCreated:
    secret = new_secret()
    sub = Subscriber(
        name=body.name, webhook_url=str(body.webhook_url), secret=secret, owner=principal.name
    )
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
async def list_subscribers(
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> list[Subscriber]:
    rows = (await session.execute(select(Subscriber).order_by(Subscriber.name))).scalars()
    return [s for s in rows if owns(principal, s)]


@router.get("/{subscriber_id}", response_model=SubscriberOut)
async def read_subscriber(
    subscriber_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> Subscriber:
    return await get_subscriber(session, subscriber_id, principal)


@router.delete("/{subscriber_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_subscriber(
    subscriber_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> None:
    sub = await get_subscriber(session, subscriber_id, principal)
    await session.delete(sub)
    await session.commit()


@router.post("/{subscriber_id}/heartbeat", response_model=SubscriberOut)
async def heartbeat(
    subscriber_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> Subscriber:
    sub = await get_subscriber(session, subscriber_id, principal)
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
    principal: Principal = Depends(get_principal),
) -> ReplayOut:
    """Re-queue this subscriber's undelivered events created since a given time."""
    await get_subscriber(session, subscriber_id, principal)
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
