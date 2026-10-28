import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import Principal, get_principal, require_type
from dbpersist_common.filters import dump_filter, parse_filter
from dbpersist_pubsub.db import get_session
from dbpersist_pubsub.models import Subscription
from dbpersist_pubsub.routers.subscribers import get_subscriber
from dbpersist_pubsub.schemas import SubscriptionIn, SubscriptionOut

router = APIRouter(tags=["subscriptions"], dependencies=[Depends(get_principal)])


@router.post(
    "/subscribers/{subscriber_id}/subscriptions",
    response_model=SubscriptionOut,
    status_code=status.HTTP_201_CREATED,
)
async def subscribe(
    subscriber_id: uuid.UUID,
    body: SubscriptionIn,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> Subscription:
    # receiving a type's events is reading it
    require_type(principal, body.type, "read")
    await get_subscriber(session, subscriber_id, principal)
    filt = None
    if body.filter is not None:
        try:
            filt = dump_filter(parse_filter(body.filter))
        except ValueError as exc:
            raise HTTPException(422, f"invalid filter: {exc}") from exc
    sub = Subscription(
        subscriber_id=subscriber_id, type_name=body.type, filter=filt, keys=body.keys, by=body.by
    )
    session.add(sub)
    await session.commit()
    return sub


@router.get("/subscribers/{subscriber_id}/subscriptions", response_model=list[SubscriptionOut])
async def list_subscriptions(
    subscriber_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> list[Subscription]:
    await get_subscriber(session, subscriber_id, principal)
    stmt = (
        select(Subscription)
        .where(Subscription.subscriber_id == subscriber_id)
        .order_by(Subscription.created_at)
    )
    return list((await session.execute(stmt)).scalars())


@router.delete("/subscriptions/{subscription_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unsubscribe(
    subscription_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
    principal: Principal = Depends(get_principal),
) -> None:
    sub = await session.get(Subscription, subscription_id)
    if sub is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "subscription not found")
    await get_subscriber(session, sub.subscriber_id, principal)
    await session.delete(sub)
    await session.commit()
