"""Publish an event without persisting anything."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import require_api_key
from dbpersist_common.events import ObjectEvent
from dbpersist_common.keys import build_key
from dbpersist_persistence.db import get_session
from dbpersist_persistence.schemas import PublishIn, PublishOut
from dbpersist_persistence.services import objects as svc
from dbpersist_persistence.services.errors import InvalidData
from dbpersist_persistence.services.events import EventPublisher, get_publisher

router = APIRouter(prefix="/publish", tags=["publish"], dependencies=[Depends(require_api_key)])


@router.post("", response_model=PublishOut, status_code=202)
async def publish_only(
    body: PublishIn,
    session: AsyncSession = Depends(get_session),
    publisher: EventPublisher = Depends(get_publisher),
) -> PublishOut:
    otype = await svc.get_type(session, body.type)
    if body.key is not None:
        key = body.key
    else:
        try:
            key = build_key(list(otype.key_fields), body.data)
        except ValueError as exc:
            raise InvalidData(str(exc)) from exc
    event = ObjectEvent(
        type_name=otype.name,
        key=key,
        op=body.op,
        data=body.data,
        changed_fields=body.changed_fields,
    )
    publisher.publish([event])
    return PublishOut(event_id=event.event_id, key=key)
