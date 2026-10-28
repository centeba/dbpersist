"""Publish an event without persisting an object.

The event goes through the outbox like every other event, so the 202 means "durably
accepted": a broker outage delays it instead of failing the call.
"""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import Principal, get_principal, require_type
from dbpersist_common.events import ObjectEvent
from dbpersist_common.keys import build_key
from dbpersist_persistence.db import get_session
from dbpersist_persistence.schemas import PublishIn, PublishOut
from dbpersist_persistence.services import objects as svc
from dbpersist_persistence.services import outbox
from dbpersist_persistence.services.errors import InvalidData
from dbpersist_persistence.settings import Settings

router = APIRouter(prefix="/publish", tags=["publish"], dependencies=[Depends(get_principal)])


@router.post("", response_model=PublishOut, status_code=202)
async def publish_only(
    body: PublishIn,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(outbox.get_settings),
    principal: Principal = Depends(get_principal),
) -> PublishOut:
    require_type(principal, body.type, "write")
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
    await outbox.stage(session, settings, [event])
    await session.commit()
    return PublishOut(event_id=event.event_id, key=key)
