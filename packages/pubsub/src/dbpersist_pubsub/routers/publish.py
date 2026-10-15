"""HTTP ingress for events (external producers, tests); persistence normally enqueues directly."""

from fastapi import APIRouter, Depends, status

from dbpersist_common.auth import require_api_key
from dbpersist_common.events import ObjectEvent
from dbpersist_pubsub.schemas import PublishAccepted
from dbpersist_pubsub.tasks.match import publish_event

router = APIRouter(prefix="/publish", tags=["publish"], dependencies=[Depends(require_api_key)])


@router.post("", response_model=PublishAccepted, status_code=status.HTTP_202_ACCEPTED)
async def publish(event: ObjectEvent) -> PublishAccepted:
    publish_event.delay(event.to_wire())  # pyright: ignore[reportFunctionMemberAccess]
    return PublishAccepted(event_id=event.event_id)
