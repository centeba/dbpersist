"""HTTP ingress for events (external producers, tests); persistence normally enqueues directly."""

from fastapi import APIRouter, Depends, status

from dbpersist_common.auth import Principal, get_principal, require_type
from dbpersist_common.events import ObjectEvent
from dbpersist_pubsub.schemas import PublishAccepted
from dbpersist_pubsub.tasks.match import publish_event

router = APIRouter(prefix="/publish", tags=["publish"], dependencies=[Depends(get_principal)])


@router.post("", response_model=PublishAccepted, status_code=status.HTTP_202_ACCEPTED)
async def publish(
    event: ObjectEvent, principal: Principal = Depends(get_principal)
) -> PublishAccepted:
    require_type(principal, event.type_name, "write")
    publish_event.delay(event.to_wire())  # pyright: ignore[reportFunctionMemberAccess]
    return PublishAccepted(event_id=event.event_id)
