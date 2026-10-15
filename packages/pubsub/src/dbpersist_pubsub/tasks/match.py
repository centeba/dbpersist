"""pubsub.publish_event: validate, match, record deliveries, fan out."""

import logging
from typing import Any

from dbpersist_common.events import PUBLISH_TASK_NAME, ObjectEvent
from dbpersist_pubsub.celery_app import celery_app
from dbpersist_pubsub.db import sync_sessionmaker
from dbpersist_pubsub.matching import match_event
from dbpersist_pubsub.models import Delivery
from dbpersist_pubsub.stats import get_stats
from dbpersist_pubsub.tasks.deliver import deliver_webhook

log = logging.getLogger(__name__)


def fan_out(raw_event: dict[str, Any]) -> list[int]:
    """Create pending Delivery rows for every matching subscriber; return their ids."""
    event = ObjectEvent.model_validate(raw_event)
    stats = get_stats()
    stats.incr("received")
    with sync_sessionmaker()() as session:
        targets = match_event(session, event)
        rows: list[Delivery] = []
        for subscriber_id, sub_ids in targets.items():
            rows.append(
                Delivery(
                    event_id=event.event_id,
                    subscriber_id=subscriber_id,
                    subscription_ids=sub_ids,
                    event=event.to_wire(),
                )
            )
        session.add_all(rows)
        session.commit()
        ids = [r.id for r in rows]
    if ids:
        stats.incr("matched", len(ids))
    log.info(
        "event matched %d subscriber(s)",
        len(ids),
        extra={
            "event_id": str(event.event_id),
            "type_name": event.type_name,
            "op": event.op,
            "object_key": event.key,
            "matched": len(ids),
        },
    )
    return ids


@celery_app.task(name=PUBLISH_TASK_NAME)
def publish_event(raw_event: dict[str, Any]) -> int:
    ids = fan_out(raw_event)
    for did in ids:
        deliver_webhook.delay(did)  # pyright: ignore[reportFunctionMemberAccess]
    return len(ids)
