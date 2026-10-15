"""pubsub.publish_event: validate, match, record deliveries, fan out."""

import logging
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert

from dbpersist_common.events import PUBLISH_TASK_NAME, ObjectEvent
from dbpersist_pubsub.celery_app import celery_app, settings
from dbpersist_pubsub.db import autocommit_sessionmaker
from dbpersist_pubsub.matching import match_event
from dbpersist_pubsub.models import Delivery
from dbpersist_pubsub.stats import get_stats
from dbpersist_pubsub.tasks.deliver import attempt_delivery, backoff_seconds, deliver_webhook

log = logging.getLogger(__name__)


def fan_out(raw_event: dict[str, Any]) -> list[int]:
    """Create pending Delivery rows for every matching subscriber; return the ids to deliver.

    Idempotent: (event_id, subscriber_id) is unique, so a redelivered publish_event message
    (acks_late, worker died mid-task) finds its rows already there and returns the ones that
    are still undelivered instead of creating duplicates.
    """
    event = ObjectEvent.model_validate(raw_event)
    # Autocommit: one SELECT for the subscriptions, one multi-row INSERT ... RETURNING.
    with autocommit_sessionmaker()() as session:
        targets = match_event(session, event)
        ids: list[int] = []
        if targets:
            wire = event.to_wire()
            stmt = (
                insert(Delivery)
                .values(
                    [
                        {
                            "event_id": event.event_id,
                            "subscriber_id": subscriber_id,
                            "subscription_ids": sub_ids,
                            "event": wire,
                            "status": "pending",
                            "attempts": 0,
                        }
                        for subscriber_id, sub_ids in targets.items()
                    ]
                )
                .on_conflict_do_nothing(index_elements=["event_id", "subscriber_id"])
                .returning(Delivery.id)
            )
            ids = list(session.execute(stmt).scalars())
            if len(ids) < len(targets):  # redelivery: pick up the rows a previous run left
                existing = select(Delivery.id).where(
                    Delivery.event_id == event.event_id, Delivery.status != "delivered"
                )
                ids = list(session.execute(existing).scalars())
    get_stats().incr_many({"received": 1, "matched": len(ids)} if ids else {"received": 1})
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
    if len(ids) == 1 and settings.deliver_inline:
        # Single subscriber: make the first attempt here and skip the second Celery hop. On
        # failure the delivery is handed to deliver_webhook with `retries=1`, so it runs the
        # same backoff schedule and attempt count as if it had owned the first attempt.
        try:
            attempt_delivery(ids[0])
        except httpx.HTTPError:
            deliver_webhook.apply_async(  # pyright: ignore[reportFunctionMemberAccess]
                (ids[0],),
                countdown=backoff_seconds(0, settings.webhook_backoff_max_seconds),
                retries=1,
            )
        return 1
    for did in ids:
        deliver_webhook.delay(did)  # pyright: ignore[reportFunctionMemberAccess]
    return len(ids)
