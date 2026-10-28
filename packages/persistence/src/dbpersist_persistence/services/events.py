"""Build ObjectEvents from changes and hand them to the pubsub broker.

Persistence never imports pubsub code: it sends the task by name to the shared
Redis broker (``PUBLISH_TASK_NAME``), which pubsub workers consume. Write paths never call
a publisher directly: they stage events in the outbox (``services/outbox.py``) inside their
transaction, and the relay hands committed rows to the publisher.
"""

import logging
import uuid
from typing import Protocol

from celery import Celery

from dbpersist_common.events import EVENTS_QUEUE, PUBLISH_TASK_NAME, EventOp, ObjectEvent
from dbpersist_common.telemetry import get_meter
from dbpersist_persistence.models import Object

log = logging.getLogger(__name__)

_events_counter = get_meter("dbpersist.persistence").create_counter(
    "dbpersist.persistence.events", unit="1", description="Change events emitted, by type and op"
)


def _record(ev: ObjectEvent) -> None:
    _events_counter.add(1, {"type_name": ev.type_name, "op": ev.op})
    log.info(
        "event %s %s/%s v%d",
        ev.op,
        ev.type_name,
        ev.key,
        ev.version,
        extra={
            "event_id": str(ev.event_id),
            "type_name": ev.type_name,
            "op": ev.op,
            "object_key": ev.key,
            "object_version": ev.version,
            "changed_fields": ",".join(ev.changed_fields),
            "transaction_id": str(ev.transaction_id) if ev.transaction_id else None,
        },
    )


class EventPublisher(Protocol):
    def publish(self, events: list[ObjectEvent]) -> None: ...


class CeleryEventPublisher:
    def __init__(self, app: Celery) -> None:
        self._app = app

    def publish(self, events: list[ObjectEvent]) -> None:
        for ev in events:
            self._app.send_task(PUBLISH_TASK_NAME, args=[ev.to_wire()], queue=EVENTS_QUEUE)
            _record(ev)


class NullPublisher:
    """Installed when PUBSUB_ENABLED is false. No event is staged then and the relay does not
    run, so it is never called; it exists so ``app.state.publisher`` satisfies the protocol."""

    def __init__(self) -> None:
        self.dropped = 0

    def publish(self, events: list[ObjectEvent]) -> None:
        self.dropped += len(events)
        for ev in events:
            _record(ev)


class RecordingPublisher:
    """Test double: keeps every event in memory."""

    def __init__(self) -> None:
        self.events: list[ObjectEvent] = []

    def publish(self, events: list[ObjectEvent]) -> None:
        self.events.extend(events)
        for ev in events:
            _record(ev)


def event_for(
    obj: Object,
    op: EventOp,
    changed: list[str] | None = None,
    transaction_id: uuid.UUID | None = None,
) -> ObjectEvent:
    return ObjectEvent(
        type_name=obj.type_name,
        object_id=obj.id,
        key=obj.key,
        op=op,
        version=obj.version,
        data=obj.data,
        changed_fields=changed or [],
        transaction_id=transaction_id,
    )
