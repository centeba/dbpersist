"""Ingest external event notifications through an inbound source.

For each mapped event: resolve the local type (optionally auto-registering it), build the
document and key, deduplicate on the sender's event id, then either upsert the object (mode
``upsert``: insert, merge/replace update, or soft delete, each producing the normal change
event) or emit an event without storing anything (mode ``publish``). All events of one request
are applied in one transaction; their events are staged in the outbox with it, like every write.
"""

import logging
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.events import ObjectEvent
from dbpersist_common.inbound import InboundMapping, MappedEvent, map_payload
from dbpersist_common.keys import build_key
from dbpersist_persistence.models import InboundSource, ObjectType
from dbpersist_persistence.schemas import IngestedEvent, IngestOut
from dbpersist_persistence.services import objects as svc
from dbpersist_persistence.services.errors import InvalidData, NotFound
from dbpersist_persistence.services.events import event_for

log = logging.getLogger(__name__)

_RECEIPT = text(
    """
    INSERT INTO inbound_receipts (source_name, external_id, type_name, key, op, event_id)
    VALUES (:source, :ext, :type, :key, :op, :event_id)
    ON CONFLICT (source_name, external_id) DO NOTHING
    RETURNING id
    """
)


async def _resolve_type(
    session: AsyncSession, source: InboundSource, ev: MappedEvent
) -> ObjectType:
    try:
        return await svc.get_type(session, ev.type_name)
    except NotFound:
        if not source.auto_register_types or not ev.key_values:
            raise
        otype = ObjectType(name=ev.type_name, key_fields=list(ev.key_values.keys()), json_schema={})
        session.add(otype)
        await session.flush()
        log.info(
            "inbound source auto-registered type",
            extra={"source": source.name, "type_name": ev.type_name},
        )
        return otype


async def _claim_receipt(
    session: AsyncSession, source: InboundSource, ev: MappedEvent, key: str
) -> bool:
    """Record the external id; False when it was already recorded (a redelivery)."""
    if ev.external_id is None:
        return True
    row = await session.execute(
        _RECEIPT,
        {
            "source": source.name,
            "ext": ev.external_id,
            "type": ev.type_name,
            "key": key,
            "op": ev.op,
            "event_id": None,
        },
    )
    return row.first() is not None


def _without(data: dict[str, Any], field: str | None) -> dict[str, Any]:
    return {k: v for k, v in data.items() if k != field} if field else data


async def _apply_one(
    session: AsyncSession, source: InboundSource, ev: MappedEvent, envelope_field: str | None
) -> tuple[IngestedEvent, ObjectEvent | None]:
    otype = await _resolve_type(session, source, ev)
    try:
        key = build_key(list(otype.key_fields), ev.data)
    except ValueError as exc:
        raise InvalidData(str(exc)) from exc

    if not await _claim_receipt(session, source, ev, key):
        return (
            IngestedEvent(
                index=ev.index, type_name=otype.name, key=key, op=ev.op, status="duplicate"
            ),
            None,
        )

    if source.mode == "publish":
        event = ObjectEvent(
            type_name=otype.name, key=key, op=ev.op, data=ev.data, changed_fields=ev.changed_fields
        )
        if ev.occurred_at is not None:
            event.occurred_at = ev.occurred_at
        return (
            IngestedEvent(
                index=ev.index,
                type_name=otype.name,
                key=key,
                op=ev.op,
                event_id=event.event_id,
                status="accepted",
            ),
            event,
        )

    # upsert mode
    try:
        existing = await svc.get_by_key(session, otype, key)
    except NotFound:
        existing = None

    if ev.op == "delete":
        if existing is None:
            return (
                IngestedEvent(
                    index=ev.index,
                    type_name=otype.name,
                    key=key,
                    op=ev.op,
                    status="unchanged",
                    reason="delete of an unknown key",
                ),
                None,
            )
        obj = await svc.delete(session, otype, existing.id, existing.version)
        event = event_for(obj, "delete")
    elif existing is None:
        obj = await svc.insert(session, otype, ev.data)
        event = event_for(obj, "insert")
    else:
        mode: svc.UpdateMode = "merge" if source.update_mode == "merge" else "replace"
        new_data = svc.deep_merge(existing.data, ev.data) if mode == "merge" else ev.data
        # the envelope is metadata about the delivery, not about the object: a message that
        # changes nothing else is a no-op, and the envelope never counts as a changed field
        if (
            svc.changed_fields(
                _without(existing.data, envelope_field), _without(new_data, envelope_field)
            )
            == []
        ):
            return (
                IngestedEvent(
                    index=ev.index,
                    type_name=otype.name,
                    key=key,
                    op=ev.op,
                    status="unchanged",
                    reason="no attribute changed",
                ),
                None,
            )
        obj, changed = await svc.update(
            session, otype, existing.id, existing.version, ev.data, mode
        )
        changed = [c for c in changed if c != envelope_field]
        event = event_for(obj, "update", changed or ev.changed_fields)
    return (
        IngestedEvent(
            index=ev.index,
            type_name=otype.name,
            key=key,
            op=event.op,
            event_id=event.event_id,
            status="accepted",
        ),
        event,
    )


async def ingest(
    session: AsyncSession, source: InboundSource, body: Any
) -> tuple[IngestOut, list[ObjectEvent]]:
    """Map and apply one request body. The caller stages the returned events and commits."""
    mapping = InboundMapping.model_validate(source.mapping)
    mapped, problems = map_payload(mapping, body)
    results: list[IngestedEvent] = []
    events: list[ObjectEvent] = []

    for p in problems:
        results.append(
            IngestedEvent(
                index=p.index, type_name="", key="", op="update", status="rejected", reason=p.reason
            )
        )

    for ev in mapped:
        savepoint = await session.begin_nested()
        try:
            result, event = await _apply_one(session, source, ev, mapping.envelope_field)
            await savepoint.commit()
        except (NotFound, InvalidData) as exc:
            await savepoint.rollback()
            result, event = (
                IngestedEvent(
                    index=ev.index,
                    type_name=ev.type_name,
                    key="",
                    op=ev.op,
                    status="rejected",
                    reason=str(exc),
                ),
                None,
            )
        results.append(result)
        if event is not None:
            events.append(event)

    results.sort(key=lambda r: r.index)
    accepted = sum(r.status == "accepted" for r in results)
    duplicates = sum(r.status == "duplicate" for r in results)
    rejected = sum(r.status == "rejected" for r in results)

    source.received_count += len(results)
    source.accepted_count += accepted
    source.duplicate_count += duplicates
    source.rejected_count += rejected
    source.last_received_at = datetime.now(UTC)

    out = IngestOut(
        source=source.name,
        received=len(results),
        accepted=accepted,
        duplicates=duplicates,
        rejected=rejected,
        events=results,
    )
    return out, events
