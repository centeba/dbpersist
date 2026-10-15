"""Atomic multi-object commit."""

import uuid

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import require_api_key
from dbpersist_common.events import ObjectEvent
from dbpersist_persistence.db import get_session
from dbpersist_persistence.schemas import (
    DeleteOp,
    InsertOp,
    ObjectOut,
    OpResult,
    TransactionIn,
    TransactionOut,
    UpdateOp,
)
from dbpersist_persistence.services import objects as svc
from dbpersist_persistence.services.events import EventPublisher, event_for, get_publisher

router = APIRouter(
    prefix="/transactions", tags=["transactions"], dependencies=[Depends(require_api_key)]
)


@router.post("", response_model=TransactionOut)
async def commit_transaction(
    body: TransactionIn,
    session: AsyncSession = Depends(get_session),
    publisher: EventPublisher = Depends(get_publisher),
) -> TransactionOut:
    tx_id = uuid.uuid4()

    async def attempt() -> tuple[list[OpResult], list[ObjectEvent]]:
        results: list[OpResult] = []
        events: list[ObjectEvent] = []
        for op in body.ops:
            otype = await svc.get_type(session, op.type)
            if isinstance(op, InsertOp):
                obj = await svc.insert(session, otype, op.data)
                events.append(event_for(obj, "insert", transaction_id=tx_id))
            elif isinstance(op, UpdateOp):
                obj, changed = await svc.update(session, otype, op.id, op.version, op.data, op.mode)
                events.append(event_for(obj, "update", changed, transaction_id=tx_id))
            else:
                assert isinstance(op, DeleteOp)
                obj = await svc.delete(session, otype, op.id, op.version)
                events.append(event_for(obj, "delete", transaction_id=tx_id))
            results.append(OpResult(op=op.op, object=ObjectOut.model_validate(obj)))
        await session.commit()
        return results, events

    results, events = await svc.with_retry(attempt)
    publisher.publish(events)
    return TransactionOut(transaction_id=tx_id, results=results)
