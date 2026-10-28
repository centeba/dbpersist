"""Inbound sources: the public ingress for external webhooks and its management API.

``POST /v1/inbound/{source}`` is called by the external system and is authenticated by the
source's own scheme, not by the dbpersist API key. By default it only stores the message
(202 Accepted); the background processor applies it. A source with ``processing: sync``
applies the message before answering (200 with the per-event outcome). Everything under
``/v1/inbound-sources`` is operator-facing and needs a platform-admin API key.
"""

import json
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import admin_only
from dbpersist_common.inbound import (
    InboundAuth,
    InboundMapping,
    InboundPull,
    map_payload,
    verify_request,
)
from dbpersist_persistence.db import get_session
from dbpersist_persistence.models import InboundMessage, InboundReceipt, InboundSource, ObjectType
from dbpersist_persistence.schemas import (
    EnqueuedOut,
    InboundMessageOut,
    InboundReceiptOut,
    InboundSourceIn,
    InboundSourceOut,
    InboundTestIn,
    InboundTestOut,
    IngestOut,
    PullOut,
    RetryOut,
)
from dbpersist_persistence.services import inbound_queue as queue
from dbpersist_persistence.services import objects as svc
from dbpersist_persistence.services import outbox
from dbpersist_persistence.services.errors import NotFound
from dbpersist_persistence.services.inbound import ingest
from dbpersist_persistence.settings import Settings

log = logging.getLogger(__name__)

ingress = APIRouter(prefix="/inbound", tags=["inbound"])
manage = APIRouter(prefix="/inbound-sources", tags=["inbound"], dependencies=[Depends(admin_only)])


def _mask(auth: dict[str, Any]) -> dict[str, Any]:
    out = dict(auth)
    if out.get("secret"):
        out["secret"] = "***"
    return out


async def _out(session: AsyncSession, s: InboundSource) -> InboundSourceOut:
    return InboundSourceOut(
        name=s.name,
        description=s.description,
        enabled=s.enabled,
        mode=s.mode,
        update_mode=s.update_mode,
        auto_register_types=s.auto_register_types,
        processing=s.processing,
        auth=_mask(s.auth),
        mapping=InboundMapping.model_validate(s.mapping),
        pull=InboundPull.model_validate(s.pull) if s.pull else None,
        pull_cursor=s.pull_cursor,
        last_pulled_at=s.last_pulled_at,
        last_pull_error=s.last_pull_error,
        ingest_path=f"/v1/inbound/{s.name}",
        queue=await queue.queue_counts(session, s.name),
        received_count=s.received_count,
        accepted_count=s.accepted_count,
        duplicate_count=s.duplicate_count,
        rejected_count=s.rejected_count,
        last_received_at=s.last_received_at,
        created_at=s.created_at,
        updated_at=s.updated_at,
    )


async def _load(session: AsyncSession, name: str) -> InboundSource:
    s = await session.get(InboundSource, name)
    if s is None:
        raise NotFound(f"inbound source {name!r}")
    return s


def _apply(s: InboundSource, body: InboundSourceIn) -> None:
    s.description = body.description
    s.enabled = body.enabled
    s.mode = body.mode
    s.update_mode = body.update_mode
    s.auto_register_types = body.auto_register_types
    s.processing = body.processing
    s.auth = body.auth.model_dump()
    s.mapping = body.mapping.model_dump(exclude_none=True)
    s.pull = body.pull.model_dump() if body.pull else None


# ---- public ingress ---------------------------------------------------------------------------


@ingress.post("/{name}", response_model=IngestOut | EnqueuedOut)
async def receive(
    name: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(outbox.get_settings),
) -> IngestOut | EnqueuedOut | JSONResponse:
    try:
        source = await session.get(InboundSource, name)
    except (OperationalError, DBAPIError) as exc:
        # our database is unreachable: tell the sender to retry later
        log.error(
            "inbound ingress: database unavailable", extra={"source": name, "error": repr(exc)}
        )
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "store unavailable, retry"
        ) from exc
    if source is None or not source.enabled:
        # the same answer for unknown and disabled sources: do not leak which names exist
        raise HTTPException(status.HTTP_404_NOT_FOUND, "no such inbound source")

    body = await request.body()
    auth = InboundAuth.model_validate(source.auth)
    reason = verify_request(auth, request.headers, body)
    if reason is not None:
        log.warning("inbound auth failed", extra={"source": name, "reason": reason})
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, reason)

    try:
        payload = json.loads(body or b"null")
    except ValueError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, f"body is not JSON: {exc}") from exc

    if source.processing == "sync":

        async def attempt() -> IngestOut:
            out, events = await ingest(session, source, payload)
            await outbox.stage(session, settings, events)
            await session.commit()
            return out

        out = await svc.with_retry(attempt)
        log.info(
            "inbound request processed",
            extra={
                "source": name,
                "received": out.received,
                "accepted": out.accepted,
                "duplicates": out.duplicates,
                "rejected": out.rejected,
            },
        )
        return out

    try:
        msg = await queue.enqueue(session, source, payload, origin="push")
        await session.commit()
    except (OperationalError, DBAPIError) as exc:
        log.error(
            "inbound ingress: could not store message", extra={"source": name, "error": repr(exc)}
        )
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "store unavailable, retry"
        ) from exc
    log.info("inbound request queued", extra={"source": name, "message_id": msg.id})
    return JSONResponse(
        EnqueuedOut(source=name, message_id=msg.id).model_dump(),
        status_code=status.HTTP_202_ACCEPTED,
    )


# ---- management -------------------------------------------------------------------------------


@manage.post("", response_model=InboundSourceOut, status_code=status.HTTP_201_CREATED)
async def create_source(
    body: InboundSourceIn, session: AsyncSession = Depends(get_session)
) -> InboundSourceOut:
    if await session.get(InboundSource, body.name) is not None:
        raise HTTPException(status.HTTP_409_CONFLICT, f"inbound source {body.name!r} exists")
    s = InboundSource(name=body.name, auth={}, mapping={})
    _apply(s, body)
    session.add(s)
    await session.commit()
    await session.refresh(s)
    return await _out(session, s)


@manage.get("", response_model=list[InboundSourceOut])
async def list_sources(session: AsyncSession = Depends(get_session)) -> list[InboundSourceOut]:
    rows = list(
        (await session.execute(select(InboundSource).order_by(InboundSource.name))).scalars()
    )
    return [await _out(session, s) for s in rows]


@manage.get("/{name}", response_model=InboundSourceOut)
async def read_source(name: str, session: AsyncSession = Depends(get_session)) -> InboundSourceOut:
    return await _out(session, await _load(session, name))


@manage.put("/{name}", response_model=InboundSourceOut)
async def replace_source(
    name: str, body: InboundSourceIn, session: AsyncSession = Depends(get_session)
) -> InboundSourceOut:
    if body.name != name:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "name in body must match path")
    s = await _load(session, name)
    _apply(s, body)
    await session.commit()
    await session.refresh(s)
    return await _out(session, s)


@manage.delete("/{name}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_source(name: str, session: AsyncSession = Depends(get_session)) -> None:
    s = await _load(session, name)
    await session.delete(s)
    await session.commit()


@manage.post("/{name}/test", response_model=InboundTestOut)
async def test_source(
    name: str, body: InboundTestIn, session: AsyncSession = Depends(get_session)
) -> InboundTestOut:
    """Dry run: show how a sample payload would be mapped, and whether sample headers would pass."""
    s = await _load(session, name)
    mapping = InboundMapping.model_validate(s.mapping)
    events, problems = map_payload(mapping, body.payload)
    auth_ok: bool | None = None
    auth_error: str | None = None
    if body.headers:
        raw = (body.body if body.body is not None else json.dumps(body.payload)).encode()
        auth_error = verify_request(InboundAuth.model_validate(s.auth), body.headers, raw)
        auth_ok = auth_error is None
    known = {r[0] for r in (await session.execute(select(ObjectType.name))).all()}
    unknown = sorted({e.type_name for e in events if e.type_name not in known})
    return InboundTestOut(
        auth_ok=auth_ok,
        auth_error=auth_error,
        events=events,
        problems=problems,
        unknown_types=unknown,
    )


@manage.get("/{name}/receipts", response_model=list[InboundReceiptOut])
async def receipts(
    name: str,
    limit: int = Query(default=50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[InboundReceipt]:
    await _load(session, name)
    stmt = (
        select(InboundReceipt)
        .where(InboundReceipt.source_name == name)
        .order_by(InboundReceipt.received_at.desc(), InboundReceipt.id.desc())
        .limit(limit)
    )
    return list((await session.execute(stmt)).scalars())


@manage.get("/{name}/messages", response_model=list[InboundMessageOut])
async def messages(
    name: str,
    status_: str | None = Query(default=None, alias="status", pattern="^(pending|done|failed)$"),
    limit: int = Query(default=50, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
) -> list[InboundMessage]:
    """The durable queue, newest first. Filter with ?status=pending|done|failed."""
    await _load(session, name)
    stmt = select(InboundMessage).where(InboundMessage.source_name == name)
    if status_:
        stmt = stmt.where(InboundMessage.status == status_)
    stmt = stmt.order_by(InboundMessage.id.desc()).limit(limit)
    return list((await session.execute(stmt)).scalars())


@manage.post("/{name}/messages/retry", response_model=RetryOut)
async def retry_messages(
    name: str,
    message_id: int | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> RetryOut:
    """Put failed messages (one, or all of the source) back on the queue."""
    await _load(session, name)
    n = await queue.retry_failed(session, name, message_id)
    await session.commit()
    return RetryOut(requeued=n)


@manage.post("/{name}/pull", response_model=PullOut)
async def pull_now(
    name: str, request: Request, session: AsyncSession = Depends(get_session)
) -> PullOut:
    """Fetch one page from the source's feed right now, ignoring the interval."""
    s = await _load(session, name)
    if not s.pull:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, "source has no pull configuration"
        )
    factory = getattr(request.app.state, "inbound_client_factory", queue.default_client_factory)
    pulled = await queue.pull_source(request.app.state.sessionmaker, name, factory, force=True)
    await session.refresh(s)
    return PullOut(pulled=pulled, pull_cursor=s.pull_cursor, last_pull_error=s.last_pull_error)
