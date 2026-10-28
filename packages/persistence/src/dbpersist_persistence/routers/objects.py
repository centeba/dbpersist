import uuid
from typing import Any

from fastapi import APIRouter, Depends, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.auth import require_api_key
from dbpersist_common.keys import key_from_values
from dbpersist_persistence.db import get_session
from dbpersist_persistence.models import Object
from dbpersist_persistence.schemas import CountIn, CountOut, ObjectOut, QueryIn, QueryOut, UpdateIn
from dbpersist_persistence.services import objects as svc
from dbpersist_persistence.services import outbox
from dbpersist_persistence.services import query as qsvc
from dbpersist_persistence.services.events import event_for
from dbpersist_persistence.settings import Settings

router = APIRouter(prefix="/objects", tags=["objects"], dependencies=[Depends(require_api_key)])


def _out(obj: Object, data: dict[str, Any] | None = None) -> ObjectOut:
    out = ObjectOut.model_validate(obj)
    if data is not None:
        out.data = data
    return out


async def _expanded(
    session: AsyncSession, otype: Any, objs: list[Object], expand: list[str]
) -> list[ObjectOut]:
    if not expand:
        return [_out(o) for o in objs]
    datas = await svc.expand(session, otype, objs, expand)
    return [_out(o, d) for o, d in zip(objs, datas, strict=True)]


@router.post("/{type_name}", response_model=ObjectOut, status_code=status.HTTP_201_CREATED)
async def insert_object(
    type_name: str,
    data: dict[str, Any],
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(outbox.get_settings),
) -> ObjectOut:
    otype = await svc.get_type(session, type_name)
    obj = await svc.insert(session, otype, data)
    await outbox.stage(session, settings, [event_for(obj, "insert")])
    await session.commit()
    return _out(obj)


@router.get("/{type_name}/by-key/{key_path:path}", response_model=ObjectOut)
async def read_by_key(
    type_name: str,
    key_path: str,
    expand: str | None = Query(default=None, description="comma-separated x-ref fields"),
    session: AsyncSession = Depends(get_session),
) -> ObjectOut:
    otype = await svc.get_type(session, type_name)
    key = key_from_values(key_path.split("/"))
    obj = await svc.get_by_key(session, otype, key)
    return (await _expanded(session, otype, [obj], _split(expand)))[0]


@router.get("/{type_name}/{obj_id}", response_model=ObjectOut)
async def read_object(
    type_name: str,
    obj_id: uuid.UUID,
    expand: str | None = Query(default=None),
    session: AsyncSession = Depends(get_session),
) -> ObjectOut:
    otype = await svc.get_type(session, type_name)
    obj = await svc.get_object(session, otype, obj_id)
    return (await _expanded(session, otype, [obj], _split(expand)))[0]


@router.put("/{type_name}/{obj_id}", response_model=ObjectOut)
async def replace_object(
    type_name: str,
    obj_id: uuid.UUID,
    body: UpdateIn,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(outbox.get_settings),
) -> ObjectOut:
    return await _update(type_name, obj_id, body, "replace", session, settings)


@router.patch("/{type_name}/{obj_id}", response_model=ObjectOut)
async def patch_object(
    type_name: str,
    obj_id: uuid.UUID,
    body: UpdateIn,
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(outbox.get_settings),
) -> ObjectOut:
    return await _update(type_name, obj_id, body, "merge", session, settings)


async def _update(
    type_name: str,
    obj_id: uuid.UUID,
    body: UpdateIn,
    mode: svc.UpdateMode,
    session: AsyncSession,
    settings: Settings,
) -> ObjectOut:
    async def attempt() -> Object:
        otype = await svc.get_type(session, type_name)
        obj, changed = await svc.update(session, otype, obj_id, body.version, body.data, mode)
        await outbox.stage(session, settings, [event_for(obj, "update", changed)])
        await session.commit()
        return obj

    obj = await svc.with_retry(attempt)
    return _out(obj)


@router.delete("/{type_name}/{obj_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_object(
    type_name: str,
    obj_id: uuid.UUID,
    version: int = Query(...),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(outbox.get_settings),
) -> None:
    async def attempt() -> None:
        otype = await svc.get_type(session, type_name)
        obj = await svc.delete(session, otype, obj_id, version)
        await outbox.stage(session, settings, [event_for(obj, "delete")])
        await session.commit()

    await svc.with_retry(attempt)


@router.post("/{type_name}/query", response_model=QueryOut)
async def query_objects(
    type_name: str, body: QueryIn, session: AsyncSession = Depends(get_session)
) -> QueryOut:
    otype = await svc.get_type(session, type_name)
    rows, next_cursor = await qsvc.run_query(session, otype, body)
    return QueryOut(
        items=await _expanded(session, otype, rows, body.expand), next_cursor=next_cursor
    )


@router.post("/{type_name}/count", response_model=CountOut)
async def count_objects(
    type_name: str, body: CountIn, session: AsyncSession = Depends(get_session)
) -> CountOut:
    otype = await svc.get_type(session, type_name)
    return CountOut(count=await qsvc.run_count(session, otype, body.filter))


def _split(expand: str | None) -> list[str]:
    return [e for e in (expand or "").split(",") if e]
