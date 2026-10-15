"""Object-level operations: key derivation, schema validation, versioned writes, expansion."""

import uuid
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any, Literal

import jsonschema
from sqlalchemy import select
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from dbpersist_common.keys import build_key
from dbpersist_persistence.models import Object, ObjectType
from dbpersist_persistence.services.errors import (
    DuplicateKey,
    InvalidData,
    NotFound,
    VersionConflict,
)

UpdateMode = Literal["replace", "merge"]

_RETRYABLE_SQLSTATES = {"40001", "40P01"}  # serialization failure, deadlock detected
MAX_ATTEMPTS = 3


async def get_type(session: AsyncSession, name: str) -> ObjectType:
    otype = await session.get(ObjectType, name)
    if otype is None:
        raise NotFound(f"object type {name!r}")
    return otype


def validate_data(otype: ObjectType, data: dict[str, Any]) -> None:
    if otype.json_schema:
        try:
            jsonschema.validate(data, otype.json_schema)
        except jsonschema.ValidationError as exc:
            raise InvalidData(exc.message) from exc
    try:
        build_key(list(otype.key_fields), data)
    except ValueError as exc:
        raise InvalidData(str(exc)) from exc


def deep_merge(base: dict[str, Any], patch: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in patch.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def changed_fields(old: dict[str, Any], new: dict[str, Any]) -> list[str]:
    """Top-level attribute names whose value differs between old and new data."""
    names = sorted(set(old) | set(new))
    return [n for n in names if old.get(n, _MISSING) != new.get(n, _MISSING)]


_MISSING = object()


async def get_object(session: AsyncSession, otype: ObjectType, obj_id: uuid.UUID) -> Object:
    obj = await session.get(Object, obj_id)
    if obj is None or obj.type_name != otype.name or obj.deleted_at is not None:
        raise NotFound(f"{otype.name} {obj_id}")
    return obj


async def get_by_key(session: AsyncSession, otype: ObjectType, key: str) -> Object:
    stmt = select(Object).where(
        Object.type_name == otype.name, Object.key == key, Object.deleted_at.is_(None)
    )
    obj = (await session.execute(stmt)).scalar_one_or_none()
    if obj is None:
        raise NotFound(f"{otype.name} key={key!r}")
    return obj


async def insert(session: AsyncSession, otype: ObjectType, data: dict[str, Any]) -> Object:
    validate_data(otype, data)
    key = build_key(list(otype.key_fields), data)
    type_name = otype.name  # captured: attributes expire on rollback
    obj = Object(type_name=type_name, key=key, data=data, version=1)
    session.add(obj)
    try:
        await session.flush()
    except IntegrityError as exc:
        await session.rollback()
        raise DuplicateKey(type_name, key) from exc
    return obj


async def _locked(session: AsyncSession, otype: ObjectType, obj_id: uuid.UUID) -> Object:
    stmt = (
        select(Object)
        .where(Object.id == obj_id, Object.type_name == otype.name, Object.deleted_at.is_(None))
        .with_for_update()
    )
    obj = (await session.execute(stmt)).scalar_one_or_none()
    if obj is None:
        raise NotFound(f"{otype.name} {obj_id}")
    return obj


async def update(
    session: AsyncSession,
    otype: ObjectType,
    obj_id: uuid.UUID,
    version: int,
    data: dict[str, Any],
    mode: UpdateMode,
) -> tuple[Object, list[str]]:
    obj = await _locked(session, otype, obj_id)
    if obj.version != version:
        raise VersionConflict(version, obj.version)
    new_data = deep_merge(obj.data, data) if mode == "merge" else data
    validate_data(otype, new_data)
    new_key = build_key(list(otype.key_fields), new_data)
    if new_key != obj.key:
        raise InvalidData("key fields cannot be changed by an update")
    changed = changed_fields(obj.data, new_data)
    obj.data = new_data
    obj.version = version + 1
    obj.updated_at = datetime.now(UTC)
    await session.flush()
    return obj, changed


async def delete(
    session: AsyncSession, otype: ObjectType, obj_id: uuid.UUID, version: int
) -> Object:
    obj = await _locked(session, otype, obj_id)
    if obj.version != version:
        raise VersionConflict(version, obj.version)
    obj.deleted_at = datetime.now(UTC)
    obj.version = version + 1
    await session.flush()
    return obj


async def expand(
    session: AsyncSession, otype: ObjectType, objs: list[Object], fields: list[str]
) -> list[dict[str, Any]]:
    """Return data dicts with referenced objects inlined under ``_expanded``.

    A field is expandable when the type's JSON schema declares
    ``properties.<field>.x-ref = "<OtherType>"``; its value is an object id or a list of ids.
    """
    props: dict[str, Any] = otype.json_schema.get("properties", {}) if otype.json_schema else {}
    ref_types: dict[str, str] = {}
    for f in fields:
        ref = props.get(f, {}).get("x-ref")
        if not ref:
            raise InvalidData(f"field {f!r} is not an x-ref field of {otype.name}")
        ref_types[f] = ref

    wanted: set[uuid.UUID] = set()
    for o in objs:
        for f in ref_types:
            v = o.data.get(f)
            for raw in v if isinstance(v, list) else [v]:
                if raw:
                    try:
                        wanted.add(uuid.UUID(str(raw)))
                    except ValueError:
                        continue
    found: dict[uuid.UUID, Object] = {}
    if wanted:
        stmt = select(Object).where(Object.id.in_(wanted), Object.deleted_at.is_(None))
        for ref_obj in (await session.execute(stmt)).scalars():
            found[ref_obj.id] = ref_obj

    def shape(ref_obj: Object) -> dict[str, Any]:
        return {
            "id": str(ref_obj.id),
            "type_name": ref_obj.type_name,
            "key": ref_obj.key,
            "version": ref_obj.version,
            "data": ref_obj.data,
        }

    out: list[dict[str, Any]] = []
    for o in objs:
        expanded: dict[str, Any] = {}
        for f in ref_types:
            v = o.data.get(f)
            if isinstance(v, list):
                ids = [_as_uuid(x) for x in v]
                expanded[f] = [shape(found[i]) for i in ids if i is not None and i in found]
            else:
                i = _as_uuid(v)
                expanded[f] = shape(found[i]) if i is not None and i in found else None
        out.append({**o.data, "_expanded": expanded})
    return out


def _as_uuid(v: Any) -> uuid.UUID | None:
    try:
        return uuid.UUID(str(v)) if v else None
    except ValueError:
        return None


def is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, DBAPIError):
        orig = exc.orig
        code = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
        return code in _RETRYABLE_SQLSTATES
    return False


async def with_retry[T](attempt: Callable[[], Coroutine[Any, Any, T]]) -> T:
    """Run ``attempt`` up to MAX_ATTEMPTS times on serialization/deadlock failures."""
    last: BaseException | None = None
    for _ in range(MAX_ATTEMPTS):
        try:
            return await attempt()
        except DBAPIError as exc:
            if not is_retryable(exc):
                raise
            last = exc
    assert last is not None
    raise last
