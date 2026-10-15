"""Criteria queries: filter DSL -> SELECT, ordering, offset or keyset cursor paging."""

import base64
import json
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import Select, Table, and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import ColumnElement

from dbpersist_common.filters import parse_filter, to_sqla
from dbpersist_common.filters.paths import is_column_field, split_path
from dbpersist_persistence.models import Object, ObjectType
from dbpersist_persistence.schemas import OrderBy, QueryIn
from dbpersist_persistence.services.errors import InvalidData

_T: Table = Object.__table__  # type: ignore[assignment]
DATA_COL: ColumnElement[Any] = _T.c.data
COLUMNS: dict[str, ColumnElement[Any]] = {
    "id": _T.c.id,
    "key": _T.c.key,
    "version": _T.c.version,
    "created_at": _T.c.created_at,
    "updated_at": _T.c.updated_at,
}


def _where(otype: ObjectType, raw_filter: dict[str, Any] | None) -> ColumnElement[Any]:
    base = and_(Object.type_name == otype.name, Object.deleted_at.is_(None))
    if raw_filter is None:
        return base
    try:
        f = parse_filter(raw_filter)
    except ValueError as exc:
        raise InvalidData(f"invalid filter: {exc}") from exc
    return and_(base, to_sqla(f, DATA_COL, COLUMNS))


def _order_expr(ob: OrderBy) -> ColumnElement[Any]:
    if is_column_field(ob.field):
        col = COLUMNS[ob.field]
    else:
        # order by the JSONB value, not its text form: numbers then sort numerically
        col = DATA_COL[tuple(split_path(ob.field))]
    return col.desc() if ob.desc else col.asc()


def encode_cursor(updated_at: datetime, obj_id: uuid.UUID) -> str:
    raw = json.dumps({"u": updated_at.isoformat(), "i": str(obj_id)})
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> tuple[datetime, uuid.UUID]:
    try:
        raw = json.loads(base64.urlsafe_b64decode(cursor.encode()))
        return datetime.fromisoformat(raw["u"]), uuid.UUID(raw["i"])
    except (ValueError, KeyError, TypeError) as exc:
        raise InvalidData("invalid cursor") from exc


def build_select(otype: ObjectType, q: QueryIn) -> Select[Any]:
    stmt = select(Object).where(_where(otype, q.filter))
    if q.cursor is not None:
        if q.order_by or q.offset:
            raise InvalidData("cursor cannot be combined with order_by or offset")
        u, i = decode_cursor(q.cursor)
        stmt = stmt.where(or_(Object.updated_at > u, and_(Object.updated_at == u, Object.id > i)))
    if q.order_by:
        stmt = stmt.order_by(*(_order_expr(ob) for ob in q.order_by), Object.id)
        stmt = stmt.offset(q.offset)
    else:
        stmt = stmt.order_by(Object.updated_at, Object.id)
        if q.offset:
            stmt = stmt.offset(q.offset)
    return stmt.limit(q.limit)


async def run_query(
    session: AsyncSession, otype: ObjectType, q: QueryIn
) -> tuple[list[Object], str | None]:
    rows: list[Object] = list((await session.execute(build_select(otype, q))).scalars())
    next_cursor = None
    if not q.order_by and len(rows) == q.limit:
        last = rows[-1]
        next_cursor = encode_cursor(last.updated_at, last.id)
    return rows, next_cursor


async def run_count(
    session: AsyncSession, otype: ObjectType, raw_filter: dict[str, Any] | None
) -> int:
    stmt = select(func.count()).select_from(Object).where(_where(otype, raw_filter))
    return int((await session.execute(stmt)).scalar_one())
