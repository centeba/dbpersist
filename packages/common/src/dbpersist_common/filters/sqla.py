"""Translate a filter tree into a SQLAlchemy boolean expression over a JSONB column.

Rules:
- Column fields (id, key, version, created_at, updated_at) compare against the
  matching column of ``columns``.
- Numbers compare as ``(data #>> path)::numeric``; strings and datetimes as text
  (ISO-8601 sorts lexicographically); booleans as ``::boolean``.
- ``eq`` on a scalar additionally uses JSONB containment (``@>``) so the GIN
  index on ``data`` can be used.
"""

import json
from typing import Any

from sqlalchemy import Boolean, Numeric, Text, and_, cast, literal, not_, or_, true
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.sql import ColumnElement

from dbpersist_common.filters.models import And, Clause, Or
from dbpersist_common.filters.paths import is_column_field, split_path


def _nested(path: list[str], value: Any) -> dict[str, Any]:
    out: Any = value
    for seg in reversed(path):
        out = {seg: out}
    return out


def _text_expr(data_col: ColumnElement[Any], path: list[str]) -> ColumnElement[Any]:
    return data_col[tuple(path)].astext  # type: ignore[index]


def _typed_expr(data_col: ColumnElement[Any], path: list[str], sample: Any) -> ColumnElement[Any]:
    text = _text_expr(data_col, path)
    if isinstance(sample, bool):
        return cast(text, Boolean)
    if isinstance(sample, (int, float)):
        return cast(text, Numeric)
    return text


def _compare(
    clause: Clause, target: ColumnElement[Any], containment: ColumnElement[Any] | None
) -> ColumnElement[Any]:
    value = clause.value
    match clause.op:
        case "eq":
            expr = target == value
            return and_(containment, expr) if containment is not None else expr
        case "ne":
            return target != value
        case "lt":
            return target < value
        case "le":
            return target <= value
        case "gt":
            return target > value
        case "ge":
            return target >= value
        case "like":
            return target.like(value)
        case "not_like":
            return not_(target.like(value))
        case "in":
            return target.in_(value)  # type: ignore[arg-type]
    raise ValueError(f"unsupported op {clause.op!r}")


def _clause(
    clause: Clause,
    data_col: ColumnElement[Any],
    columns: dict[str, ColumnElement[Any]],
) -> ColumnElement[Any]:
    if is_column_field(clause.field):
        col = columns[clause.field]
        if clause.op == "is_null":
            return col.is_(None)
        if clause.op == "is_not_null":
            return col.is_not(None)
        return _compare(clause, col, None)

    path = split_path(clause.field)
    text = _text_expr(data_col, path)
    if clause.op == "is_null":
        return text.is_(None)
    if clause.op == "is_not_null":
        return text.is_not(None)

    value = clause.value
    sample = value[0] if isinstance(value, list) else value
    target = _typed_expr(data_col, path, sample)
    containment = None
    if clause.op == "eq" and not isinstance(value, list):
        doc = cast(literal(json.dumps(_nested(path, value)), Text), JSONB)
        containment = cast(data_col, JSONB).contains(doc)
    return _compare(clause, target, containment)


def to_sqla(
    f: Clause | And | Or | None,
    data_col: ColumnElement[Any],
    columns: dict[str, ColumnElement[Any]] | None = None,
) -> ColumnElement[Any]:
    """Build a WHERE expression. ``columns`` maps reserved column field names to columns."""
    cols = columns or {}
    if f is None:
        return true()
    if isinstance(f, Clause):
        return _clause(f, data_col, cols)
    if isinstance(f, And):
        return and_(*(to_sqla(x, data_col, cols) for x in f.and_))
    return or_(*(to_sqla(x, data_col, cols) for x in f.or_))
