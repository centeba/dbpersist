"""Helpers for dotted field paths into JSONB data."""

import base64
from datetime import datetime
from typing import Any

from dbpersist_common.filters.models import COLUMN_FIELDS, Clause

MISSING = object()


def split_path(field: str) -> list[str]:
    return field.split(".")


def is_column_field(field: str) -> bool:
    return "." not in field and field in COLUMN_FIELDS


def lookup(data: dict[str, Any], field: str) -> Any:
    """Walk a dotted path; returns MISSING when any segment is absent."""
    cur: Any = data
    for seg in split_path(field):
        if not isinstance(cur, dict) or seg not in cur:
            return MISSING
        cur = cur[seg]
    return cur


def coerce_value(clause: Clause) -> Any:
    """Convert the clause's JSON value into the Python value used for comparison."""
    v = clause.value
    if clause.value_type == "datetime":
        if isinstance(v, list):
            return [datetime.fromisoformat(str(x)) for x in v]
        return datetime.fromisoformat(str(v))
    if clause.value_type == "bytes":
        return base64.b64decode(str(v))
    return v
