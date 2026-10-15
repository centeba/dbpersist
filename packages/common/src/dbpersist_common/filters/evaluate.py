"""In-memory evaluation of a filter against an object's data dict.

Used by the pubsub matcher so the same DSL that drives DB queries also decides
which subscribers receive a published event.
"""

import base64
import re
from datetime import datetime
from typing import Any

from dbpersist_common.filters.models import And, Clause, Or
from dbpersist_common.filters.paths import MISSING, coerce_value, lookup

_NUMERIC = (int, float)


def _like_to_regex(pattern: str) -> re.Pattern[str]:
    out: list[str] = []
    for ch in pattern:
        if ch == "%":
            out.append(".*")
        elif ch == "_":
            out.append(".")
        else:
            out.append(re.escape(ch))
    return re.compile("^" + "".join(out) + "$", re.DOTALL)


def _normalise_actual(actual: Any, clause: Clause) -> Any:
    """Bring the stored value into the comparison domain of the clause value."""
    if clause.value_type == "datetime" and isinstance(actual, str):
        try:
            return datetime.fromisoformat(actual)
        except ValueError:
            return MISSING
    if clause.value_type == "bytes" and isinstance(actual, str):
        try:
            return base64.b64decode(actual)
        except ValueError:
            return MISSING
    return actual


def _comparable(a: Any, b: Any) -> bool:
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool)
    if isinstance(a, _NUMERIC) and isinstance(b, _NUMERIC):
        return True
    return type(a) is type(b)


def _eval_clause(clause: Clause, data: dict[str, Any]) -> bool:
    actual = lookup(data, clause.field)
    is_null = actual is MISSING or actual is None

    if clause.op == "is_null":
        return is_null
    if clause.op == "is_not_null":
        return not is_null
    if is_null:
        return False

    expected = coerce_value(clause)
    actual = _normalise_actual(actual, clause)
    if actual is MISSING:
        return False

    if clause.op == "in":
        return any(_comparable(actual, e) and actual == e for e in expected)
    if clause.op in ("like", "not_like"):
        if not isinstance(actual, str):
            return False
        hit = _like_to_regex(expected).match(actual) is not None
        return hit if clause.op == "like" else not hit
    if not _comparable(actual, expected):
        return False
    match clause.op:
        case "eq":
            return actual == expected
        case "ne":
            return actual != expected
        case "lt":
            return actual < expected
        case "le":
            return actual <= expected
        case "gt":
            return actual > expected
        case "ge":
            return actual >= expected
    return False


def matches(f: Clause | And | Or, data: dict[str, Any]) -> bool:
    if isinstance(f, Clause):
        return _eval_clause(f, data)
    if isinstance(f, And):
        return all(matches(x, data) for x in f.and_)
    return any(matches(x, data) for x in f.or_)
