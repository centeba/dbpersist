"""Pydantic models for the JSON filter DSL.

A filter is a tree of ``Clause`` leaves joined by ``And`` / ``Or`` nodes::

    {"and": [{"field": "qty", "op": "gt", "value": 100},
             {"or": [{"field": "book", "op": "eq", "value": "A"},
                     {"field": "book", "op": "is_null"}]}]}

The same tree drives database queries (via SQLAlchemy) and in-memory event matching.
"""

import re
from datetime import datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

Op = Literal[
    "eq",
    "ne",
    "lt",
    "le",
    "gt",
    "ge",
    "like",
    "not_like",
    "is_null",
    "is_not_null",
    "in",
]

Scalar = str | int | float | bool | None

FIELD_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$")

# Top-level names that address object columns rather than JSONB data.
COLUMN_FIELDS: frozenset[str] = frozenset({"id", "key", "version", "created_at", "updated_at"})

_NULL_OPS = {"is_null", "is_not_null"}
_PATTERN_OPS = {"like", "not_like"}


class Clause(BaseModel):
    model_config = ConfigDict(extra="forbid")

    field: str
    op: Op
    value: Scalar | list[Scalar] = None
    value_type: Literal["datetime", "bytes"] | None = None

    @model_validator(mode="after")
    def _check(self) -> Clause:
        if not FIELD_RE.match(self.field):
            raise ValueError(f"invalid field path {self.field!r}")
        if self.op in _NULL_OPS:
            if self.value is not None:
                raise ValueError(f"op {self.op!r} takes no value")
            return self
        if self.op == "in":
            if not isinstance(self.value, list) or not self.value:
                raise ValueError("op 'in' requires a non-empty list value")
        else:
            if isinstance(self.value, list):
                raise ValueError(f"op {self.op!r} takes a scalar value")
            if self.value is None:
                raise ValueError(f"op {self.op!r} requires a value; use is_null for null tests")
            if self.op in _PATTERN_OPS and not isinstance(self.value, str):
                raise ValueError(f"op {self.op!r} requires a string pattern")
        if self.value_type == "datetime":
            vals = self.value if isinstance(self.value, list) else [self.value]
            for v in vals:
                if not isinstance(v, str):
                    raise ValueError("datetime values must be ISO-8601 strings")
                datetime.fromisoformat(v)
        if self.value_type == "bytes" and not isinstance(self.value, str):
            raise ValueError("bytes values must be base64 strings")
        return self


class And(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    and_: list[Filter] = Field(alias="and", min_length=1)


class Or(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    or_: list[Filter] = Field(alias="or", min_length=1)


Filter = Annotated[Clause | And | Or, Field(union_mode="left_to_right")]

And.model_rebuild()
Or.model_rebuild()

_filter_adapter: TypeAdapter[Clause | And | Or] = TypeAdapter(Filter)


def parse_filter(raw: Any) -> Clause | And | Or:
    """Validate a raw JSON value (or an existing model) into a filter tree."""
    if isinstance(raw, (Clause, And, Or)):
        return raw
    return _filter_adapter.validate_python(raw)


def dump_filter(f: Clause | And | Or) -> dict[str, Any]:
    return f.model_dump(by_alias=True, exclude_none=True)
